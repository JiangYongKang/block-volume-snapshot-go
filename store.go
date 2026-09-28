package blockvol

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// 每 commitCheckpointInterval 条已提交操作做一次“检查点落盘 + 清空 WAL”。
const commitCheckpointInterval int64 = 32

// Store 是本地块存储的顶层句柄，管理卷、快照、物理块与崩溃恢复。
// 一个目录同一时刻只能被一个 Store 实例独占打开。
type Store struct {
	dir       string
	opt       Options
	blockSize int

	// metaMu 保护 meta / refs / nextSeq / closed。
	//
	// 该 RWMutex 同时承担“读操作 GC 互斥”职责：
	//   - 读块（卷或快照）在 RLock 下完成“查映射 + 读物理文件”全过程；
	//   - 写块（含 COW 落盘新物理块）在 Lock 下完成；
	//   - GC 删除无引用块在 Lock 下完成。
	// 因此 GC 要么在所有读者之前删（读者随后查到的新映射不会指向它），
	// 要么等读者结束；删除期间不可能有读者正读该文件。
	metaMu  sync.RWMutex
	meta    *metaState
	refs    map[string]int // 物理块 ID -> 活引用数（活卷映射 + 全部快照映射 + 克隆映射）
	nextSeq int64
	closed  bool

	bs   *blockStore
	wal  *wal
	lock *fileLock

	stopGC context.CancelFunc
	gcWG   sync.WaitGroup

	logf func(format string, args ...any)
}

// Open 打开（必要时初始化）dir 下的存储，并执行崩溃恢复（重放 WAL）。
func Open(dir string, opts ...Option) (*Store, error) {
	opt := Options{BlockSize: defaultBlockSize, GCInterval: defaultGCInterval}
	for _, o := range opts {
		o(&opt)
	}
	if opt.BlockSize <= 0 {
		return nil, fmt.Errorf("%w: block size must be positive", ErrInvalidArgument)
	}
	logger := opt.Logger
	if logger == nil {
		logger = discardLogger{}
	}
	logf := func(f string, a ...any) { logger.Printf("[blockvol] "+f, a...) }

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	fl, err := acquireLock(dir)
	if err != nil {
		return nil, err
	}

	bs, err := newBlockStore(dir)
	if err != nil {
		fl.release()
		return nil, err
	}
	w, err := openWAL(dir)
	if err != nil {
		fl.release()
		return nil, err
	}

	// 1) 重放 WAL：识别并截断崩溃遗留的半帧。
	recs, err := w.replay()
	if err != nil {
		w.close()
		fl.release()
		return nil, err
	}
	// 2) 载入最近检查点。
	cp, err := loadCheckpoint(dir)
	if err != nil {
		w.close()
		fl.release()
		return nil, err
	}

	blockSize := opt.BlockSize
	if cp != nil {
		blockSize = cp.BlockSize
	}
	st := newMetaState(blockSize)
	var refs map[string]int
	var nextSeq int64
	if cp != nil {
		st.volumes = cp.Volumes
		st.snapshots = cp.Snapshots
		nextSeq = cp.NextSeq
		refs = computeRefs(st)
	} else {
		refs = map[string]int{}
	}

	s := &Store{
		dir:       dir,
		opt:       opt,
		blockSize: blockSize,
		meta:      st,
		refs:      refs,
		nextSeq:   nextSeq,
		bs:        bs,
		wal:       w,
		lock:      fl,
		logf:      logf,
	}

	// 3) 顺序应用检查点之后的已提交 WAL 记录（半帧已在 replay 时被截断，不会应用）。
	replayed := 0
	for i := range recs {
		r := recs[i]
		if r.Seq < nextSeq {
			continue // 已并入上一个检查点
		}
		s.applyRecord(&r)
		nextSeq = r.Seq + 1
		replayed++
	}
	s.nextSeq = nextSeq

	// 4) 清理崩溃前未落定的临时块（它们从未被任何已提交元数据引用）。
	nTmp, err := bs.cleanTempFiles()
	if err != nil {
		logf("WARN clean temp blocks failed: %v", err)
	}

	// 5) 固化恢复结果：新检查点落盘后清空旧 WAL。
	//    即使再次崩溃也只会从这个干净状态开始，不会重复应用记录。
	if err := s.checkpointLocked(); err != nil {
		w.close()
		fl.release()
		return nil, fmt.Errorf("post-recovery checkpoint: %w", err)
	}
	if err := w.truncate(); err != nil {
		w.close()
		fl.release()
		return nil, fmt.Errorf("post-recovery wal truncate: %w", err)
	}

	logf("opened dir=%s blockSize=%d volumes=%d snapshots=%d liveBlocks=%d replayed=%d removedTemp=%d",
		dir, blockSize, len(liveVolumes(st)), len(st.snapshots), len(refs), replayed, nTmp)

	if opt.GCInterval > 0 {
		s.startBackgroundGC(opt.GCInterval)
	}
	return s, nil
}

// Close 刷盘（检查点 + 清空 WAL）并释放资源。重复关闭返回 ErrClosed。
func (s *Store) Close() error {
	if s.stopGC != nil {
		s.stopGC()
		s.gcWG.Wait()
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.closed = true
	if err := s.checkpointLocked(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := s.wal.truncate(); err != nil {
		return fmt.Errorf("close wal truncate: %w", err)
	}
	if err := s.wal.close(); err != nil {
		return err
	}
	return s.lock.release()
}

// ---- 提交协议：先让副作用（物理块）落盘，再追加 WAL 并 fsync，最后改内存 ----

// commit 追加并应用一条记录。调用方必须持有 metaMu 写锁。
func (s *Store) commit(r *walRecord) error {
	if s.closed {
		return ErrClosed
	}
	r.Seq = s.nextSeq
	if err := s.wal.append(r); err != nil {
		return err
	}
	s.applyRecord(r)
	s.nextSeq++
	if s.nextSeq%commitCheckpointInterval == 0 {
		if err := s.checkpointLocked(); err != nil {
			// 检查点失败不影响已提交操作：WAL 仍完整，返回错误让上层感知 IO 问题。
			s.logf("WARN checkpoint failed: %v", err)
		} else {
			if err := s.wal.truncate(); err != nil {
				s.logf("WARN wal truncate after checkpoint failed: %v", err)
			}
		}
	}
	return nil
}

// checkpointLocked 将当前元数据（含 NextSeq）原子落盘。调用方持写锁。
func (s *Store) checkpointLocked() error {
	cp := s.meta.toCheckpoint()
	cp.NextSeq = s.nextSeq
	return saveAtomic(s.dir, cp)
}

// applyRecord 把一条已提交记录应用到内存状态，并增量维护 refs。
// 该函数必须是确定性的：崩溃恢复重放与在线应用走同一份代码。
func (s *Store) applyRecord(r *walRecord) {
	m := s.meta
	switch r.Op {
	case walCreateVolume, walClone:
		blocks := make([]string, r.Size)
		copy(blocks, r.Blocks)
		m.volumes[r.Volume] = &volumeMeta{
			Name:      r.Volume,
			Size:      r.Size,
			CreatedAt: r.CreatedAt,
			Blocks:    blocks,
		}
		s.refBlocks(blocks, +1)
	case walDeleteVolume:
		vm := m.volumes[r.Volume]
		if vm != nil && !vm.Deleted {
			vm.Deleted = true
			s.refBlocks(vm.Blocks, -1) // 快照仍独立持有这些块
		}
	case walWriteBlock:
		vm := m.volumes[r.Volume]
		if vm == nil {
			s.logf("WARN replay write_block for missing volume=%s, skipping", r.Volume)
			return
		}
		if r.Index < 0 || r.Index >= len(vm.Blocks) {
			s.logf("WARN replay write_block bad index=%d, skipping", r.Index)
			return
		}
		old := vm.Blocks[r.Index]
		if old == r.Physical {
			return
		}
		if old != "" {
			s.refBlock(old, -1)
		}
		if r.Physical != "" {
			s.refBlock(r.Physical, +1)
		}
		vm.Blocks[r.Index] = r.Physical
	case walCreateSnapshot:
		m.snapshots[r.Snapshot] = &snapshotMeta{
			Name:      r.Snapshot,
			Source:    r.Source,
			Size:      r.Size,
			CreatedAt: r.CreatedAt,
			Blocks:    cloneBlocks(r.Blocks),
		}
		s.refBlocks(r.Blocks, +1)
	case walDeleteSnapshot:
		sn := m.snapshots[r.Snapshot]
		if sn != nil {
			delete(m.snapshots, r.Snapshot)
			s.refBlocks(sn.Blocks, -1)
		}
	case walRestore:
		vm := m.volumes[r.Volume]
		if vm == nil {
			s.logf("WARN replay restore for missing volume=%s, skipping", r.Volume)
			return
		}
		// 已删除卷的映射在删除时就已经从引用计数中移除，不能再次递减。
		if !vm.Deleted {
			s.refBlocks(vm.Blocks, -1)
		}
		vm.Blocks = cloneBlocks(r.Blocks)
		s.refBlocks(vm.Blocks, +1)
		vm.Deleted = false
	}
}

// refBlocks 按映射批量调整引用计数；归零即从表中移除。
func (s *Store) refBlocks(blocks []string, delta int) {
	for _, id := range blocks {
		if id == "" {
			continue
		}
		s.refBlock(id, delta)
	}
}

// refBlock 调整单个物理块的引用计数。
func (s *Store) refBlock(id string, delta int) {
	n := s.refs[id] + delta
	if n < 0 {
		// 理论上不可达：任何 -1 都对应早先的 +1。出现即说明状态机有 bug。
		s.logf("FATAL refcount underflow block=%s delta=%d", id, delta)
		delete(s.refs, id)
		return
	}
	if n == 0 {
		delete(s.refs, id)
	} else {
		s.refs[id] = n
	}
}

// computeRefs 从一份元数据全量计算物理块引用计数（用于恢复与一致性校验）。
// 已删除卷的映射不计入：其块的存活由快照/克隆持有。
func computeRefs(m *metaState) map[string]int {
	refs := map[string]int{}
	add := func(blocks []string) {
		for _, id := range blocks {
			if id != "" {
				refs[id]++
			}
		}
	}
	for _, vm := range m.volumes {
		if !vm.Deleted {
			add(vm.Blocks)
		}
	}
	for _, sn := range m.snapshots {
		add(sn.Blocks)
	}
	return refs
}

func liveVolumes(m *metaState) map[string]*volumeMeta {
	out := map[string]*volumeMeta{}
	for k, v := range m.volumes {
		if !v.Deleted {
			out[k] = v
		}
	}
	return out
}

// ---- 卷的生命周期 ----

// CreateVolume 创建一个包含 sizeBlocks 个逻辑块的空卷。
func (s *Store) CreateVolume(name string, sizeBlocks int) (*Volume, error) {
	if name == "" || sizeBlocks <= 0 {
		return nil, fmt.Errorf("%w: name required and sizeBlocks must be positive", ErrInvalidArgument)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if _, ok := s.meta.volumes[name]; ok {
		return nil, fmt.Errorf("%w: %s", ErrVolumeExists, name)
	}
	err := s.commit(&walRecord{
		Op:        walCreateVolume,
		Volume:    name,
		Size:      sizeBlocks,
		CreatedAt: time.Now().UnixNano(),
	})
	if err != nil {
		return nil, err
	}
	s.logf("create-volume name=%s size=%d", name, sizeBlocks)
	return &Volume{store: s, name: name}, nil
}

// DeleteVolume 删除卷；只要仍有快照/克隆引用该卷，数据仍可通过快照还原。
// 对从未存在的卷名返回 ErrVolumeNotFound；重复删除已删除卷是幂等成功。
func (s *Store) DeleteVolume(name string) error {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return ErrClosed
	}
	vm := s.meta.volumes[name]
	if vm == nil {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, name)
	}
	if vm.Deleted {
		return nil // 幂等：不重复递减引用计数
	}
	if err := s.commit(&walRecord{Op: walDeleteVolume, Volume: name}); err != nil {
		return err
	}
	s.logf("delete-volume name=%s refsAfter=%d", name, s.totalRefs())
	return nil
}

// GetVolume 获取存活卷句柄；卷已删除或不存在返回 ErrVolumeNotFound。
func (s *Store) GetVolume(name string) (*Volume, error) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	vm := s.meta.volumes[name]
	if vm == nil || vm.Deleted {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, name)
	}
	return &Volume{store: s, name: name}, nil
}

// ListVolumes 列出当前存活（未删除）的卷名。
func (s *Store) ListVolumes() []string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	out := make([]string, 0, len(s.meta.volumes))
	for k, vm := range s.meta.volumes {
		if !vm.Deleted {
			out = append(out, k)
		}
	}
	return out
}

// ListSnapshots 列出全部快照名。
func (s *Store) ListSnapshots() []string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	out := make([]string, 0, len(s.meta.snapshots))
	for k := range s.meta.snapshots {
		out = append(out, k)
	}
	return out
}

// GetSnapshot 按名查找快照（包括源卷已被删除的快照）。
func (s *Store) GetSnapshot(snapName string) (*Snapshot, error) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	sn := s.meta.snapshots[snapName]
	if sn == nil {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapName)
	}
	return newSnapshotHandle(s, sn), nil
}

// BlockRefCount 返回某物理块当前引用数（供测试与排障）。
func (s *Store) BlockRefCount(physicalID string) (int, bool) {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	n, ok := s.refs[physicalID]
	return n, ok
}

func (s *Store) totalRefs() int {
	n := 0
	for _, c := range s.refs {
		n += c
	}
	return n
}

// ---- 以下为只读诊断接口，主要供测试与排障使用 ----

// RefsSnapshot 返回物理块 ID -> 引用数 的拷贝。
func (s *Store) RefsSnapshot() map[string]int {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	out := make(map[string]int, len(s.refs))
	for k, v := range s.refs {
		out[k] = v
	}
	return out
}

// TotalRefs 返回当前全部活引用数之和。
func (s *Store) TotalRefs() int {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	return s.totalRefs()
}

// CheckRefConsistency 校验“从元数据全量计算的引用计数”与增量维护值一致，
// 返回不一致说明；一致时返回空串。用于测试在任意操作序列后检查内部不变量。
func (s *Store) CheckRefConsistency() string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	want := computeRefs(s.meta)
	if len(want) != len(s.refs) {
		return fmt.Sprintf("ref map size mismatch: incremental=%d full=%d", len(s.refs), len(want))
	}
	for id, n := range want {
		if s.refs[id] != n {
			return fmt.Sprintf("block %s: incremental=%d full=%d", id, s.refs[id], n)
		}
	}
	return ""
}

// BlockSize 返回该存储的逻辑块大小。
func (s *Store) BlockSize() int { return s.blockSize }

// crashSimulateClose 仅用于测试：模拟进程硬崩溃——释放排他锁与文件句柄，
// 但绝不写检查点、绝不清空 WAL。之后可用 Open 在同目录重放恢复。
func (s *Store) crashSimulateClose() {
	if s.stopGC != nil {
		s.stopGC()
		s.gcWG.Wait()
	}
	s.metaMu.Lock()
	s.closed = true
	_ = s.wal.close()
	_ = s.lock.release()
	s.metaMu.Unlock()
}
