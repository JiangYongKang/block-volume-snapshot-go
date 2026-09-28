package blocksnap

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 默认参数。
const (
	defaultBlockSize  = 4096
	defaultGCInterval = 200 * time.Millisecond
	checkpointWALSize = 1 << 20 // WAL 超过 1MB 时做一次检查点压缩
)

// Options 控制 Store 的运行参数。
type Options struct {
	BlockSize int // 块大小，默认 4096

	DisableGC   bool                 // 显式关闭后台回收
	GCInterval  time.Duration        // 后台回收间隔，默认 200ms
	DisableSync bool                 // 仅测试用：跳过 fsync
	Logf        func(string, ...any) // 关键操作序列日志（测试可接 t.Logf）
}

// Store 是块存储卷快照系统的入口，所有方法线程安全。
type Store struct {
	dataDir string
	opts    Options

	mu     sync.RWMutex
	meta   *metaState
	blocks *blockStore
	wal    *wal

	crashMu   sync.Mutex
	crashes   map[string]bool
	crashLeft map[string]int // 命名崩溃点剩余触发次数；-1 表示每次都触发

	gcStop chan struct{}
	gcWg   sync.WaitGroup
	closed bool
}

// Open 打开（或初始化）dataDir 下的存储并执行崩溃恢复。
func Open(dataDir string, opts Options) (*Store, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = defaultBlockSize
	}
	if opts.GCInterval <= 0 {
		opts.GCInterval = defaultGCInterval
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "blocks"), 0o755); err != nil {
		return nil, fmt.Errorf("open data dir: %w", err)
	}
	bs, err := openBlockStore(filepath.Join(dataDir, "blocks"))
	if err != nil {
		return nil, err
	}
	st, err := recoverStore(dataDir, opts.DisableSync)
	if err != nil {
		return nil, err
	}

	// 恢复后校验：被引用块必须在磁盘上存在；不被任何元数据引用的块视为
	// 崩溃中断写入留下的孤儿，立即安全回收。
	refs := st.blockRefs()
	for id := range refs {
		ok, err := bs.exists(id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: referenced block %s missing after recovery", ErrCorrupt, id[:12])
		}
	}
	if n, err := bs.sweep(func(id string) bool {
		_, keep := refs[id]
		return keep
	}); err != nil {
		return nil, err
	} else if n > 0 {
		opts.logf("recover: swept %d orphan block(s)", n)
	}

	w, err := openWAL(filepath.Join(dataDir, "wal.log"), opts.DisableSync)
	if err != nil {
		return nil, err
	}

	s := &Store{
		dataDir:   dataDir,
		opts:      opts,
		meta:      st,
		blocks:    bs,
		wal:       w,
		crashes:   map[string]bool{},
		crashLeft: map[string]int{},
		gcStop:    make(chan struct{}),
	}
	if !opts.DisableGC {
		s.gcWg.Add(1)
		go s.gcLoop(opts.GCInterval)
	}
	s.logf("open: dataDir=%s volumes=%d snapshots=%d blocks=%d",
		dataDir, len(st.Volumes), len(st.Snapshots), len(st.RefCount))
	return s, nil
}

// Close 停止后台回收并关闭 WAL（同时做一次检查点压缩）。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	close(s.gcStop)
	s.gcWg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkpointLocked(); err != nil {
		s.wal.close()
		return err
	}
	return s.wal.close()
}

func (o Options) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

func (s *Store) logf(format string, args ...any) { s.opts.logf(format, args...) }

// ---------------------------------------------------------------- 卷管理

// CreateVolume 创建卷。clientID 为幂等键，重试返回同一结果。
func (s *Store) CreateVolume(clientID, name string, sizeBytes int64) error {
	if name == "" || sizeBytes < 0 {
		return ErrInvalidArg
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.idemSeen(clientID) {
		s.logf("create-volume: idempotent replay client=%s name=%s", clientID, name)
		return nil
	}
	if _, ok := s.meta.Volumes[name]; ok {
		return ErrAlreadyExists
	}
	seq := s.meta.NextSeq + 1
	if err := s.wal.append(walRecord{
		Seq: seq, Client: clientID, Type: recCreateVolume,
		Payload: map[string]any{"volume": name, "size": sizeBytes},
	}); err != nil {
		return err
	}
	if err := applyRecord(s.meta, walRecord{Type: recCreateVolume,
		Payload: map[string]any{"volume": name, "size": float64(sizeBytes)}}); err != nil {
		return err
	}
	s.meta.NextSeq = seq
	s.idemMark(clientID)
	s.logf("[seq=%d] create-volume name=%s size=%d", seq, name, sizeBytes)
	return nil
}

// DeleteVolume 删除卷；只要还有快照引用，卷元数据与数据块都会保留，可随时还原。
func (s *Store) DeleteVolume(clientID, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.idemSeen(clientID) {
		return nil
	}
	if _, ok := s.meta.Volumes[name]; !ok {
		return fmt.Errorf("%w: volume %q", ErrNotFound, name)
	}
	seq := s.meta.NextSeq + 1
	if err := s.wal.append(walRecord{
		Seq: seq, Client: clientID, Type: recDeleteVolume,
		Payload: map[string]any{"volume": name},
	}); err != nil {
		return err
	}
	if err := applyRecord(s.meta, walRecord{Type: recDeleteVolume,
		Payload: map[string]any{"volume": name}}); err != nil {
		return err
	}
	s.meta.NextSeq = seq
	s.idemMark(clientID)
	s.afterCommitLocked("delete-volume", seq, map[string]any{"volume": name})
	return nil
}

// ---------------------------------------------------------------- 读写

// WriteAt 在卷偏移处写入数据。部分块写采用读-改-写，产生新的不可变块（COW），
// 因此与 CreateSnapshot 并发时旧块始终保持快照时点内容。
// 整个用户操作在【单个 WAL 记录】中提交：要么全部块可见，要么全部不可见。
func (s *Store) WriteAt(clientID, name string, offset int64, data []byte) error {
	if offset < 0 || len(data) == 0 {
		return ErrInvalidArg
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.idemSeen(clientID) {
		s.logf("write: idempotent replay client=%s name=%s off=%d", clientID, name, offset)
		return nil
	}
	v, ok := s.meta.Volumes[name]
	if !ok || v.Deleted {
		return fmt.Errorf("%w: volume %q", ErrNotFound, name)
	}
	if offset+int64(len(data)) > v.Size {
		return fmt.Errorf("%w: write past volume size", ErrInvalidArg)
	}

	bs := s.opts.BlockSize
	// 计算本操作涉及的块集合，依次做读-改-写并落盘新块（旧块不可变）。
	newBlocks := map[uint64]string{}
	end := offset + int64(len(data))
	first := uint64(offset / int64(bs))
	last := uint64((end - 1) / int64(bs))
	for idx := first; idx <= last; idx++ {
		content := make([]byte, bs)
		if oldID, ok := v.Blocks[idx]; ok {
			old, err := s.blocks.read(oldID)
			if err != nil {
				return err
			}
			copy(content, old)
		}
		blkStart := int64(idx) * int64(bs)
		blkEnd := blkStart + int64(bs)
		start := offset
		if start < blkStart {
			start = blkStart
		}
		stop := end
		if stop > blkEnd {
			stop = blkEnd
		}
		copy(content[start-blkStart:stop-blkStart], data[start-offset:stop-offset])

		id := blockID(content)
		if err := s.blocks.write(id, content); err != nil {
			return err
		}
		s.crash("write:afterBlockBeforeWAL")
		newBlocks[idx] = id
	}

	seq := s.meta.NextSeq + 1
	if err := s.wal.append(walRecord{
		Seq: seq, Client: clientID, Type: recWrite,
		Payload: map[string]any{"volume": name, "blocks": uint64Map(newBlocks)},
	}); err != nil {
		return err
	}
	if err := applyRecord(s.meta, walRecord{Type: recWrite,
		Payload: map[string]any{"volume": name, "blocks": uint64Map(newBlocks)}}); err != nil {
		return err
	}
	s.meta.NextSeq = seq
	s.idemMark(clientID)
	s.afterCommitLocked("write", seq, map[string]any{"volume": name, "nblocks": len(newBlocks)})
	return s.maybeCheckpointLocked()
}

// ReadAt 读取卷数据（从未写入的稀疏区域返回零字节）。
func (s *Store) ReadAt(name string, offset int64, data []byte) (int, error) {
	if offset < 0 {
		return 0, ErrInvalidArg
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.meta.Volumes[name]
	if !ok || v.Deleted {
		return 0, fmt.Errorf("%w: volume %q", ErrNotFound, name)
	}
	bs := int64(s.opts.BlockSize)
	for i := range data {
		pos := offset + int64(i)
		if pos >= v.Size {
			return i, nil
		}
		idx := uint64(pos / bs)
		if id, ok := v.Blocks[idx]; ok {
			block, err := s.blocks.read(id)
			if err != nil {
				return i, err
			}
			data[i] = block[pos%bs]
		} else {
			data[i] = 0
		}
	}
	return len(data), nil
}

// ---------------------------------------------------------------- 快照

// CreateSnapshot 基于 COW 语义生成卷的只读快照：快照保存当前块映射的副本。
// 此后任何写入都走新块，永远不会修改快照引用的块。
func (s *Store) CreateSnapshot(clientID, volume, snapshot string) error {
	if snapshot == "" {
		return ErrInvalidArg
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.idemSeen(clientID) {
		s.logf("create-snapshot: idempotent replay client=%s snap=%s", clientID, snapshot)
		return nil
	}
	v, ok := s.meta.Volumes[volume]
	if !ok || v.Deleted {
		return fmt.Errorf("%w: volume %q", ErrNotFound, volume)
	}
	if _, ok := s.meta.Snapshots[snapshot]; ok {
		return fmt.Errorf("%w: snapshot %q", ErrAlreadyExists, snapshot)
	}
	blocksCopy := uint64Map(cloneBlockMap(v.Blocks))
	seq := s.meta.NextSeq + 1
	if err := s.wal.append(walRecord{
		Seq: seq, Client: clientID, Type: recCreateSnap,
		Payload: map[string]any{"volume": volume, "snapshot": snapshot, "blocks": blocksCopy},
	}); err != nil {
		return err
	}
	s.crash("snap:afterWAL")
	if err := applyRecord(s.meta, walRecord{Type: recCreateSnap,
		Payload: map[string]any{"volume": volume, "snapshot": snapshot, "blocks": blocksCopy}}); err != nil {
		return err
	}
	s.meta.NextSeq = seq
	s.idemMark(clientID)
	s.afterCommitLocked("create-snapshot", seq, map[string]any{"snap": snapshot, "volume": volume})
	return nil
}

// DeleteSnapshot 删除快照；仅被该快照引用的块引用计数归零，交由 GC 回收。
func (s *Store) DeleteSnapshot(clientID, snapshot string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.idemSeen(clientID) {
		return nil
	}
	if _, ok := s.meta.Snapshots[snapshot]; !ok {
		return fmt.Errorf("%w: snapshot %q", ErrNotFound, snapshot)
	}
	seq := s.meta.NextSeq + 1
	if err := s.wal.append(walRecord{
		Seq: seq, Client: clientID, Type: recDeleteSnap,
		Payload: map[string]any{"snapshot": snapshot},
	}); err != nil {
		return err
	}
	if err := applyRecord(s.meta, walRecord{Type: recDeleteSnap,
		Payload: map[string]any{"snapshot": snapshot}}); err != nil {
		return err
	}
	s.meta.NextSeq = seq
	s.idemMark(clientID)
	s.afterCommitLocked("delete-snapshot", seq, map[string]any{"snap": snapshot})
	return s.maybeCheckpointLocked()
}

// Restore 将卷内容还原到快照时点。目标卷即使已被删除（但仍被该快照引用）
// 也会原样复活；还原后逐字节等于快照时点内容。
func (s *Store) Restore(clientID, volume, snapshot string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.idemSeen(clientID) {
		return nil
	}
	snap, ok := s.meta.Snapshots[snapshot]
	if !ok {
		return fmt.Errorf("%w: snapshot %q", ErrNotFound, snapshot)
	}
	blocksCopy := uint64Map(cloneBlockMap(snap.Blocks))
	seq := s.meta.NextSeq + 1
	if err := s.wal.append(walRecord{
		Seq: seq, Client: clientID, Type: recRestore,
		Payload: map[string]any{"volume": volume, "snapshot": snapshot, "blocks": blocksCopy},
	}); err != nil {
		return err
	}
	if err := applyRecord(s.meta, walRecord{Type: recRestore,
		Payload: map[string]any{"volume": volume, "snapshot": snapshot, "blocks": blocksCopy}}); err != nil {
		return err
	}
	s.meta.NextSeq = seq
	s.idemMark(clientID)
	s.afterCommitLocked("restore", seq, map[string]any{"volume": volume, "snap": snapshot})
	return s.maybeCheckpointLocked()
}

// CloneVolume 从快照克隆出新卷：共享物理块（引用计数增加），后续写入互不影响。
func (s *Store) CloneVolume(clientID, snapshot, newVolume string, sizeBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.idemSeen(clientID) {
		return nil
	}
	snap, ok := s.meta.Snapshots[snapshot]
	if !ok {
		return fmt.Errorf("%w: snapshot %q", ErrNotFound, snapshot)
	}
	if _, ok := s.meta.Volumes[newVolume]; ok {
		return fmt.Errorf("%w: volume %q", ErrAlreadyExists, newVolume)
	}
	if sizeBytes <= 0 {
		if src, ok := s.meta.Volumes[snap.Volume]; ok {
			sizeBytes = src.Size
		} else {
			sizeBytes = int64(s.opts.BlockSize)
		}
	}
	blocksCopy := uint64Map(cloneBlockMap(snap.Blocks))
	seq := s.meta.NextSeq + 1
	if err := s.wal.append(walRecord{
		Seq: seq, Client: clientID, Type: recClone,
		Payload: map[string]any{"snapshot": snapshot, "volume": newVolume,
			"size":   sizeBytes,
			"blocks": blocksCopy},
	}); err != nil {
		return err
	}
	if err := applyRecord(s.meta, walRecord{Type: recClone,
		Payload: map[string]any{"snapshot": snapshot, "volume": newVolume,
			"size":   float64(sizeBytes),
			"blocks": blocksCopy}}); err != nil {
		return err
	}
	s.meta.NextSeq = seq
	s.idemMark(clientID)
	s.afterCommitLocked("clone", seq, map[string]any{"volume": newVolume, "snap": snapshot})
	return nil
}

// ---------------------------------------------------------------- 可观测接口

// VolumeInfo 描述一个卷的可见状态。
type VolumeInfo struct {
	Name    string
	Size    int64
	Deleted bool
}

// ListVolumes 列出所有逻辑卷（含删除但仍被快照引用而保留的卷）。
func (s *Store) ListVolumes() ([]VolumeInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]VolumeInfo, 0, len(s.meta.Volumes))
	for _, v := range s.meta.Volumes {
		out = append(out, VolumeInfo{Name: v.Name, Size: v.Size, Deleted: v.Deleted})
	}
	return out, nil
}

// ListSnapshots 列出快照；volume 为空时列出全部。
func (s *Store) ListSnapshots(volume string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for id, snap := range s.meta.Snapshots {
		if volume == "" || snap.Volume == volume {
			out = append(out, id)
		}
	}
	return out, nil
}

// BlockRefCount 返回物理块当前引用计数（测试/可解释性用）。
func (s *Store) BlockRefCount(physicalBlockID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.meta.refCountOf(physicalBlockID)
}

// SnapshotBlockID 返回快照中某逻辑块当前映射到的物理块 id（测试用）。
func (s *Store) SnapshotBlockID(snapshot string, logicalIndex uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap, ok := s.meta.Snapshots[snapshot]
	if !ok {
		return "", false
	}
	id, ok := snap.Blocks[logicalIndex]
	return id, ok
}

// VolumeBlockID 返回卷某逻辑块当前映射到的物理块 id（测试用）。
func (s *Store) VolumeBlockID(volume string, logicalIndex uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.meta.Volumes[volume]
	if !ok {
		return "", false
	}
	id, ok := v.Blocks[logicalIndex]
	return id, ok
}

// ---------------------------------------------------------------- 崩溃注入

// SetCrashPoint 启用一个命名崩溃点：之后每次经过该点，进程都以退出码 99 中断，
// 用于在子进程中模拟真实的“操作中途掉电”。
func (s *Store) SetCrashPoint(name string) {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	s.crashes[name] = true
	s.crashLeft[name] = -1
}

// SetCrashPointN 启用命名崩溃点，但只在第 n 次经过该点时触发（n 从 1 计）。
// 例如 n=2 表示第一次经过不崩、第二次经过才崩。
func (s *Store) SetCrashPointN(name string, n int) {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	s.crashes[name] = true
	s.crashLeft[name] = n
}

// ClearCrashPoints 清除全部崩溃点。
func (s *Store) ClearCrashPoints() {
	s.crashMu.Lock()
	defer s.crashMu.Unlock()
	s.crashes = map[string]bool{}
	s.crashLeft = map[string]int{}
}

func (s *Store) crash(name string) {
	s.crashMu.Lock()
	if !s.crashes[name] {
		s.crashMu.Unlock()
		return
	}
	left := s.crashLeft[name]
	if left > 0 {
		left--
		s.crashLeft[name] = left
	}
	shouldCrash := left == 0 || left == -1
	s.crashMu.Unlock()
	if shouldCrash {
		s.logf("CRASH POINT HIT: %s — exiting 99", name)
		os.Exit(99)
	}
}

// ForceGC 同步执行一次回收，返回回收块数。
func (s *Store) ForceGC(ctx context.Context) (int, error) {
	return s.collectGC(ctx)
}

// ForceCheckpoint 同步执行一次检查点压缩（meta.json 落盘并截断 WAL）。
func (s *Store) ForceCheckpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.checkpointLocked()
}

// ---------------------------------------------------------------- 内部辅助

// afterCommitLocked 在变更提交后重算引用计数并打印引用计数变化日志。
func (s *Store) afterCommitLocked(op string, seq int64, detail map[string]any) {
	before := s.meta.RefCount
	s.meta.recomputeRefs()
	var changed []any
	for id, n := range s.meta.RefCount {
		if before[id] != n {
			changed = append(changed, fmt.Sprintf("%s:%d->%d", id[:8], before[id], n))
		}
	}
	for id, n := range before {
		if _, ok := s.meta.RefCount[id]; !ok && n != 0 {
			changed = append(changed, fmt.Sprintf("%s:%d->0", id[:8], n))
		}
	}
	s.logf("[seq=%d] %s %v refs=%v", seq, op, detail, changed)
}

// maybeCheckpointLocked 在 WAL 过大时做一次检查点压缩。
func (s *Store) maybeCheckpointLocked() error {
	info, err := os.Stat(filepath.Join(s.dataDir, "wal.log"))
	if err != nil || info.Size() < checkpointWALSize {
		return nil
	}
	return s.checkpointLocked()
}

// checkpointLocked 以“先落盘 meta.json，再截断 WAL”的顺序压缩日志；
// 任意一步崩溃都可在 recoverStore 中正确处理。
func (s *Store) checkpointLocked() error {
	if err := writeCheckpoint(s.dataDir, s.meta, s.opts.DisableSync); err != nil {
		return err
	}
	s.crash("checkpoint:afterMetaBeforeTruncate")
	if err := s.wal.truncate(); err != nil {
		return err
	}
	s.crash("checkpoint:afterTruncate")
	s.logf("checkpoint: compacted at seq=%d", s.meta.NextSeq)
	return nil
}

// uint64Map 在 map[uint64]string 与 map[string]any(JSON 安全类型) 间转换。
func uint64Map(m map[uint64]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[fmt.Sprintf("%d", k)] = v
	}
	return out
}

// mapToUint64 将 WAL/JSON 的字符串键映射转回 uint64 键。
func mapToUint64(m map[string]any) (map[uint64]string, error) {
	out := make(map[uint64]string, len(m))
	for k, v := range m {
		var idx uint64
		if _, err := fmt.Sscanf(k, "%d", &idx); err != nil {
			return nil, fmt.Errorf("%w: block index %q", ErrCorrupt, k)
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%w: block id for index %q", ErrCorrupt, k)
		}
		out[idx] = s
	}
	return out, nil
}
