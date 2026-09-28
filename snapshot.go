package blockvol

import (
	"fmt"
	"time"
)

// ---- Store 层：快照的创建 / 删除 / 还原 / 克隆 ----

// CreateSnapshot 对指定卷创建写时复制快照；与卷上的并发读写串行化，
// 快照保存的是某一条已提交写入边界上的完整映射，绝不包含“写了一半”的状态。
func (s *Store) CreateSnapshot(volName, snapName string) (*Snapshot, error) {
	if volName == "" || snapName == "" {
		return nil, fmt.Errorf("%w: volume and snapshot names required", ErrInvalidArgument)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	vm := s.meta.volumes[volName]
	if vm == nil || vm.Deleted {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, volName)
	}
	if _, ok := s.meta.snapshots[snapName]; ok {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotExists, snapName)
	}
	blocks := cloneBlocks(vm.Blocks) // 深拷贝：此后卷的 COW 写入与快照彻底隔离
	rec := &walRecord{
		Op:        walCreateSnapshot,
		Volume:    volName,
		Snapshot:  snapName,
		Source:    volName,
		Size:      vm.Size,
		Blocks:    blocks,
		CreatedAt: time.Now().UnixNano(),
	}
	if err := s.commit(rec); err != nil {
		return nil, err
	}
	s.logf("create-snapshot name=%s source=%s size=%d refsAfter=%d",
		snapName, volName, vm.Size, s.totalRefs())
	return newSnapshotHandle(s, s.meta.snapshots[snapName]), nil
}

// DeleteSnapshot 删除快照并递减引用计数；幂等：快照不存在返回
// ErrSnapshotNotFound（重试调用只会再次得到该结果，绝不会重复递减计数）。
func (s *Store) DeleteSnapshot(snapName string) error {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, ok := s.meta.snapshots[snapName]; !ok {
		return fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapName)
	}
	if err := s.commit(&walRecord{Op: walDeleteSnapshot, Snapshot: snapName}); err != nil {
		return err
	}
	s.logf("delete-snapshot name=%s refsAfter=%d", snapName, s.totalRefs())
	return nil
}

// RestoreSnapshot 用快照内容还原到卷 volName：
//   - volName 是已存在的活卷：容量必须一致，映射整体切换为快照映射；
//   - volName 是已删除卷（名字墓碑仍在）：容量一致时原地复活；
//   - volName 不存在：按快照容量新建卷。
//
// 还原后卷内容与快照时点逐块一致；共享物理块（不复制数据）。
func (s *Store) RestoreSnapshot(snapName, volName string) (*Volume, error) {
	if snapName == "" || volName == "" {
		return nil, fmt.Errorf("%w: snapshot and volume names required", ErrInvalidArgument)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	sn := s.meta.snapshots[snapName]
	if sn == nil {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, snapName)
	}
	vm := s.meta.volumes[volName]
	if vm == nil {
		// 按快照容量重建卷（源卷可能早已删除）。
		rec := &walRecord{
			Op:        walCreateVolume,
			Volume:    volName,
			Size:      sn.Size,
			Blocks:    cloneBlocks(sn.Blocks),
			CreatedAt: time.Now().UnixNano(),
		}
		if err := s.commit(rec); err != nil {
			return nil, err
		}
		s.logf("restore snapshot=%s into-new-volume=%s refsAfter=%d",
			snapName, volName, s.totalRefs())
		return &Volume{store: s, name: volName}, nil
	}
	if vm.Size != sn.Size {
		return nil, fmt.Errorf("%w: snapshot size %d != volume size %d",
			ErrInvalidArgument, sn.Size, vm.Size)
	}
	rec := &walRecord{
		Op:       walRestore,
		Volume:   volName,
		Snapshot: snapName,
		Blocks:   cloneBlocks(sn.Blocks),
	}
	if err := s.commit(rec); err != nil {
		return nil, err
	}
	s.logf("restore snapshot=%s volume=%s deletedWas=%t refsAfter=%d",
		snapName, volName, vm.Deleted, s.totalRefs())
	return &Volume{store: s, name: volName}, nil
}

// CloneVolume 基于活卷创建克隆卷。克隆与源共享全部物理块（仅复制映射），
// 此后任一方的写时复制都不影响对方。
func (s *Store) CloneVolume(srcName, cloneName string) (*Volume, error) {
	if srcName == "" || cloneName == "" {
		return nil, fmt.Errorf("%w: source and clone names required", ErrInvalidArgument)
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	src := s.meta.volumes[srcName]
	if src == nil || src.Deleted {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, srcName)
	}
	if _, ok := s.meta.volumes[cloneName]; ok {
		return nil, fmt.Errorf("%w: %s", ErrVolumeExists, cloneName)
	}
	rec := &walRecord{
		Op:        walClone,
		Volume:    cloneName,
		Source:    srcName,
		Size:      src.Size,
		Blocks:    cloneBlocks(src.Blocks),
		CreatedAt: time.Now().UnixNano(),
	}
	if err := s.commit(rec); err != nil {
		return nil, err
	}
	s.logf("clone src=%s clone=%s refsAfter=%d", srcName, cloneName, s.totalRefs())
	return &Volume{store: s, name: cloneName}, nil
}

// ---- Snapshot 只读句柄 ----

// Snapshot 是卷在某一时点的只读映像。
type Snapshot struct {
	store     *Store
	name      string
	source    string
	size      int
	createdAt int64
}

func newSnapshotHandle(s *Store, m *snapshotMeta) *Snapshot {
	return &Snapshot{store: s, name: m.Name, source: m.Source, size: m.Size, createdAt: m.CreatedAt}
}

// Name 返回快照名。
func (sn *Snapshot) Name() string { return sn.name }

// SourceVolume 返回源卷名（源卷可能已被删除）。
func (sn *Snapshot) SourceVolume() string { return sn.source }

// CreatedAt 返回快照创建时间。
func (sn *Snapshot) CreatedAt() time.Time { return time.Unix(0, sn.createdAt) }

// SizeBlocks 返回快照的逻辑块数。
func (sn *Snapshot) SizeBlocks() int { return sn.size }

// ReadBlock 读取快照中第 index 个逻辑块。
// 读路径持 RLock 覆盖“查映射 + 读物理文件”全过程，因此要么读到快照中
// 完整一致的块，要么在快照已被删除时得到 ErrSnapshotNotFound，不会读到
// 被 GC 回收一半的文件。
func (sn *Snapshot) ReadBlock(index int) ([]byte, error) {
	if index < 0 || index >= sn.size {
		return nil, fmt.Errorf("%w: index=%d size=%d", ErrOutOfRange, index, sn.size)
	}
	sn.store.metaMu.RLock()
	defer sn.store.metaMu.RUnlock()
	if sn.store.closed {
		return nil, ErrClosed
	}
	m := sn.store.meta.snapshots[sn.name]
	if m == nil {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, sn.name)
	}
	return sn.store.readPhysical(m.Blocks[index])
}

// Delete 删除该快照（语义同 Store.DeleteSnapshot）。
func (sn *Snapshot) Delete() error { return sn.store.DeleteSnapshot(sn.name) }

// Restore 将该快照还原为名为 volName 的卷。
func (sn *Snapshot) Restore(volName string) (*Volume, error) {
	return sn.store.RestoreSnapshot(sn.name, volName)
}
