package blockvol

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"time"
)

// Volume 是一个可随机读写的逻辑块卷。
type Volume struct {
	store *Store
	name  string
}

// Name 返回卷名。
func (v *Volume) Name() string { return v.name }

// SizeBlocks 返回卷的逻辑块数；卷已删除时返回 0。
func (v *Volume) SizeBlocks() int {
	v.store.metaMu.RLock()
	defer v.store.metaMu.RUnlock()
	if vm := v.store.meta.volumes[v.name]; vm != nil && !vm.Deleted {
		return vm.Size
	}
	return 0
}

// CreatedAt 返回卷创建时间。
func (v *Volume) CreatedAt() time.Time {
	v.store.metaMu.RLock()
	defer v.store.metaMu.RUnlock()
	if vm := v.store.meta.volumes[v.name]; vm != nil {
		return time.Unix(0, vm.CreatedAt)
	}
	return time.Time{}
}

// ReadBlock 读取第 index 个逻辑块，返回长度等于块大小的字节切片。
// 读路径在 RLock 下完成“查映射 + 读物理文件”，与 GC 删除互斥，与其他读并发。
func (v *Volume) ReadBlock(index int) ([]byte, error) {
	s := v.store
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	vm := s.meta.volumes[v.name]
	if vm == nil || vm.Deleted {
		return nil, fmt.Errorf("%w: %s", ErrVolumeNotFound, v.name)
	}
	if index < 0 || index >= vm.Size {
		return nil, fmt.Errorf("%w: index=%d size=%d", ErrOutOfRange, index, vm.Size)
	}
	return s.readPhysical(vm.Blocks[index])
}

// readPhysical 返回物理块内容；id 为空表示从未写过的全零块。
// 调用方必须持有 metaMu（读锁或写锁）。
func (s *Store) readPhysical(id string) ([]byte, error) {
	if id == "" {
		return bytes.Repeat([]byte{0}, s.blockSize), nil
	}
	data, err := s.bs.read(id)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// 不变量被破坏（引用存在而文件丢失），按数据损坏显式失败而不是返回错误数据。
			return nil, fmt.Errorf("referenced physical block %s missing: %w", id, err)
		}
		return nil, err
	}
	return data, nil
}

// WriteBlock 覆盖写第 index 个逻辑块。
//
// 写时复制（COW）：新内容先作为“新物理块”落盘（绝不修改旧块），WAL 提交后
// 才把卷的逻辑映射切到新块。因此：
//   - 进行中的写不会影响任何既有快照（快照仍指向旧块）；
//   - 整个过程在写锁内，读者要么看到旧块、要么看到新块，不会看到部分写；
//   - 写完全零等价于空槽（读回仍是全零），不占用物理块。
func (v *Volume) WriteBlock(index int, data []byte) error {
	s := v.store
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if len(data) != s.blockSize {
		return fmt.Errorf("%w: data length %d != block size %d",
			ErrInvalidArgument, len(data), s.blockSize)
	}
	vm := s.meta.volumes[v.name]
	if vm == nil || vm.Deleted {
		return fmt.Errorf("%w: %s", ErrVolumeNotFound, v.name)
	}
	if index < 0 || index >= vm.Size {
		return fmt.Errorf("%w: index=%d size=%d", ErrOutOfRange, index, vm.Size)
	}

	physical := blockID(data)
	if isAllZero(data) {
		physical = "" // 全零不占物理块
	}
	// 幂等快路径：映射已经指向相同内容（重试写），不重复落盘也不重复记账。
	if vm.Blocks[index] == physical {
		return nil
	}
	if physical != "" {
		// 先落盘新块。内容寻址保证与别的写者写同内容时幂等复用。
		// 落盘在写锁内，排除 GC 把“刚写好、尚未提交引用”的块当孤儿删除的窗口。
		id, err := s.bs.write(data)
		if err != nil {
			return err
		}
		physical = id
		if vm.Blocks[index] == physical {
			return nil
		}
	}
	rec := &walRecord{
		Op:          walWriteBlock,
		Volume:      v.name,
		Index:       index,
		Physical:    physical,
		OldPhysical: vm.Blocks[index],
	}
	if err := s.commit(rec); err != nil {
		return err
	}
	s.logf("write-block volume=%s index=%d ref=%s oldRef=%s refsAfter=%d",
		v.name, index, shortID(physical), shortID(rec.OldPhysical), s.totalRefs())
	return nil
}

// CreateSnapshot 对该卷打快照。
func (v *Volume) CreateSnapshot(snapName string) (*Snapshot, error) {
	return v.store.CreateSnapshot(v.name, snapName)
}

// Restore 用快照内容覆盖当前卷（容量必须一致）。
func (v *Volume) Restore(snap *Snapshot) error {
	if snap == nil {
		return fmt.Errorf("%w: nil snapshot", ErrInvalidArgument)
	}
	_, err := v.store.RestoreSnapshot(snap.Name(), v.name)
	return err
}

// Delete 删除该卷（语义同 Store.DeleteVolume）。
func (v *Volume) Delete() error { return v.store.DeleteVolume(v.name) }

func isAllZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

func shortID(id string) string {
	if id == "" {
		return "-"
	}
	if len(id) > 10 {
		return id[:10]
	}
	return id
}
