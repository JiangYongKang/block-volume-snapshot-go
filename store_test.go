package blockvol

import (
	"bytes"
	"errors"
	"testing"
)

// testLogger 把关键操作日志打到 testing 日志（go test -v 可见）。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(f string, a ...any) { l.t.Logf("[store-log] "+f, a...) }

func testBlock(bs int, v byte) []byte {
	b := make([]byte, bs)
	for i := range b {
		b[i] = v
	}
	return b
}

func openTestStore(t *testing.T, opts ...Option) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	all := append([]Option{WithLogger(testLogger{t})}, opts...)
	s, err := Open(dir, all...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func assertBlock(t *testing.T, got []byte, want byte, bs int, where string) {
	t.Helper()
	exp := testBlock(bs, want)
	if !bytes.Equal(got, exp) {
		t.Fatalf("%s: block mismatch: got first=%d ... want all=%d", where, got[0], want)
	}
}

func TestCreateWriteRead(t *testing.T) {
	s, _ := openTestStore(t)
	bs := s.BlockSize()
	v, err := s.CreateVolume("v1", 4)
	if err != nil {
		t.Fatal(err)
	}
	// 初始块为全零。
	got, err := v.ReadBlock(0)
	if err != nil || !bytes.Equal(got, testBlock(bs, 0)) {
		t.Fatalf("initial block wrong: %v", err)
	}
	if err := v.WriteBlock(1, testBlock(bs, 0xAA)); err != nil {
		t.Fatal(err)
	}
	got, _ = v.ReadBlock(1)
	assertBlock(t, got, 0xAA, bs, "after write")
	got, _ = v.ReadBlock(0)
	assertBlock(t, got, 0x00, bs, "untouched block")

	// 越界与错误长度。
	if _, err := v.ReadBlock(4); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("want ErrOutOfRange, got %v", err)
	}
	if err := v.WriteBlock(0, make([]byte, bs-1)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
	// 重名。
	if _, err := s.CreateVolume("v1", 1); !errors.Is(err, ErrVolumeExists) {
		t.Fatalf("want ErrVolumeExists, got %v", err)
	}
}

func TestSnapshotIsolationAndRestore(t *testing.T) {
	s, _ := openTestStore(t)
	bs := s.BlockSize()
	v, _ := s.CreateVolume("vol", 2)
	if err := v.WriteBlock(0, testBlock(bs, 1)); err != nil {
		t.Fatal(err)
	}
	snap, err := v.CreateSnapshot("snap1")
	if err != nil {
		t.Fatal(err)
	}

	// 快照之后的写不得污染快照。
	if err := v.WriteBlock(0, testBlock(bs, 2)); err != nil {
		t.Fatal(err)
	}
	got, _ := v.ReadBlock(0)
	assertBlock(t, got, 2, bs, "live volume")
	got, _ = snap.ReadBlock(0)
	assertBlock(t, got, 1, bs, "snapshot must stay at point-in-time")

	// 再打一个快照，确认两个时点各自独立。
	snap2, _ := v.CreateSnapshot("snap2")
	if err := v.WriteBlock(0, testBlock(bs, 3)); err != nil {
		t.Fatal(err)
	}
	g1, _ := snap.ReadBlock(0)
	g2, _ := snap2.ReadBlock(0)
	assertBlock(t, g1, 1, bs, "snap1")
	assertBlock(t, g2, 2, bs, "snap2")

	// 还原 snap1：卷内容必须回到时点 1。
	if err := v.Restore(snap); err != nil {
		t.Fatal(err)
	}
	got, _ = v.ReadBlock(0)
	assertBlock(t, got, 1, bs, "after restore snap1")
	if msg := s.CheckRefConsistency(); msg != "" {
		t.Fatalf("ref inconsistency: %s", msg)
	}
}

func TestDeleteVolumeThenSnapshotRestore(t *testing.T) {
	s, _ := openTestStore(t)
	bs := s.BlockSize()
	v, _ := s.CreateVolume("vol", 2)
	_ = v.WriteBlock(0, testBlock(bs, 7))
	_ = v.WriteBlock(1, testBlock(bs, 8))
	snap, _ := v.CreateSnapshot("snap")
	if s.TotalRefs() != 4 { // 2 in vol + 2 in snapshot
		t.Fatalf("refs before delete volume = %d, want 4", s.TotalRefs())
	}

	// 删卷：快照仍持有块，ref 减半但物理块不丢。
	if err := s.DeleteVolume("vol"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetVolume("vol"); !errors.Is(err, ErrVolumeNotFound) {
		t.Fatalf("want ErrVolumeNotFound, got %v", err)
	}
	if s.TotalRefs() != 2 {
		t.Fatalf("refs after delete volume = %d, want 2", s.TotalRefs())
	}
	got, _ := snap.ReadBlock(0)
	assertBlock(t, got, 7, bs, "snapshot readable after volume deleted")

	// 用快照重建同名卷（源卷已删，只要快照在就能还原）。
	v2, err := s.RestoreSnapshot("snap", "vol")
	if err != nil {
		t.Fatal(err)
	}
	got, _ = v2.ReadBlock(0)
	assertBlock(t, got, 7, bs, "restored block 0")
	got, _ = v2.ReadBlock(1)
	assertBlock(t, got, 8, bs, "restored block 1")
	if s.TotalRefs() != 4 {
		t.Fatalf("refs after restore = %d, want 4", s.TotalRefs())
	}
}

func TestCloneSharesBlocks(t *testing.T) {
	s, _ := openTestStore(t)
	bs := s.BlockSize()
	v, _ := s.CreateVolume("v", 2)
	_ = v.WriteBlock(0, testBlock(bs, 9))
	clone, err := s.CloneVolume("v", "c")
	if err != nil {
		t.Fatal(err)
	}
	if s.TotalRefs() != 2 { // 同一个块被两个卷引用 -> 计数 2
		t.Fatalf("refs after clone = %d, want 2", s.TotalRefs())
	}
	// 克隆内容与源一致。
	g, _ := clone.ReadBlock(0)
	assertBlock(t, g, 9, bs, "clone content")

	// 源的 COW 写不影响克隆；两者写同一逻辑块后计数行为正确。
	_ = v.WriteBlock(0, testBlock(bs, 10))
	g, _ = clone.ReadBlock(0)
	assertBlock(t, g, 9, bs, "clone isolated from source write")
	g, _ = v.ReadBlock(0)
	assertBlock(t, g, 10, bs, "source changed")

	// 删除源卷，克隆仍然可读，块计数不下降到 0。
	if err := s.DeleteVolume("v"); err != nil {
		t.Fatal(err)
	}
	g, _ = clone.ReadBlock(0)
	assertBlock(t, g, 9, bs, "clone survives source deletion")
}

func TestIdempotentDeletes(t *testing.T) {
	s, _ := openTestStore(t)
	bs := s.BlockSize()
	v, _ := s.CreateVolume("v", 1)
	if err := v.WriteBlock(0, testBlock(bs, 1)); err != nil {
		t.Fatal(err)
	}
	snap, _ := v.CreateSnapshot("s")
	before := s.TotalRefs() // 1 个块被卷与快照各引用一次 -> 2
	if before != 2 {
		t.Fatalf("refs before deletes = %d, want 2", before)
	}
	// 卷删除两次：第二次幂等成功且引用计数不变。
	if err := s.DeleteVolume("v"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteVolume("v"); err != nil {
		t.Fatalf("second DeleteVolume should be idempotent, got %v", err)
	}
	if err := s.DeleteSnapshot("s"); err != nil {
		t.Fatal(err)
	}
	if s.TotalRefs() != before-2 {
		t.Fatalf("refs after idempotent deletes = %d, want %d", s.TotalRefs(), before-2)
	}
	// 快照再删：得到 NotFound（可分类），且没有改变任何计数。
	err := s.DeleteSnapshot("s")
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("want ErrSnapshotNotFound, got %v", err)
	}
	if err := snap.Delete(); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("handle Delete want ErrSnapshotNotFound, got %v", err)
	}
}

func TestErrorTypes(t *testing.T) {
	s, _ := openTestStore(t)
	if _, err := s.GetVolume("nope"); !errors.Is(err, ErrVolumeNotFound) {
		t.Fatalf("got %v", err)
	}
	if _, err := s.GetSnapshot("nope"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("got %v", err)
	}
	if _, err := s.RestoreSnapshot("nope", "x"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("got %v", err)
	}
	if _, err := s.CreateVolume("", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("double close want ErrClosed, got %v", err)
	}
	if names := s.ListVolumes(); len(names) != 0 {
		t.Fatalf("after close list should be empty, got %v", names)
	}
}
