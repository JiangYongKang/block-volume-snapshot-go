package blockvol

import (
	"context"
	"errors"
	"testing"
)

// TestExclusiveLock：同一目录不能被两个 Store 同时打开；关闭后锁必须释放。
func TestExclusiveLock(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	_, err = Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open want ErrLocked, got %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatalf("reopen after close should succeed: %v", err)
	}
	_ = s3.Close()
}

// TestInProcessCrashReplay：通过 crashSimulateClose 模拟硬崩溃，
// 验证仅有 WAL（没有新检查点）的情况下重放能恢复全部已提交操作，
// 且重复打开结果稳定（恢复后已固化检查点，重放不会重复应用）。
func TestInProcessCrashReplay(t *testing.T) {
	dir := t.TempDir()
	open := func() *Store {
		s, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	s := open()
	bs := s.BlockSize()
	v, _ := s.CreateVolume("a", 3)
	_ = v.WriteBlock(0, testBlock(bs, 1))
	_ = v.WriteBlock(1, testBlock(bs, 2))
	_, _ = v.CreateSnapshot("ss")
	_ = v.WriteBlock(0, testBlock(bs, 9)) // 快照仍应看到 1
	refsBefore := s.TotalRefs()
	s.crashSimulateClose() // 不做检查点

	s2 := open()
	defer s2.Close()
	if s2.TotalRefs() != refsBefore {
		t.Fatalf("refs after replay = %d, want %d", s2.TotalRefs(), refsBefore)
	}
	v2, err := s2.GetVolume("a")
	if err != nil {
		t.Fatal(err)
	}
	g, _ := v2.ReadBlock(0)
	if g[0] != 9 {
		t.Fatalf("live block0 = %d, want 9", g[0])
	}
	sn2, err := s2.GetSnapshot("ss")
	if err != nil {
		t.Fatal(err)
	}
	g, _ = sn2.ReadBlock(0)
	if g[0] != 1 {
		t.Fatalf("snapshot block0 after replay = %d, want 1", g[0])
	}
	if msg := s2.CheckRefConsistency(); msg != "" {
		t.Fatalf("refs inconsistent after replay: %s", msg)
	}
	// GC 后只剩：卷引用 [9,2] + 快照引用 [1,2] = 3 个不同块。
	st, _ := s2.RunGC(context.Background())
	if st.LiveBlocks != 3 {
		t.Fatalf("live blocks = %d, want 3", st.LiveBlocks)
	}
}

// TestDeleteAllThenRecovery：所有卷与快照删除后崩溃，恢复后无残留引用，
// 且 GC 能清空全部物理块。
func TestDeleteAllThenRecovery(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	bs := s.BlockSize()
	v, _ := s.CreateVolume("v", 2)
	_ = v.WriteBlock(0, testBlock(bs, 4))
	_, _ = v.CreateSnapshot("s")
	_ = s.DeleteVolume("v")
	_ = s.DeleteSnapshot("s")
	if s.TotalRefs() != 0 {
		t.Fatalf("refs = %d, want 0", s.TotalRefs())
	}
	s.crashSimulateClose()

	s2, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.TotalRefs() != 0 {
		t.Fatalf("refs after reopen = %d, want 0", s2.TotalRefs())
	}
	st, _ := s2.RunGC(context.Background())
	if st.LiveBlocks != 0 {
		t.Fatalf("live blocks = %d, want 0", st.LiveBlocks)
	}
	if n := len(s2.ListVolumes()) + len(s2.ListSnapshots()); n != 0 {
		t.Fatalf("names left: %d", n)
	}
}
