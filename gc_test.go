package blockvol

import (
	"context"
	"errors"
	"io/fs"
	"testing"
)

func countPhysical(t *testing.T, s *Store) int {
	t.Helper()
	ids, err := s.bs.list()
	if err != nil {
		t.Fatal(err)
	}
	return len(ids)
}

// TestGCBoundaries 验证：
//   - 写后旧块在仍被快照引用时不被回收；
//   - 删除最后一个引用后，旧块可被回收；
//   - 被多个快照/克隆共享的块在任何一个引用存活时都不会被误删；
//   - GC 可重复运行（幂等），引用计数始终与元数据一致。
func TestGCBoundaries(t *testing.T) {
	ctx := context.Background()
	// 关闭后台 GC，全部手动触发。
	s, _ := openTestStore(t, WithGCInterval(0))
	bs := s.BlockSize()
	v, _ := s.CreateVolume("v", 2)

	_ = v.WriteBlock(0, testBlock(bs, 1))
	idV1 := blockID(testBlock(bs, 1))
	stats, err := s.RunGC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Reclaimed != 0 {
		t.Fatalf("nothing should be reclaimed yet, got %d", stats.Reclaimed)
	}

	snap1, _ := v.CreateSnapshot("s1")
	// COW 写：旧块 idV1 现在被 s1 引用，新块 idV2 被卷引用。
	_ = v.WriteBlock(0, testBlock(bs, 2))
	idV2 := blockID(testBlock(bs, 2))

	stats, _ = s.RunGC(ctx)
	if stats.Reclaimed != 0 {
		t.Fatalf("both blocks referenced, reclaimed=%d", stats.Reclaimed)
	}
	if ok, _ := s.bs.exists(idV1); !ok {
		t.Fatal("idV1 must survive while snapshot holds it")
	}
	if ok, _ := s.bs.exists(idV2); !ok {
		t.Fatal("idV2 must survive while volume holds it")
	}

	// 再打快照 s2（引用 idV2），然后把卷写到 idV3。
	_, _ = v.CreateSnapshot("s2")
	_ = v.WriteBlock(0, testBlock(bs, 3))
	stats, _ = s.RunGC(ctx)
	if stats.Reclaimed != 0 {
		t.Fatalf("s1->idV1, s2->idV2, vol->idV3; reclaimed=%d", stats.Reclaimed)
	}

	// 删除 s1：idV1 失去最后引用，应被回收；idV2/idV3 保留。
	if err := s.DeleteSnapshot("s1"); err != nil {
		t.Fatal(err)
	}
	stats, _ = s.RunGC(ctx)
	if stats.Reclaimed != 1 {
		t.Fatalf("expect exactly 1 reclaimed, got %d", stats.Reclaimed)
	}
	if ok, _ := s.bs.exists(idV1); ok {
		t.Fatal("idV1 should be reclaimed")
	}
	if exists, _ := s.bs.exists(idV2); !exists {
		t.Fatal("idV2 must stay (s2 references it)")
	}

	// 删卷再删 s2：idV2 与 idV3 都失去引用。
	if err := s.DeleteVolume("v"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteSnapshot("s2"); err != nil {
		t.Fatal(err)
	}
	if s.TotalRefs() != 0 {
		t.Fatalf("all refs gone, got %d", s.TotalRefs())
	}
	stats, _ = s.RunGC(ctx)
	if stats.Reclaimed != 2 {
		t.Fatalf("expect 2 reclaimed, got stats=%+v", stats)
	}
	if n := countPhysical(t, s); n != 0 {
		t.Fatalf("physical files left = %d, want 0", n)
	}

	// GC 再跑一次：稳定、无副作用。
	stats, _ = s.RunGC(ctx)
	if stats.Reclaimed != 0 || stats.Scanned != 0 {
		t.Fatalf("repeat GC not stable: %+v", stats)
	}
	_ = snap1
	if msg := s.CheckRefConsistency(); msg != "" {
		t.Fatalf("ref inconsistency: %s", msg)
	}
}

// TestGCKeepsClonedBlocks：克隆与源共享块，删除任一方都不能回收共享块。
func TestGCKeepsClonedBlocks(t *testing.T) {
	ctx := context.Background()
	s, _ := openTestStore(t, WithGCInterval(0))
	bs := s.BlockSize()
	v, _ := s.CreateVolume("v", 1)
	_ = v.WriteBlock(0, testBlock(bs, 5))
	shared := blockID(testBlock(bs, 5))
	clone, _ := s.CloneVolume("v", "c")

	// 源 COW 写后，shared 只被克隆引用；不能被删。
	_ = v.WriteBlock(0, testBlock(bs, 6))
	if _, err := s.RunGC(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.bs.exists(shared); !ok {
		t.Fatal("shared block must survive: clone holds it")
	}

	// 删源，克隆仍在。源最后写入的块(6)失去引用，本轮即可被回收；shared 保留。
	if err := s.DeleteVolume("v"); err != nil {
		t.Fatal(err)
	}
	st1, err := s.RunGC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g, err := clone.ReadBlock(0)
	if err != nil {
		t.Fatal(err)
	}
	if g[0] != 5 {
		t.Fatalf("clone content changed: %d", g[0])
	}

	// 删克隆，shared 才能被回收。
	if err := s.DeleteVolume("c"); err != nil {
		t.Fatal(err)
	}
	st2, _ := s.RunGC(ctx)
	if st1.Reclaimed+st2.Reclaimed != 2 {
		t.Fatalf("expect total 2 reclaimed (block6 then shared), got %d+%d",
			st1.Reclaimed, st2.Reclaimed)
	}
	if n := countPhysical(t, s); n != 0 {
		t.Fatalf("physical files left = %d, want 0", n)
	}
	if _, err := s.bs.read(shared); err == nil || !fsErrNotExist(err) {
		t.Fatalf("shared block should be gone, err=%v", err)
	}
}

func fsErrNotExist(err error) bool { return errors.Is(err, fs.ErrNotExist) }
