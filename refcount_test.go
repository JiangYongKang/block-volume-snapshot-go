package blocksnap

import (
	"context"
	"errors"
	"testing"
)

// TestRefCountAndGC 覆盖引用计数边界：
// 写->快照->覆盖写->克隆->删除快照->删除卷，每一步共享块与新块的计数都要正确；
// GC 只能回收计数归零的块，仍被引用的块绝不能被误删。
func TestRefCountAndGC(t *testing.T) {
	const bs = 64
	s := newTestStore(t, Options{BlockSize: bs, DisableGC: true})
	must(t, s.CreateVolume("c", "v", bs), "create")

	old := pad([]byte("OLD"), bs)
	must(t, s.WriteAt("w1", "v", 0, old), "write old")
	oldID, _ := s.VolumeBlockID("v", 0)
	if got := s.BlockRefCount(oldID); got != 1 {
		t.Fatalf("after write refs=%d want 1", got)
	}

	must(t, s.CreateSnapshot("s1", "v", "snap"), "snapshot")
	if got := s.BlockRefCount(oldID); got != 2 {
		t.Fatalf("after snapshot refs=%d want 2", got)
	}

	// 覆盖写：旧块仍被快照引用(1)，新块被卷引用(1)。
	must(t, s.WriteAt("w2", "v", 0, pad([]byte("NEW"), bs)), "overwrite")
	newID, _ := s.VolumeBlockID("v", 0)
	if oldID == newID {
		t.Fatal("expected new physical block after overwrite")
	}
	if got := s.BlockRefCount(oldID); got != 1 {
		t.Fatalf("old block refs=%d want 1 (snapshot)", got)
	}
	if got := s.BlockRefCount(newID); got != 1 {
		t.Fatalf("new block refs=%d want 1", got)
	}

	// 克隆：克隆卷与快照共享 oldID，oldID 计数升到 2；newID 仍为 1。
	must(t, s.CloneVolume("cl", "snap", "clone", bs), "clone")
	if got := s.BlockRefCount(oldID); got != 2 {
		t.Fatalf("after clone old refs=%d want 2", got)
	}
	if data := readFull(t, s, "clone", bs); string(data[:3]) != "OLD" {
		t.Fatalf("clone content wrong: %q", data[:3])
	}

	// 第一次 GC：什么都不该回收。
	n, err := s.ForceGC(context.Background())
	must(t, err, "gc")
	if n != 0 {
		t.Fatalf("gc reclaimed %d blocks while all referenced", n)
	}

	// 删快照：oldID 仍被 clone 引用，不得回收。
	must(t, s.DeleteSnapshot("d1", "snap"), "delete snapshot")
	if got := s.BlockRefCount(oldID); got != 1 {
		t.Fatalf("after delete snap old refs=%d want 1 (clone)", got)
	}
	if n, _ := s.ForceGC(context.Background()); n != 0 {
		t.Fatalf("gc reclaimed %d, clone still holds old block", n)
	}
	if data := readFull(t, s, "clone", bs); string(data[:3]) != "OLD" {
		t.Fatal("clone data lost after snapshot deletion")
	}

	// 删除并重建克隆卷内容后，oldID 归零，可被安全回收；newID 被卷引用，不可回收。
	must(t, s.WriteAt("w3", "clone", 0, pad([]byte("X"), bs)), "write clone")
	if got := s.BlockRefCount(oldID); got != 0 {
		t.Fatalf("old refs=%d want 0", got)
	}
	if n, _ := s.ForceGC(context.Background()); n != 1 {
		t.Fatalf("gc reclaimed %d want exactly 1 (oldID only)", n)
	}
	if ok, _ := s.blocks.exists(oldID); ok {
		t.Fatal("old block file still on disk after gc")
	}
	if ok, _ := s.blocks.exists(newID); !ok {
		t.Fatal("new block wrongly removed while referenced by volume")
	}

	// 删除卷后 GC：newID 也要被回收。
	must(t, s.DeleteVolume("dv", "v"), "delete volume")
	if n, _ := s.ForceGC(context.Background()); n != 1 {
		t.Fatalf("gc after delete volume reclaimed %d want 1", n)
	}
	if ok, _ := s.blocks.exists(newID); ok {
		t.Fatal("new block still on disk after volume deleted")
	}
	t.Log("refcount/GC boundaries OK")
}

// TestIdempotentRetry 验证用同一 clientID 重试不会产生重复数据或重复快照，
// 引用计数也不会被重复累加。
func TestIdempotentRetry(t *testing.T) {
	const bs = 32
	s := newTestStore(t, Options{BlockSize: bs, DisableGC: true})
	must(t, s.CreateVolume("same-create", "v", 2*bs), "create")
	// 重复创建：不报错（幂等成功），也不产生第二个卷。
	must(t, s.CreateVolume("same-create", "v", 2*bs), "retry create")

	must(t, s.WriteAt("same-write", "v", 0, pad([]byte("Z"), bs)), "write")
	must(t, s.WriteAt("same-write", "v", 0, pad([]byte("Z"), bs)), "retry write")
	id, _ := s.VolumeBlockID("v", 0)
	if got := s.BlockRefCount(id); got != 1 {
		t.Fatalf("retry write inflated refs to %d", got)
	}

	must(t, s.CreateSnapshot("same-snap", "v", "snap"), "snap")
	must(t, s.CreateSnapshot("same-snap", "v", "snap"), "retry snap")
	if got := s.BlockRefCount(id); got != 2 {
		t.Fatalf("retry snapshot inflated refs to %d", got)
	}
	snaps, _ := s.ListSnapshots("v")
	if len(snaps) != 1 {
		t.Fatalf("retry created %d snapshots", len(snaps))
	}

	// 不同 clientID 但同名快照应明确报 AlreadyExists，失败原因可按类型区分。
	err := s.CreateSnapshot("other", "v", "snap")
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
	if err := s.Restore("no-such", "v", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	t.Log("idempotent retry OK; typed errors OK")
}
