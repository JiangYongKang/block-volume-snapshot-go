package blocksnap

import (
	"context"
	"testing"
)

func openAt(t *testing.T, dir string, bs int) *Store {
	t.Helper()
	s, err := Open(dir, Options{BlockSize: bs, DisableGC: true, Logf: t.Logf})
	must(t, err, "open")
	return s
}

// TestRestartRoundTrip：正常关闭（触发检查点）后重开，卷/快照/数据/引用计数全部保留。
func TestRestartRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const bs = 64
	s := openAt(t, dir, bs)
	must(t, s.CreateVolume("c", "v", 2*bs), "create")
	must(t, s.WriteAt("w1", "v", 0, pad([]byte("FIRST"), bs)), "write 1")
	must(t, s.WriteAt("w2", "v", bs, pad([]byte("SECOND"), bs)), "write 2")
	must(t, s.CreateSnapshot("s", "v", "snap"), "snap")
	must(t, s.WriteAt("w3", "v", 0, pad([]byte("LATER"), bs)), "overwrite")
	oldID, _ := s.SnapshotBlockID("snap", 0)
	if rc := s.BlockRefCount(oldID); rc != 1 {
		t.Fatalf("pre-close refs=%d want 1", rc)
	}
	must(t, s.Close(), "close")

	s2 := openAt(t, dir, bs)
	defer s2.Close()
	if data := readFull(t, s2, "v", 2*bs); string(data[:5]) != "LATER" || string(data[bs:bs+6]) != "SECOND" {
		t.Fatalf("data not preserved across restart: %q / %q", data[:5], data[bs:bs+6])
	}
	id, ok := s2.SnapshotBlockID("snap", 0)
	if !ok || id != oldID {
		t.Fatal("snapshot mapping not preserved across restart")
	}
	if rc := s2.BlockRefCount(oldID); rc != 1 {
		t.Fatalf("post-restart refs=%d want 1", rc)
	}
	// 幂等键也持久化：重复 s 不产生新快照。
	must(t, s2.CreateSnapshot("s", "v", "snap"), "idempotent after restart")
	if snaps, _ := s2.ListSnapshots("v"); len(snaps) != 1 {
		t.Fatalf("idempotency not persisted: %v", snaps)
	}
	// 还原在重启后仍工作。
	must(t, s2.Restore("r", "v", "snap"), "restore after restart")
	if data := readFull(t, s2, "v", bs); string(data[:5]) != "FIRST" {
		t.Fatalf("restore after restart wrong: %q", data[:5])
	}
}

// TestDeletedVolumeRestoreAcrossRestart：卷删除 -> 关闭 -> 重开 -> 快照仍在、可还原。
func TestDeletedVolumeRestoreAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	const bs = 32
	s := openAt(t, dir, bs)
	must(t, s.CreateVolume("c", "v", bs), "create")
	must(t, s.WriteAt("w", "v", 0, pad([]byte("KEEPME"), bs)), "write")
	must(t, s.CreateSnapshot("s", "v", "snap"), "snap")
	must(t, s.DeleteVolume("d", "v"), "delete")
	must(t, s.Close(), "close")

	s2 := openAt(t, dir, bs)
	defer s2.Close()
	if err := s2.Restore("r2", "v", "snap"); err != nil {
		t.Fatalf("restore deleted volume across restart: %v", err)
	}
	if data := readFull(t, s2, "v", bs); string(data[:6]) != "KEEPME" {
		t.Fatalf("restored data wrong: %q", data[:6])
	}
	// GC 在重启后也不得回收仍被快照+卷引用的块。
	if n, err := s2.ForceGC(context.Background()); err != nil || n != 0 {
		t.Fatalf("gc after restart n=%d err=%v", n, err)
	}
}

// TestRepeatedRestartsNoDrift：多次重启后引用计数和块数保持稳定，不随重放漂移。
func TestRepeatedRestartsNoDrift(t *testing.T) {
	dir := t.TempDir()
	const bs = 16
	s := openAt(t, dir, bs)
	must(t, s.CreateVolume("c", "v", bs), "create")
	must(t, s.WriteAt("w", "v", 0, pad([]byte("A"), bs)), "write a")
	must(t, s.CreateSnapshot("s1", "v", "s1"), "s1")
	must(t, s.WriteAt("w2", "v", 0, pad([]byte("B"), bs)), "write b")
	must(t, s.CreateSnapshot("s2", "v", "s2"), "s2")
	must(t, s.Close(), "close")

	prev := -1
	for i := 0; i < 3; i++ {
		s = openAt(t, dir, bs)
		ids, err := s.blocks.ids()
		must(t, err, "ids")
		if prev >= 0 && len(ids) != prev {
			t.Fatalf("block count drifted across restarts: %d -> %d", prev, len(ids))
		}
		prev = len(ids)
		n, err := s.ForceGC(context.Background())
		must(t, err, "gc")
		if n != 0 {
			t.Fatalf("restart %d spuriously reclaimed %d blocks", i, n)
		}
		must(t, s.Close(), "close")
	}
}
