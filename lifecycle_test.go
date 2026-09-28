package blocksnap

import (
	"errors"
	"testing"
)

// TestDeleteVolumeStillRestorable：卷删除后只要快照还在，数据必须可还原。
func TestDeleteVolumeStillRestorable(t *testing.T) {
	const bs = 64
	s := newTestStore(t, Options{BlockSize: bs, DisableGC: true})
	must(t, s.CreateVolume("c", "v", bs), "create")
	must(t, s.WriteAt("w", "v", 0, pad([]byte("DATA"), bs)), "write")
	must(t, s.CreateSnapshot("s", "v", "snap"), "snap")
	must(t, s.DeleteVolume("d", "v"), "delete")

	// 卷标记为删除但元数据保留。
	infos, _ := s.ListVolumes()
	var found bool
	for _, info := range infos {
		if info.Name == "v" {
			found = true
			if !info.Deleted {
				t.Fatal("volume should be marked deleted")
			}
		}
	}
	if !found {
		t.Fatal("deleted-but-referenced volume metadata must be retained")
	}
	if _, err := s.ReadAt("v", 0, make([]byte, 1)); err == nil {
		t.Fatal("reads on deleted volume should fail until restored")
	}

	// 用快照还原到原名，卷复活，内容一致。
	must(t, s.Restore("r", "v", "snap"), "restore deleted volume")
	if data := readFull(t, s, "v", bs); string(data[:4]) != "DATA" {
		t.Fatalf("restored content wrong: %q", data[:4])
	}
}

// TestDeleteSnapshotPrunesVolume：最后一个快照删除后，无引用的已删卷元数据消失。
func TestDeleteSnapshotPrunesVolume(t *testing.T) {
	const bs = 16
	s := newTestStore(t, Options{BlockSize: bs, DisableGC: true})
	must(t, s.CreateVolume("c", "v", bs), "create")
	must(t, s.WriteAt("w", "v", 0, pad([]byte("Q"), bs)), "write")
	must(t, s.CreateSnapshot("s", "v", "snap"), "snap")
	must(t, s.DeleteVolume("d", "v"), "delete")
	must(t, s.DeleteSnapshot("ds", "snap"), "delete snap")

	if vols, _ := s.ListVolumes(); len(vols) != 0 {
		t.Fatalf("expected volume metadata pruned, got %v", vols)
	}
	// 再还原不存在的快照应返回 NotFound（类型可区分）。
	if err := s.Restore("r", "v", "snap"); !isNotFound(err) {
		t.Fatalf("want not-found, got %v", err)
	}
}

// TestCloneIndependence：克隆卷与源卷共享旧块，但写入后分叉、互不影响。
func TestCloneIndependence(t *testing.T) {
	const bs = 32
	s := newTestStore(t, Options{BlockSize: bs, DisableGC: true})
	must(t, s.CreateVolume("c", "v", bs), "create")
	must(t, s.WriteAt("w", "v", 0, pad([]byte("SRC"), bs)), "write")
	must(t, s.CreateSnapshot("s", "v", "snap"), "snap")
	must(t, s.CloneVolume("cl", "snap", "clone", bs), "clone")

	sharedID, _ := s.SnapshotBlockID("snap", 0)
	cloneID, _ := s.VolumeBlockID("clone", 0)
	if sharedID != cloneID {
		t.Fatal("clone should initially share physical blocks with snapshot")
	}

	must(t, s.WriteAt("wc", "clone", 0, pad([]byte("CLONE"), bs)), "write clone")
	if data := readFull(t, s, "v", bs); string(data[:3]) != "SRC" {
		t.Fatalf("source changed after clone write: %q", data[:3])
	}
	if data := readFull(t, s, "clone", bs); string(data[:5]) != "CLONE" {
		t.Fatalf("clone content wrong: %q", data[:5])
	}
	if id, _ := s.VolumeBlockID("clone", 0); id == sharedID {
		t.Fatal("clone write must diverge via COW")
	}
}

// TestSparseReads：未写入区域读零，跨块部分写走读-改-写且保持其余字节。
func TestSparseAndPartialWrite(t *testing.T) {
	const bs = 64
	s := newTestStore(t, Options{BlockSize: bs, DisableGC: true})
	must(t, s.CreateVolume("c", "v", 3*bs), "create")

	// 稀疏区域为零。
	if b := readFull(t, s, "v", 3*bs); !allZero(b) {
		t.Fatal("fresh volume must read zeros")
	}
	// 非对齐部分写：只改 4 个字节。
	must(t, s.WriteAt("w1", "v", 10, []byte("HELLO")), "partial write")
	b := readFull(t, s, "v", 3*bs)
	if string(b[10:15]) != "HELLO" {
		t.Fatalf("partial write lost: %q", b[10:15])
	}
	for i := 0; i < 10; i++ {
		if b[i] != 0 {
			t.Fatalf("byte before write region corrupted: %d", b[i])
		}
	}
	for i := 15; i < 3*bs; i++ {
		if b[i] != 0 {
			t.Fatalf("byte after write region corrupted at %d", i)
		}
	}
	// 跨块写（覆盖块 2 末尾与块 3 开头）。
	must(t, s.WriteAt("w2", "v", bs-2, []byte("XXYY")), "cross-block write")
	b = readFull(t, s, "v", 3*bs)
	if string(b[bs-2:bs+2]) != "XXYY" {
		t.Fatalf("cross-block write wrong: %q", b[bs-2:bs+2])
	}
	if string(b[10:15]) != "HELLO" {
		t.Fatal("earlier partial write lost after cross-block write")
	}
}

func isNotFound(err error) bool { return errors.Is(err, ErrNotFound) }

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}
