package blocksnap

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentWritesAndSnapshot 验证：快照与多写者并发时，快照捕获一致时点、
// 不出现部分可见；快照之后的写入绝不污染快照。
func TestConcurrentWritesAndSnapshot(t *testing.T) {
	const (
		blockSize = 256 // 小一点的块，便于精确触发跨块写
		nBlocks   = 16
		volSize   = blockSize * nBlocks
		nWriters  = 8
	)
	s := newTestStore(t, Options{BlockSize: blockSize})
	must(t, s.CreateVolume("req-create", "vol", volSize), "create volume")

	// 初始内容：第 i 块填 i。
	for i := 0; i < nBlocks; i++ {
		buf := make([]byte, blockSize)
		for j := range buf {
			buf[j] = byte(i)
		}
		must(t, s.WriteAt(fmt.Sprintf("req-init-%d", i), "vol", int64(i*blockSize), buf), "init write")
	}

	// 抓一个“初始版本”快照。
	must(t, s.CreateSnapshot("req-snap0", "vol", "snap0"), "create snap0")

	var wg sync.WaitGroup
	// 每个写者反复把自己负责的块覆盖为写者编号，制造持续 COW。
	for w := 0; w < nWriters; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for round := 0; round < 20; round++ {
				buf := make([]byte, blockSize)
				for j := range buf {
					buf[j] = byte(100 + id)
				}
				idx := (id + round) % nBlocks
				if err := s.WriteAt(fmt.Sprintf("req-w-%d-%d", id, round), "vol",
					int64(idx*blockSize), buf); err != nil {
					t.Errorf("writer %d round %d: %v", id, round, err)
					return
				}
			}
		}(w)
	}

	// 并发生成多个快照：每个快照必须是自洽的——每块内容都是某次完整写入，
	// 绝不能出现一个逻辑块内前后两半来自不同版本的撕裂。
	var snapMu sync.Mutex
	var snaps []string
	for k := 0; k < 5; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			name := fmt.Sprintf("snapC%d", k)
			if err := s.CreateSnapshot(fmt.Sprintf("req-snapc-%d", k), "vol", name); err != nil {
				t.Errorf("snapshot %d: %v", k, err)
				return
			}
			snapMu.Lock()
			snaps = append(snaps, name)
			snapMu.Unlock()
		}(k)
	}
	wg.Wait()

	verify := func(snap string) {
		for i := 0; i < nBlocks; i++ {
			id, ok := s.SnapshotBlockID(snap, uint64(i))
			if !ok {
				t.Fatalf("%s: missing block %d", snap, i)
			}
			data, err := s.blocks.read(id)
			if err != nil {
				t.Fatalf("read snapshot block: %v", err)
			}
			first := data[0]
			for _, b := range data {
				if b != first {
					t.Fatalf("%s block %d torn: mixed bytes %d/%d", snap, i, first, b)
				}
			}
		}
	}
	for _, name := range snaps {
		verify(name)
	}

	// snap0 必须保持初始版本（后续并发写全部走 COW 新块）。
	for i := 0; i < nBlocks; i++ {
		id, _ := s.SnapshotBlockID("snap0", uint64(i))
		data, err := s.blocks.read(id)
		must(t, err, "read snap0 block")
		for _, b := range data {
			if b != byte(i) {
				t.Fatalf("snap0 polluted by later write at block %d: got %d want %d", i, b, i)
			}
		}
	}
	t.Logf("concurrency OK: %d concurrent snapshots, snap0 untouched", len(snaps)+1)
}

// TestSnapshotPointInTime 在单线程下严格验证快照时点与 COW：
// 快照块 id 在覆盖写之后不变，还原后逐字节一致。
func TestSnapshotPointInTime(t *testing.T) {
	const bs = 128
	s := newTestStore(t, Options{BlockSize: bs})
	must(t, s.CreateVolume("c", "v", 4*bs), "create")

	a := []byte("AAAA")
	b := []byte("BBBB")
	must(t, s.WriteAt("w1", "v", 0, pad(a, bs)), "write A")
	must(t, s.CreateSnapshot("s1", "v", "snap1"), "snap1")
	idAtSnap, _ := s.VolumeBlockID("v", 0)

	must(t, s.WriteAt("w2", "v", 0, pad(b, bs)), "overwrite B")
	idAfterWrite, _ := s.VolumeBlockID("v", 0)
	snapID, _ := s.SnapshotBlockID("snap1", 0)

	if idAtSnap == idAfterWrite {
		t.Fatalf("COW failed: block id unchanged after overwrite")
	}
	if snapID != idAtSnap {
		t.Fatalf("snapshot block drifted: %s != %s", snapID[:8], idAtSnap[:8])
	}
	if got := readFull(t, s, "v", bs); string(got[:4]) != "BBBB" {
		t.Fatalf("live volume should read latest: %q", got[:4])
	}

	must(t, s.Restore("r1", "v", "snap1"), "restore")
	if got := readFull(t, s, "v", bs); string(got[:4]) != "AAAA" {
		t.Fatalf("restore mismatch: %q", got[:4])
	}
	if id, _ := s.VolumeBlockID("v", 0); id != idAtSnap {
		t.Fatalf("restore did not point back to snapshot block")
	}
	t.Logf("point-in-time OK: oldBlock=%s newBlock=%s refs(old)=%d",
		idAtSnap[:8], idAfterWrite[:8], s.BlockRefCount(idAtSnap))
}

func pad(prefix []byte, size int) []byte {
	b := make([]byte, size)
	copy(b, prefix)
	return b
}

// TestBackgroundGCConcurrentWrites 验证后台回收与持续读写/快照并发时：
// 不删除仍被引用的块、数据始终可读，运行结束后磁盘块集合与引用集合一致。
func TestBackgroundGCConcurrentWrites(t *testing.T) {
	const (
		bs      = 32
		nBlocks = 8
	)
	s := newTestStore(t, Options{BlockSize: bs, GCInterval: 10_000_000 /*10ms*/})
	must(t, s.CreateVolume("c", "v", bs*nBlocks), "create")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 持续校验读取：任何时刻被引用的块都必须在磁盘上
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			buf := make([]byte, bs*nBlocks)
			if _, err := s.ReadAt("v", 0, buf); err != nil {
				t.Errorf("read during gc: %v", err)
				return
			}
		}
	}()
	for i := 0; i < nBlocks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for r := 0; r < 30; r++ {
				if err := s.WriteAt(fmt.Sprintf("w-%d-%d", i, r), "v",
					int64(i*bs), pad([]byte{byte(r)}, bs)); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(i)
	}
	for k := 0; k < 10; k++ {
		must(t, s.CreateSnapshot(fmt.Sprintf("s-%d", k), "v", fmt.Sprintf("bgSnap%d", k)), "bg snap")
	}
	close(stop)
	wg.Wait()

	// 删掉一半快照制造大量零引用块，等后台 GC 回收。
	for k := 0; k < 10; k += 2 {
		must(t, s.DeleteSnapshot(fmt.Sprintf("ds-%d", k), fmt.Sprintf("bgSnap%d", k)), "delete snap")
	}
	n, err := s.ForceGC(context.Background())
	must(t, err, "force gc")
	t.Logf("background GC test: force reclaimed=%d", n)

	s.mu.RLock()
	refs := s.meta.blockRefs()
	s.mu.RUnlock()
	ids, _ := s.blocks.ids()
	if len(ids) != len(refs) {
		t.Fatalf("disk blocks %d != referenced blocks %d after gc", len(ids), len(refs))
	}
	for _, id := range ids {
		if _, ok := refs[id]; !ok {
			t.Fatalf("unreferenced block survived: %s", id[:8])
		}
	}
	readFull(t, s, "v", bs*nBlocks) // 最终数据必须完整可读
}
