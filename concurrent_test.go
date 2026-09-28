package blockvol

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentWritersAndSnapshots：
//   - 多个 writer 各写互不相交的逻辑块，但每个块先写值 a 再写值 b；
//   - 并发 reader 不断读取，校验“读到的块是整块同一个字节”，绝不出现撕裂；
//   - 并发地反复打快照；每个快照必须对应某个合法时点：
//     任意块的内容只能是 0 / a / b，且一旦出现 b，后续快照同块也必须是 b
//     （单调，因为不存在把 b 改回 a 的操作）；
//   - 全程引用计数必须与元数据全量计算保持一致。
func TestConcurrentWritersAndSnapshots(t *testing.T) {
	s, _ := openTestStore(t)
	bs := s.BlockSize()
	const nBlocks = 16
	const nWriters = 4
	v, _ := s.CreateVolume("cv", nBlocks)

	stop := make(chan struct{})
	var readerWG, writerWG sync.WaitGroup

	// readers：读到的块必须整块相同（无部分写），值只能是 0/A/B。
	for r := 0; r < 4; r++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := (int(time.Now().UnixNano())) % nBlocks
				b, err := v.ReadBlock(i)
				if err != nil {
					if errors.Is(err, ErrVolumeNotFound) {
						return
					}
					t.Errorf("read err: %v", err)
					return
				}
				if len(b) != bs {
					t.Errorf("bad block len %d", len(b))
					return
				}
				first := b[0]
				if first != 0 && first != 'A' && first != 'B' {
					t.Errorf("unexpected value %d", first)
					return
				}
				for _, c := range b {
					if c != first {
						t.Errorf("torn read: block %d mixed bytes", i)
						return
					}
				}
			}
		}()
	}

	// writers：每个块依次写 A 再写 B（块按 writer 取模分区）。
	for w := 0; w < nWriters; w++ {
		writerWG.Add(1)
		go func(id int) {
			defer writerWG.Done()
			for i := id; i < nBlocks; i += nWriters {
				if err := v.WriteBlock(i, testBlock(bs, 'A')); err != nil {
					t.Errorf("write A: %v", err)
					return
				}
				if err := v.WriteBlock(i, testBlock(bs, 'B')); err != nil {
					t.Errorf("write B: %v", err)
					return
				}
			}
		}(w)
	}

	// snapshotters：并发打快照并即时记录句柄。
	var snapMu sync.Mutex
	var snaps []*Snapshot
	snapDone := make(chan struct{})
	go func() {
		defer close(snapDone)
		n := 0
		for n < 20 {
			name := fmt.Sprintf("csnap-%d-%d", n, time.Now().UnixNano())
			sn, err := v.CreateSnapshot(name)
			if err != nil {
				if errors.Is(err, ErrVolumeNotFound) {
					return
				}
				t.Errorf("create snapshot: %v", err)
				return
			}
			snapMu.Lock()
			snaps = append(snaps, sn)
			snapMu.Unlock()
			n++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// writer 完成后，通过轮询卷最终状态确认（所有块都变 B 即结束）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allB := true
		for i := 0; i < nBlocks; i++ {
			b, _ := v.ReadBlock(i)
			if b[0] != 'B' {
				allB = false
				break
			}
		}
		if allB {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	<-snapDone
	readerWG.Wait()
	writerWG.Wait()

	// 最终状态：所有块都必须是 B（A 的物理块如果没有快照引用会被后续 GC 回收）。
	for i := 0; i < nBlocks; i++ {
		b, _ := v.ReadBlock(i)
		if !bytes.Equal(b, testBlock(bs, 'B')) {
			t.Fatalf("final block %d not all B", i)
		}
	}

	// 校验每个快照：只能包含 0/A/B，且随快照时间推进同块值单调 0<=A<=B。
	// 快照按创建顺序排列。
	snapMu.Lock()
	collected := append([]*Snapshot(nil), snaps...)
	snapMu.Unlock()
	if len(collected) == 0 {
		t.Fatal("no snapshots collected")
	}
	progress := make([]byte, nBlocks) // 0 -> A -> B
	valRank := map[byte]int{0: 0, 'A': 1, 'B': 2}
	for si, sn := range collected {
		for i := 0; i < nBlocks; i++ {
			b, err := sn.ReadBlock(i)
			if err != nil {
				t.Fatalf("snapshot %d read %d: %v", si, i, err)
			}
			c := b[0]
			for _, x := range b {
				if x != c {
					t.Fatalf("snapshot %d block %d torn", si, i)
				}
			}
			rank, ok := valRank[c]
			if !ok {
				t.Fatalf("snapshot %d block %d illegal value %d", si, i, c)
			}
			if rank < valRank[progress[i]] {
				t.Fatalf("snapshot %d block %d went backwards: %c -> %c",
					si, i, progress[i], c)
			}
			progress[i] = c
		}
	}

	if msg := s.CheckRefConsistency(); msg != "" {
		t.Fatalf("ref inconsistency under concurrency: %s", msg)
	}
	t.Logf("concurrent run OK: snapshots=%d finalRefs=%d", len(collected), s.TotalRefs())
}

// TestRepeatedIdempotentWrites：同一内容重试写多次，引用计数与块数量都不应增长。
func TestRepeatedIdempotentWrites(t *testing.T) {
	s, _ := openTestStore(t)
	bs := s.BlockSize()
	v, _ := s.CreateVolume("v", 1)
	data := testBlock(bs, 'Z')
	for i := 0; i < 50; i++ {
		if err := v.WriteBlock(0, data); err != nil {
			t.Fatal(err)
		}
	}
	if s.TotalRefs() != 1 {
		t.Fatalf("refs after repeated identical writes = %d, want 1", s.TotalRefs())
	}
	ids, err := s.bs.list()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("physical blocks = %d, want 1", len(ids))
	}
}
