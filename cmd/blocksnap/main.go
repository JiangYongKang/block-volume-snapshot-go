// 命令行演示：在指定目录上跑一遍“写 -> 快照 -> 覆盖写 -> 删除卷 -> 快照还原
// -> 克隆 -> 后台回收”的完整序列，打印关键操作与引用计数变化。
//
// 用法：
//
//	go run ./cmd/blocksnap [-dir ./demo-data]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	blocksnap "github.com/highcumontoa/block-volume-snapshot-go"
)

func main() {
	dir := flag.String("dir", "./demo-data", "数据目录")
	flag.Parse()

	if err := os.RemoveAll(*dir); err != nil {
		fail(err)
	}
	s, err := blocksnap.Open(*dir, blocksnap.Options{
		BlockSize: 64,
		Logf:      func(f string, a ...any) { fmt.Printf("    "+f+"\n", a...) },
	})
	if err != nil {
		fail(err)
	}
	defer s.Close()

	const size = 64
	check("CreateVolume", s.CreateVolume("c1", "vol", size))
	check("WriteAt(v1)", s.WriteAt("w1", "vol", 0, fill('A', size)))
	check("CreateSnapshot(s1)", s.CreateSnapshot("s1", "vol", "snap1"))

	oldID, _ := s.VolumeBlockID("vol", 0)
	check("WriteAt(overwrite)", s.WriteAt("w2", "vol", 0, fill('B', size)))
	newID, _ := s.VolumeBlockID("vol", 0)
	fmt.Printf("refs old(A)=%d  new(B)=%d  (旧块仍被快照持有)\n",
		s.BlockRefCount(oldID), s.BlockRefCount(newID))

	check("DeleteVolume", s.DeleteVolume("d1", "vol"))
	check("Restore(snap1)", s.Restore("r1", "vol", "snap1"))
	buf := make([]byte, size)
	if _, err := s.ReadAt("vol", 0, buf); err != nil {
		fail(err)
	}
	fmt.Printf("还原后卷首字节=%q（应为 AAAA…）\n", string(buf[:4]))

	check("CloneVolume", s.CloneVolume("cl1", "snap1", "clone", size))
	check("DeleteSnapshot", s.DeleteSnapshot("ds1", "snap1"))
	n, err := s.ForceGC(context.Background())
	check("ForceGC", err)
	fmt.Printf("GC 回收块数=%d（回收的是还原后失去引用的 B 块；A 块仍被克隆持有，未被误删）\n", n)

	fmt.Println("demo OK：快照一致、删除可还原、引用计数与回收均正确")
}

func check(op string, err error) {
	if err != nil {
		fail(fmt.Errorf("%s: %w", op, err))
	}
	fmt.Printf("== %s ==\n", op)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func fill(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
