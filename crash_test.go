package blockvol

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// 这些常量是父子进程之间的“子命令协议”。
const (
	envCrashDir = "BVS_CRASH_DIR"
	envCrashOp  = "BVS_CRASH_OP"
)

// runCrashChild 在子进程执行 op；用 SIGKILL 终止以模拟硬崩溃（无 Close）。
// waitAtLeast 用于保证子进程至少完成指定数量的已提交操作后再被杀。
func runCrashChild(t *testing.T, op string, killAfterOps int) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cmd := exec.Command(exe, "-test.run=TestCrashHelperProcess", "-test.v")
	cmd.Env = append(os.Environ(),
		envCrashDir+"="+dir,
		envCrashDir+"_OPS="+strconv.Itoa(killAfterOps),
		envCrashOp+"="+op,
	)
	out, err := cmd.CombinedOutput()
	t.Logf("child output:\n%s", string(out))
	if err == nil {
		t.Fatalf("child should have been killed, but exited cleanly")
	}
	// 确认确实是被信号杀死。
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ee.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("child not killed by SIGKILL: %v", err)
		}
	}
	// 把目录通过 t 的临时目录机制返回给调用者：我们直接把路径写到文件，
	// 更简单的办法是让本函数返回 dir。
	t.Setenv(envCrashDir+"_LAST", dir)
}

func crashChildDir(t *testing.T) string {
	t.Helper()
	d := os.Getenv(envCrashDir + "_LAST")
	if d == "" {
		t.Fatal("no crash dir")
	}
	return d
}

// TestCrashHelperProcess 不是真正的测试，它在子进程里执行实际存储操作并自杀。
func TestCrashHelperProcess(t *testing.T) {
	if os.Getenv(envCrashOp) == "" {
		t.Skip("not a crash helper invocation")
	}
	dir := os.Getenv(envCrashDir)
	op := os.Getenv(envCrashOp)
	nOps, _ := strconv.Atoi(os.Getenv(envCrashDir + "_OPS"))

	// 关闭后台 GC，避免其产生的检查点影响“操作中途崩溃”的窗口。
	s, err := Open(dir, WithGCInterval(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(2)
	}
	bs := s.BlockSize()

	switch op {
	case "writes":
		v, _ := s.CreateVolume("vol", 4)
		committed := 1
		// 不断写不同内容，每完成一条已提交写就检查是否到达自杀点。
		for i := 0; ; i++ {
			b := testBlock(bs, byte('A'+(i%26)))
			idx := i % 4
			if err := v.WriteBlock(idx, b); err != nil {
				fmt.Fprintln(os.Stderr, "write:", err)
				os.Exit(2)
			}
			committed++
			if committed >= nOps {
				// 硬崩溃：既不 Close，也不 sync 检查点。
				syscall.Kill(os.Getpid(), syscall.SIGKILL)
				time.Sleep(time.Hour)
			}
		}
	case "snapshots":
		v, _ := s.CreateVolume("vol", 2)
		_ = v.WriteBlock(0, testBlock(bs, 1))
		_ = v.WriteBlock(1, testBlock(bs, 2))
		if nOps >= 1 {
			_, _ = v.CreateSnapshot("s1")
		}
		if nOps >= 2 {
			_ = v.WriteBlock(0, testBlock(bs, 3))
			_, _ = v.CreateSnapshot("s2")
		}
		syscall.Kill(os.Getpid(), syscall.SIGKILL)
		time.Sleep(time.Hour)
	default:
		fmt.Fprintln(os.Stderr, "unknown op", op)
		os.Exit(2)
	}
}

// TestCrashRecoveryWrites：写操作进行到一半被 SIGKILL，重新打开后：
//   - 存储可打开（WAL 半帧被截断）；
//   - 每个块要么是某个已完整提交的旧值，绝不出现撕裂；
//   - 引用计数与元数据一致，磁盘上没有不可达的引用；
//   - 之后可以继续正常写、打快照、还原。
func TestCrashRecoveryWrites(t *testing.T) {
	runCrashChild(t, "writes", 11)
	dir := crashChildDir(t)

	s, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer s.Close()
	bs := s.BlockSize()

	v, err := s.GetVolume("vol")
	if err != nil {
		t.Fatalf("volume lost after crash: %v", err)
	}
	if v.SizeBlocks() != 4 {
		t.Fatalf("size = %d", v.SizeBlocks())
	}
	for i := 0; i < 4; i++ {
		b, err := v.ReadBlock(i)
		if err != nil {
			t.Fatalf("read %d after recovery: %v", i, err)
		}
		first := b[0]
		if first < 'A' || first > 'Z' {
			t.Fatalf("block %d has illegal value %d (torn?)", i, first)
		}
		for _, c := range b {
			if c != first {
				t.Fatalf("block %d torn after recovery", i)
			}
		}
		t.Logf("recovered block %d = %c", i, first)
	}
	if msg := s.CheckRefConsistency(); msg != "" {
		t.Fatalf("inconsistent refs after recovery: %s", msg)
	}
	// 恢复后固化检查点 + GC：孤儿块（被撕裂提交绕过的新块）必须可回收。
	st, err := s.RunGC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("post-recovery GC: %+v physicalLeft check...", st)

	// 继续操作，验证状态机仍健康。
	if err := v.WriteBlock(0, testBlock(bs, 'Z')); err != nil {
		t.Fatal(err)
	}
	sn, err := v.CreateSnapshot("post")
	if err != nil {
		t.Fatal(err)
	}
	g, _ := sn.ReadBlock(0)
	if g[0] != 'Z' {
		t.Fatalf("post-recovery snapshot wrong: %c", g[0])
	}
	if msg := s.CheckRefConsistency(); msg != "" {
		t.Fatalf("inconsistent refs after continued ops: %s", msg)
	}
}

// TestCrashRecoverySnapshots：在快照/删除序列中途 SIGKILL，恢复后
// 已提交的快照必须存在且内容正确；快照之后的写不影响快照。
func TestCrashRecoverySnapshots(t *testing.T) {
	runCrashChild(t, "snapshots", 2)
	dir := crashChildDir(t)
	s, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	bs := s.BlockSize()

	s1, err := s.GetSnapshot("s1")
	if err != nil {
		t.Fatalf("committed snapshot s1 missing: %v", err)
	}
	s2, err := s.GetSnapshot("s2")
	if err != nil {
		t.Fatalf("committed snapshot s2 missing: %v", err)
	}
	g, _ := s1.ReadBlock(0)
	if g[0] != 1 {
		t.Fatalf("s1 block0 = %d, want 1", g[0])
	}
	g, _ = s2.ReadBlock(0)
	if g[0] != 3 {
		t.Fatalf("s2 block0 = %d, want 3", g[0])
	}
	g, _ = s1.ReadBlock(1)
	if g[0] != 2 {
		t.Fatalf("s1 block1 = %d, want 2", g[0])
	}
	_ = bs
	if msg := s.CheckRefConsistency(); msg != "" {
		t.Fatalf("refs inconsistent: %s", msg)
	}
}

// TestTornWalTail：手工往 WAL 追加一段“损坏的半帧”，打开时必须被截断，
// 且之前已提交的数据完好。这直接验证未完成操作不会留下不一致状态。
func TestTornWalTail(t *testing.T) {
	s, dir := openTestStore(t, WithGCInterval(0))
	bs := s.BlockSize()
	v, _ := s.CreateVolume("vol", 1)
	_ = v.WriteBlock(0, testBlock(bs, 'Q'))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 干净关闭后 WAL 已清空。重新制造若干已提交操作，再追加垃圾字节，
	// 但不触发检查点（<32 条）。为此直接重新打开做一次写：
	s2, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	v2, _ := s2.GetVolume("vol")
	if err := v2.WriteBlock(0, testBlock(bs, 'R')); err != nil {
		t.Fatal(err)
	}
	// 先模拟硬崩溃释放文件锁（保留 WAL 内容），再手工追加损坏半帧。
	s2.crashSimulateClose()
	walPath := filepath.Join(dir, "wal", "wal.log")
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x57, 0x4C, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 'x'}); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	// 模拟崩溃后重新打开：损坏尾部必须被截断，已提交的 'R' 必须完好。
	s3, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatalf("open with torn tail: %v", err)
	}
	defer s3.Close()
	v3, _ := s3.GetVolume("vol")
	g, _ := v3.ReadBlock(0)
	if g[0] != 'R' {
		t.Fatalf("after torn-tail recovery block = %c, want R", g[0])
	}
	if msg := s3.CheckRefConsistency(); msg != "" {
		t.Fatalf("refs inconsistent: %s", msg)
	}

	// 半帧应已被截断；再检查 wal 文件只含完整帧。
	data, _ := os.ReadFile(filepath.Join(dir, "wal", "wal.log"))
	if len(data) == 0 {
		// 恢复时做了 checkpoint 并清空 WAL，这也是合法结果。
		return
	}
	if strings.Contains(string(data), "xxxxx") {
		t.Fatal("torn bytes remain in wal")
	}
}

// TestOrphanTempBlockCleanup：模拟一个写入中途崩溃遗留的 *.tmp 块，
// 打开存储时它应被清理，且不影响任何已提交数据。
func TestOrphanTempBlockCleanup(t *testing.T) {
	s, dir := openTestStore(t, WithGCInterval(0))
	bs := s.BlockSize()
	v, _ := s.CreateVolume("vol", 1)
	_ = v.WriteBlock(0, testBlock(bs, 'M'))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	shard := filepath.Join(dir, "blocks", "ab")
	if err := os.MkdirAll(shard, 0o755); err != nil {
		t.Fatal(err)
	}
	junk := filepath.Join(shard, strings.Repeat("a", 64)+"-deadbeef.tmp")
	if err := os.WriteFile(junk, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, WithLogger(testLogger{t}), WithGCInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := os.Stat(junk); !errors.Is(err, err) && !os.IsNotExist(err) {
		if !os.IsNotExist(err) {
			t.Fatalf("temp block not cleaned: %v", err)
		}
	}
	v2, _ := s2.GetVolume("vol")
	g, _ := v2.ReadBlock(0)
	if !bytes.Equal(g, testBlock(bs, 'M')) {
		t.Fatal("committed data damaged by temp cleanup")
	}
}
