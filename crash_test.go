package blocksnap

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// 崩溃点常量（与 store.go 中 s.crash(...) 的名字对应）。
const (
	crashWriteBlockNoWAL     = "write:afterBlockBeforeWAL"
	crashSnapAfterWAL        = "snap:afterWAL"
	crashCheckpointMid       = "checkpoint:afterMetaBeforeTruncate"
	crashCheckpointAfterTrun = "checkpoint:afterTruncate"
)

// runCrashScenario 在【子进程】里执行一段操作序列，在指定崩溃点以 exit 99
// 真实中断进程。父进程随后重新 Open 同一目录验证恢复结果。
func runCrashScenario(t *testing.T, dir, crashPoint, scenario string) {
	t.Helper()
	if os.Getenv("BLOCKSNAP_CRASH_CHILD") == "1" {
		runChild(dir, crashPoint, scenario)
		os.Exit(0)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$")
	cmd.Env = append(os.Environ(),
		"BLOCKSNAP_CRASH_CHILD=1",
		"BLOCKSNAP_DIR="+dir,
		"BLOCKSNAP_CRASH="+crashPoint,
		"BLOCKSNAP_SCENARIO="+scenario,
	)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if errors.As(err, &ee) && ee.ExitCode() == 99 {
		t.Logf("child crashed as expected at %q:\n%s", crashPoint, out)
		return
	}
	t.Fatalf("expected child exit 99, got err=%v output:\n%s", err, out)
}

// TestCrashChild 仅由子进程执行（父进程直接跳过）。
func TestCrashChild(t *testing.T) {
	if os.Getenv("BLOCKSNAP_CRASH_CHILD") != "1" {
		t.Skip("child-only test")
	}
	runChild(os.Getenv("BLOCKSNAP_DIR"), os.Getenv("BLOCKSNAP_CRASH"), os.Getenv("BLOCKSNAP_SCENARIO"))
}

func runChild(dir, crashPoint, scenario string) {
	s, err := Open(dir, Options{
		BlockSize: 64,
		DisableGC: true,
		Logf:      func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) },
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open: %v\n", err)
		os.Exit(1)
	}
	if crashPoint != "" {
		switch scenario {
		case "write":
			// 第 1 次 WriteAt 已完整提交，第 2 次在块落盘后、WAL 提交前崩溃。
			s.SetCrashPointN(crashPoint, 2)
		case "snapshot":
			// 第 1 个快照已完整提交；第 2 个快照在 WAL 落盘后、内存应用前崩溃。
			s.SetCrashPointN(crashPoint, 2)
		default:
			s.SetCrashPoint(crashPoint)
		}
	}
	switch scenario {
	case "write":
		mustChild(s.CreateVolume("c", "v", 64))
		_ = s.WriteAt("w1", "v", 0, pad([]byte("GOOD"), 64)) // 已提交
		// 下一次写入会在“块文件落盘后、WAL 提交前”崩溃。
		if err := s.WriteAt("w2", "v", 0, pad([]byte("HALF"), 64)); err != nil {
			fmt.Fprintf(os.Stderr, "unexpected err: %v\n", err)
		}
	case "snapshot":
		mustChild(s.CreateVolume("c", "v", 64))
		mustChild(s.WriteAt("w1", "v", 0, pad([]byte("SNAP"), 64)))
		mustChild(s.CreateSnapshot("s1", "v", "snap"))
		// 第二个快照在 WAL 已落盘、内存状态应用前崩溃（等价掉电：记录已提交）。
		_ = s.CreateSnapshot("s2", "v", "snap2")
	case "checkpoint":
		mustChild(s.CreateVolume("c", "v", 64))
		mustChild(s.WriteAt("w1", "v", 0, pad([]byte("CKPT"), 64)))
		mustChild(s.CreateSnapshot("s1", "v", "snap"))
		_ = s.ForceCheckpoint()
	}
	_ = s.Close()
	os.Exit(0)
}

func mustChild(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "child op failed: %v\n", err)
		os.Exit(1)
	}
}

func reopen(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, Options{BlockSize: 64, DisableGC: true, Logf: t.Logf})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestCrashWriteBlockBeforeWAL：块文件已写但映射未提交时崩溃。
// 恢复后写入必须整体不存在（无部分可见），孤儿块被安全回收；已提交数据完好。
func TestCrashWriteBlockBeforeWAL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	runCrashScenario(t, dir, crashWriteBlockNoWAL, "write")
	s := reopen(t, dir)

	data := readFull(t, s, "v", 64)
	if string(data[:4]) != "GOOD" {
		t.Fatalf("committed data lost after crash: %q", data[:4])
	}
	ids, _ := s.blocks.ids()
	if len(ids) != 1 {
		t.Fatalf("orphan in-flight block not reclaimed: %d blocks on disk", len(ids))
	}
	if snaps, _ := s.ListSnapshots(""); len(snaps) != 0 {
		t.Fatalf("unexpected snapshots: %v", snaps)
	}
	// 恢复后可继续正常工作。
	must(t, s.WriteAt("w-after", "v", 0, pad([]byte("NEW"), 64)), "write after recovery")
	if data := readFull(t, s, "v", 64); string(data[:3]) != "NEW" {
		t.Fatalf("post-recovery write wrong: %q", data[:3])
	}
	// 用旧的 clientID 重试 w2 也应正常生效（崩溃前未提交）。
	must(t, s.WriteAt("w2", "v", 0, pad([]byte("RETRY"), 64)), "retry w2")
	if data := readFull(t, s, "v", 64); string(data[:5]) != "RETRY" {
		t.Fatalf("retry after crash wrong: %q", data[:5])
	}
}

// TestCrashSnapshotAfterWAL：WAL 记录已 fsync、应用内存状态前崩溃。
// 重放后快照必须存在且内容完整——已完成操作不丢。
func TestCrashSnapshotAfterWAL(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	runCrashScenario(t, dir, crashSnapAfterWAL, "snapshot")
	s := reopen(t, dir)

	snaps, _ := s.ListSnapshots("")
	if len(snaps) != 2 {
		t.Fatalf("after replay want 2 snapshots, got %v", snaps)
	}
	id, ok := s.SnapshotBlockID("snap2", 0)
	if !ok {
		t.Fatal("snap2 lost after crash despite committed WAL")
	}
	blk, err := s.blocks.read(id)
	must(t, err, "read snap2 block")
	if string(blk[:4]) != "SNAP" {
		t.Fatalf("snap2 content wrong: %q", blk[:4])
	}
	// 已提交幂等键仍然生效：s2 重试不会再造一个快照。
	must(t, s.CreateSnapshot("s2", "v", "snap2"), "idempotent replay after recovery")
	snaps, _ = s.ListSnapshots("")
	if len(snaps) != 2 {
		t.Fatalf("retry after recovery duplicated snapshot: %v", snaps)
	}
}

// TestCrashDuringCheckpoint 覆盖检查点两步崩溃：
// meta.json 已写但 WAL 未截断；以及截断之后。两种情况下状态都必须一致完整。
func TestCrashDuringCheckpoint(t *testing.T) {
	for _, point := range []string{crashCheckpointMid, crashCheckpointAfterTrun} {
		t.Run(point, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "data")
			runCrashScenario(t, dir, point, "checkpoint")
			s := reopen(t, dir)

			data := readFull(t, s, "v", 64)
			if string(data[:4]) != "CKPT" {
				t.Fatalf("data lost after checkpoint crash: %q", data[:4])
			}
			if id, ok := s.SnapshotBlockID("snap", 0); !ok || id == "" {
				t.Fatal("snapshot lost after checkpoint crash")
			}
			if vols, _ := s.ListVolumes(); len(vols) != 1 {
				t.Fatalf("want 1 volume, got %d", len(vols))
			}
			// 引用计数正确，恢复后操作继续可用。
			id, _ := s.SnapshotBlockID("snap", 0)
			if rc := s.BlockRefCount(id); rc != 2 {
				t.Fatalf("refs after recovery=%d want 2", rc)
			}
		})
	}
}

// TestTornWALTail 手工在 WAL 末尾追加撕裂字节，验证打开时被丢弃而不是报损坏。
func TestTornWALTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	func() {
		s, err := Open(dir, Options{BlockSize: 64, DisableGC: true, Logf: t.Logf})
		must(t, err, "open")
		must(t, s.CreateVolume("c", "v", 64), "create")
		must(t, s.WriteAt("w", "v", 0, pad([]byte("OK"), 64)), "write")
		must(t, s.Close(), "close")
	}()
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	must(t, err, "open wal")
	_, _ = f.WriteString(strings.Repeat("\xff", 13)) // 撕裂尾：不完整帧
	must(t, f.Close(), "close wal")

	s := reopen(t, dir)
	if data := readFull(t, s, "v", 64); string(data[:2]) != "OK" {
		t.Fatalf("data after torn tail wrong: %q", data[:2])
	}
	// WAL 撕裂尾部应已被物理截断，再次打开依然正常。
	must(t, s.Close(), "close again")
	s2 := reopen(t, dir)
	if data := readFull(t, s2, "v", 64); string(data[:2]) != "OK" {
		t.Fatal("second reopen failed")
	}
}
