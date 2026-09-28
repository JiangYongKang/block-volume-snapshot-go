package blockvol

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const lockFileName = "store.lock"

// fileLock 是基于 flock 的进程级排他锁：同一目录不允许两个 Store 同时打开，
// 避免两个进程各自恢复、互相破坏引用计数。进程退出（含崩溃）时内核自动释放。
type fileLock struct {
	f *os.File
}

func acquireLock(dir string) (*fileLock, error) {
	p := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	const lockExNb = syscall.LOCK_EX | syscall.LOCK_NB
	if err := syscall.Flock(int(f.Fd()), lockExNb); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: flock held by another process: %v", ErrLocked, err)
		}
		return nil, fmt.Errorf("acquire flock: %w", err)
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}
