package blocksnap

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
)

func newTestStore(t *testing.T, opts Options) *Store {
	t.Helper()
	if opts.Logf == nil {
		opts.Logf = t.Logf
	}
	if opts.GCInterval == 0 {
		opts.GCInterval = 50_000_000 // 50ms
	}
	s, err := Open(t.TempDir(), opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func must(t *testing.T, err error, op string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
}

func readFull(t *testing.T, s *Store, vol string, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	n, err := s.ReadAt(vol, 0, buf)
	must(t, err, "read")
	if n != size {
		t.Fatalf("short read: %d/%d", n, size)
	}
	return buf
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// bytesEqual 报告两个字节切片是否相同。
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
