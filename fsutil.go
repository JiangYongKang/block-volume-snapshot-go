package blocksnap

import (
	"os"
	"path/filepath"
)

// syncParentDir fsync 指定路径所在目录，保证该目录下的 rename/truncate
// 等元数据变更在崩溃后仍然可见。
func syncParentDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
