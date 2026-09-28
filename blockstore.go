package blocksnap

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// blockID 计算数据的内容寻址 id（sha256 十六进制）。
func blockID(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// blockStore 管理内容寻址、不可变的数据块文件（blocks/<sha256>）。
type blockStore struct {
	dir string
}

func openBlockStore(dir string) (*blockStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create blocks dir: %w", err)
	}
	return &blockStore{dir: dir}, nil
}

func (bs *blockStore) path(id string) string {
	return filepath.Join(bs.dir, id)
}

// write 将数据原子写入块文件（临时文件 + fsync + rename）。
// 块内容不可变：文件已存在时视为成功（天然幂等）。
func (bs *blockStore) write(id string, data []byte) error {
	final := bs.path(id)
	if ok, err := bs.exists(id); err != nil {
		return err
	} else if ok {
		return nil
	}
	tmp, err := os.CreateTemp(bs.dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp block: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp block: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temp block: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		return fmt.Errorf("rename block: %w", err)
	}
	return nil
}

// read 读取整块数据。
func (bs *blockStore) read(id string) ([]byte, error) {
	data, err := os.ReadFile(bs.path(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: block %s", ErrNotFound, id)
		}
		return nil, err
	}
	return data, nil
}

// remove 物理删除一个块文件；不存在不算错误（回收幂等）。
func (bs *blockStore) remove(id string) error {
	if err := os.Remove(bs.path(id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// exists 判断块文件是否存在。
func (bs *blockStore) exists(id string) (bool, error) {
	if _, err := os.Stat(bs.path(id)); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// sweep 删除所有不被 keep 接受的块文件，返回回收块数。
// 仅删除形如 sha256 十六进制（64 字符）的文件，绝不触碰临时文件。
func (bs *blockStore) sweep(keep func(string) bool) (int, error) {
	entries, err := os.ReadDir(bs.dir)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, ".tmp-") {
			continue
		}
		if len(name) != 64 {
			continue // 只回收正式块文件
		}
		if keep(name) {
			continue
		}
		if err := bs.remove(name); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// ids 返回磁盘上现存的所有正式块 id。
func (bs *blockStore) ids() ([]string, error) {
	entries, err := os.ReadDir(bs.dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && len(name) == 64 && !strings.HasPrefix(name, ".tmp-") {
			out = append(out, name)
		}
	}
	return out, nil
}
