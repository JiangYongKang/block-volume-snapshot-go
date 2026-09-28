package blockvol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// 物理块以内容寻址（SHA-256）：blocks/<前2位>/<完整64位hex>。
// 相同内容天然去重，块文件一经写入永不修改。

const blocksDirName = "blocks"
const tempSuffix = ".tmp"

// blockStore 管理以内容寻址的不可变物理块文件。
// 物理块一经写入永不修改；写时复制通过“写新块 + 切换映射”实现。
type blockStore struct {
	dir string
}

// newBlockStore 打开/创建物理块目录。
func newBlockStore(dir string) (*blockStore, error) {
	b := &blockStore{dir: filepath.Join(dir, blocksDirName)}
	if err := os.MkdirAll(b.dir, 0o755); err != nil {
		return nil, fmt.Errorf("create blocks dir: %w", err)
	}
	return b, nil
}

// id 计算数据的物理块 ID（小写十六进制 SHA-256）。
func blockID(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validBlockID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, c := range id {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// path 返回物理块文件路径。
func (b *blockStore) path(id string) string {
	return filepath.Join(b.dir, id[:2], id)
}

// write 原子写入一个物理块（临时文件 + fsync + rename + 目录 fsync）。
// 幂等：内容相同的块已存在时直接返回其 ID（并发写同内容、重试均安全）。
func (b *blockStore) write(data []byte) (string, error) {
	id := blockID(data)
	dst := b.path(id)
	if _, err := os.Stat(dst); err == nil {
		return id, nil // 已存在，天然去重
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("stat block: %w", err)
	}
	shard := filepath.Dir(dst)
	if err := os.MkdirAll(shard, 0o755); err != nil {
		return "", fmt.Errorf("create block shard: %w", err)
	}

	var randBuf [16]byte
	if _, err := rand.Read(randBuf[:]); err != nil {
		return "", fmt.Errorf("rand temp name: %w", err)
	}
	tmp := filepath.Join(shard, id+"-"+hex.EncodeToString(randBuf[:])+tempSuffix)

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", fmt.Errorf("create temp block: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("write temp block: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("fsync temp block: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("close temp block: %w", err)
	}
	// rename 在同一文件系统内是原子的：块要么完整可见，要么不可见。
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("rename block: %w", err)
	}
	if err := syncDir(shard); err != nil {
		return "", err
	}
	return id, nil
}

// read 读取物理块全部字节；不存在返回包装了 fs.ErrNotExist 的错误。
func (b *blockStore) read(id string) ([]byte, error) {
	if !validBlockID(id) {
		return nil, fmt.Errorf("%w: bad block id", ErrInvalidArgument)
	}
	data, err := os.ReadFile(b.path(id))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("read block %s: %w", id, err)
	}
	return data, nil
}

// remove 删除一个物理块文件；文件已消失视为成功（GC 重试安全）。
func (b *blockStore) remove(id string) error {
	if !validBlockID(id) {
		return fmt.Errorf("%w: bad block id", ErrInvalidArgument)
	}
	p := b.path(id)
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove block %s: %w", id, err)
	}
	return syncDir(filepath.Dir(p))
}

// exists 判断物理块文件是否存在。
func (b *blockStore) exists(id string) (bool, error) {
	if !validBlockID(id) {
		return false, fmt.Errorf("%w: bad block id", ErrInvalidArgument)
	}
	_, err := os.Stat(b.path(id))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("stat block %s: %w", id, err)
}

// list 列出全部物理块 ID（忽略临时文件）。
func (b *blockStore) list() ([]string, error) {
	var ids []string
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read blocks dir: %w", err)
	}
	for _, shard := range entries {
		if !shard.IsDir() || len(shard.Name()) != 2 {
			continue
		}
		files, err := os.ReadDir(filepath.Join(b.dir, shard.Name()))
		if err != nil {
			return nil, fmt.Errorf("read shard dir: %w", err)
		}
		for _, f := range files {
			name := f.Name()
			if f.IsDir() || strings.HasSuffix(name, tempSuffix) || !validBlockID(name) {
				continue
			}
			ids = append(ids, name)
		}
	}
	return ids, nil
}

// cleanTempFiles 删除崩溃遗留的 *.tmp 块文件（这些块从未被任何元数据引用）。
func (b *blockStore) cleanTempFiles() (int, error) {
	removed := 0
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("read blocks dir: %w", err)
	}
	for _, shard := range entries {
		if !shard.IsDir() {
			continue
		}
		sdir := filepath.Join(b.dir, shard.Name())
		files, err := os.ReadDir(sdir)
		if err != nil {
			return removed, err
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), tempSuffix) {
				continue
			}
			if err := os.Remove(filepath.Join(sdir, f.Name())); err != nil &&
				!errors.Is(err, fs.ErrNotExist) {
				return removed, err
			}
			removed++
		}
		if removed > 0 {
			_ = syncDir(sdir)
		}
	}
	return removed, nil
}

// syncDir 对目录执行 fsync，保证其中的创建/删除/重命名已落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for sync %s: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}
