package blockvol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// volumeMeta 是卷的持久化元数据。
type volumeMeta struct {
	Name      string
	Size      int
	CreatedAt int64 // unix nano
	Deleted   bool
	// Blocks[逻辑块号] = 物理块 ID；空槽表示全零块（从未写过）。
	Blocks []string
}

// snapshotMeta 是快照的持久化元数据（映射创建后不可变）。
type snapshotMeta struct {
	Name      string
	Source    string
	Size      int
	CreatedAt int64
	// Blocks 为创建时点的映射副本（深拷贝）。
	Blocks []string
}

// cloneBlocks 返回映射的深拷贝（快照必须与后续写入隔离）。
func cloneBlocks(b []string) []string {
	if b == nil {
		return nil
	}
	c := make([]string, len(b))
	copy(c, b)
	return c
}

// metaState 是内存中的全部元数据，受 Store.metaMu 保护。
type metaState struct {
	blockSize int
	volumes   map[string]*volumeMeta
	snapshots map[string]*snapshotMeta
}

func newMetaState(blockSize int) *metaState {
	return &metaState{
		blockSize: blockSize,
		volumes:   map[string]*volumeMeta{},
		snapshots: map[string]*snapshotMeta{},
	}
}

// ---- 检查点（checkpoint）持久化 ----

const (
	metaDirName       = "meta"
	checkpointName    = "checkpoint.json"
	checkpointTmpName = "checkpoint.json.tmp"
	metaVersion       = 1
)

// checkpoint 是元数据的磁盘表示。
type checkpoint struct {
	Version   int `json:"version"`
	BlockSize int `json:"block_size"`
	// NextSeq 是检查点之后下一条 WAL 记录应使用的序号。
	NextSeq   int64                    `json:"next_seq"`
	Volumes   map[string]*volumeMeta   `json:"volumes"`
	Snapshots map[string]*snapshotMeta `json:"snapshots"`
}

// saveAtomic 以“临时文件 + fsync + rename + 目录 fsync”原子落盘检查点。
func saveAtomic(dir string, cp *checkpoint) error {
	md := filepath.Join(dir, metaDirName)
	if err := os.MkdirAll(md, 0o755); err != nil {
		return fmt.Errorf("create meta dir: %w", err)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cp); err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	tmp := filepath.Join(md, checkpointTmpName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create checkpoint tmp: %w", err)
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}
	dst := filepath.Join(md, checkpointName)
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	return syncDir(md)
}

// loadCheckpoint 读取检查点；不存在时返回 nil（全新存储）。
func loadCheckpoint(dir string) (*checkpoint, error) {
	p := filepath.Join(dir, metaDirName, checkpointName)
	data, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var cp checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	if cp.Version != metaVersion {
		return nil, fmt.Errorf("%w: unsupported checkpoint version %d", ErrInvalidArgument, cp.Version)
	}
	if cp.Volumes == nil {
		cp.Volumes = map[string]*volumeMeta{}
	}
	if cp.Snapshots == nil {
		cp.Snapshots = map[string]*snapshotMeta{}
	}
	return &cp, nil
}

// toCheckpoint 生成内存状态的检查点快照（调用方需持锁）。
func (m *metaState) toCheckpoint() *checkpoint {
	cp := &checkpoint{
		Version:   metaVersion,
		BlockSize: m.blockSize,
		Volumes:   make(map[string]*volumeMeta, len(m.volumes)),
		Snapshots: make(map[string]*snapshotMeta, len(m.snapshots)),
	}
	for k, v := range m.volumes {
		vc := *v
		vc.Blocks = cloneBlocks(v.Blocks)
		cp.Volumes[k] = &vc
	}
	for k, s := range m.snapshots {
		sc := *s
		sc.Blocks = cloneBlocks(s.Blocks)
		cp.Snapshots[k] = &sc
	}
	return cp
}
