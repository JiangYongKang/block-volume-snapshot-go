package blockvol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// 帧格式（小端序）：
//   magic[2] = 0x57 0x4C ("WL") | payloadLen uint32 | crc32 uint32 | payload[payloadLen]
// payload 为一条 JSON 编码的 walRecord。单写者 + 追加写；崩溃可能只留下半帧，
// replay 时在第一个损坏帧处截断，保证半条记录不会被当成已提交操作。

const (
	walDirName    = "wal"
	walFileName   = "wal.log"
	walMagic0     = 0x57
	walMagic1     = 0x4C
	walHeaderSize = 2 + 4 + 4
)

// walOp 标识 WAL 记录类型。
type walOp string

const (
	walCreateVolume   walOp = "create_volume"
	walDeleteVolume   walOp = "delete_volume"
	walWriteBlock     walOp = "write_block"
	walCreateSnapshot walOp = "create_snapshot"
	walDeleteSnapshot walOp = "delete_snapshot"
	walRestore        walOp = "restore_snapshot"
	walClone          walOp = "clone_volume"
)

// walRecord 是一条预写日志记录。所有字段按操作类型取用。
type walRecord struct {
	// Seq 为单调递增序号；检查点记录 NextSeq，恢复时跳过已并入检查点的记录。
	Seq         int64    `json:"seq"`
	Op          walOp    `json:"op"`
	Volume      string   `json:"volume,omitempty"`
	Snapshot    string   `json:"snapshot,omitempty"`
	Source      string   `json:"source,omitempty"`
	Index       int      `json:"index,omitempty"`
	Size        int      `json:"size,omitempty"`
	Physical    string   `json:"physical,omitempty"`
	OldPhysical string   `json:"old_physical,omitempty"`
	CreatedAt   int64    `json:"created_at,omitempty"`
	Blocks      []string `json:"blocks,omitempty"`
}

// wal 是仅追加的预写日志，单进程内通过 mu 串行化所有追加。
type wal struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// openWAL 打开（必要时创建）WAL 文件。
func openWAL(dir string) (*wal, error) {
	wd := filepath.Join(dir, walDirName)
	if err := os.MkdirAll(wd, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	p := filepath.Join(wd, walFileName)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &wal{f: f, path: p}, nil
}

// append 将一条记录以完整帧的形式追加并 fsync；返回后即视为已提交。
func (w *wal) append(rec *walRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal wal record: %w", err)
	}
	if len(payload) > 0xFFFFFFFF {
		return fmt.Errorf("%w: wal record too large", ErrInvalidArgument)
	}
	frame := make([]byte, walHeaderSize+len(payload))
	frame[0] = walMagic0
	frame[1] = walMagic1
	binary.LittleEndian.PutUint32(frame[2:6], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[6:10], crc32.ChecksumIEEE(payload))
	copy(frame[walHeaderSize:], payload)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return ErrClosed
	}
	// 单写者 + 单次 Write：除操作系统崩溃外帧不会交错；尾部半帧由 replay 截断。
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("write wal frame: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("fsync wal: %w", err)
	}
	return nil
}

// replay 顺序读取全部完整帧；发现尾部半帧/损坏帧时截断到最后一个有效边界。
func (w *wal) replay() ([]walRecord, error) {
	data, err := os.ReadFile(w.path)
	if err != nil {
		return nil, fmt.Errorf("read wal: %w", err)
	}
	var recs []walRecord
	off := 0
	for off < len(data) {
		if len(data)-off < walHeaderSize {
			break // 半帧头
		}
		if data[off] != walMagic0 || data[off+1] != walMagic1 {
			break // 损坏
		}
		pLen := int(binary.LittleEndian.Uint32(data[off+2 : off+6]))
		if pLen == 0 || len(data)-off-walHeaderSize < pLen {
			break // 半帧体
		}
		payload := data[off+walHeaderSize : off+walHeaderSize+pLen]
		wantCRC := binary.LittleEndian.Uint32(data[off+6 : off+10])
		if crc32.ChecksumIEEE(payload) != wantCRC {
			break // 帧体损坏（可能 torn write）
		}
		var rec walRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			break
		}
		recs = append(recs, rec)
		off += walHeaderSize + pLen
	}
	// 截掉尾部未完整提交的字节，使下一次追加从干净边界开始。
	if off < len(data) {
		w.mu.Lock()
		defer w.mu.Unlock()
		if err := w.f.Truncate(int64(off)); err != nil {
			return nil, fmt.Errorf("truncate torn wal: %w", err)
		}
		if err := w.f.Sync(); err != nil {
			return nil, fmt.Errorf("fsync truncated wal: %w", err)
		}
	}
	return recs, nil
}

// truncate 在元数据检查点落盘后清空 WAL 并 fsync。
func (w *wal) truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(0); err != nil {
		return fmt.Errorf("truncate wal: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek wal: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("fsync truncated wal: %w", err)
	}
	return syncDir(filepath.Dir(w.path))
}

// close 关闭 WAL 文件。
func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return ErrClosed
	}
	err := w.f.Close()
	w.f = nil
	return err
}
