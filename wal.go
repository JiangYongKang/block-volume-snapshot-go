package blocksnap

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// WAL 记录类型。
const (
	recCreateVolume = "create_volume"
	recDeleteVolume = "delete_volume"
	recWrite        = "write"
	recCreateSnap   = "create_snapshot"
	recDeleteSnap   = "delete_snapshot"
	recRestore      = "restore"
	recClone        = "clone"
	recCheckpoint   = "checkpoint"
)

// walRecord 是 WAL 中的一条记录。
// 帧格式：8 字节大端长度前缀 + JSON 负载。
type walRecord struct {
	Seq     int64          `json:"seq"`
	Client  string         `json:"client,omitempty"` // 客户端幂等键
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload,omitempty"`
}

// wal 是只追加的预写日志（data/wal.log）。
type wal struct {
	f           *os.File
	path        string
	disableSync bool
}

func openWAL(path string, disableSync bool) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &wal{f: f, path: path, disableSync: disableSync}, nil
}

// append 写入一条记录并落盘（fsync），保证崩溃后该记录要么完整存在、要么不存在。
func (w *wal) append(rec walRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], uint64(len(payload)))
	if _, err := w.f.Write(hdr[:]); err != nil {
		return fmt.Errorf("wal write header: %w", err)
	}
	if _, err := w.f.Write(payload); err != nil {
		return fmt.Errorf("wal write payload: %w", err)
	}
	if !w.disableSync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("wal sync: %w", err)
		}
	}
	return nil
}

func (w *wal) close() error { return w.f.Close() }

// truncate 清空 WAL（checkpoint 成功后调用），并同步目录项。
func (w *wal) truncate() error {
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if !w.disableSync {
		if err := w.f.Sync(); err != nil {
			return err
		}
	}
	return syncParentDir(w.path)
}

// readWALRecords 顺序读取全部完整记录；遇到帧不完整/损坏（崩溃撕裂尾）即停止，
// 并返回有效字节长度，供调用方截断撕裂尾部。
func readWALRecords(path string) (recs []walRecord, validBytes int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var offset int64
	for {
		var hdr [8]byte
		n, err := io.ReadFull(r, hdr[:])
		offset += int64(n)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// 没有完整的下一帧：尾部撕裂，丢弃。
			return recs, offset - int64(n), nil
		}
		if err != nil {
			return nil, 0, err
		}
		length := binary.BigEndian.Uint64(hdr[:])
		if length > 64*1024*1024 {
			// 长度头明显非法：只可能是崩溃撕裂的残余（正常帧不可能这么大），
			// 当作尾部丢弃，而不是让整个存储无法打开。
			return recs, offset - 8, nil
		}
		buf := make([]byte, length)
		pn, perr := io.ReadFull(r, buf)
		offset += int64(pn)
		if perr == io.ErrUnexpectedEOF || perr == io.EOF {
			return recs, offset - int64(pn) - 8, nil
		}
		if perr != nil {
			return nil, 0, perr
		}
		var rec walRecord
		if err := json.Unmarshal(buf, &rec); err != nil {
			// 负载损坏：停在该帧之前。
			return recs, offset - int64(length) - 8, nil
		}
		recs = append(recs, rec)
	}
}

// trimWAL 将 WAL 截断到 validBytes（丢弃撕裂尾部），并同步。
func trimWAL(path string, validBytes int64, disableSync bool) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(validBytes); err != nil {
		return err
	}
	if !disableSync {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return syncParentDir(path)
}
