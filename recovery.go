package blocksnap

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// checkpointFile 是 meta.json 的磁盘格式。
type checkpointFile struct {
	LastSeq   int64                    `json:"last_seq"`
	Volumes   map[string]*volumeMeta   `json:"volumes"`
	Snapshots map[string]*snapshotMeta `json:"snapshots"`
	SeenReq   map[string]bool          `json:"seen_req"`
}

// recoverStore 执行崩溃恢复：
//  1. 读取 WAL，丢弃崩溃造成的撕裂尾帧；
//  2. 以最近一次检查点（meta.json）为基线，重放其后的所有已提交记录；
//     无检查点时从零重放全部 WAL（检查点只是加速手段）；
//  3. 同一客户端幂等键的重复记录只生效一次（天然处理“操作已落盘但响应丢失”的重试）；
//  4. 重算物理块引用计数，清扫元数据中已无引用的孤儿块；
//  5. 移除已删除且无任何快照引用的卷元数据。
func recoverStore(dataDir string, disableSync bool) (*metaState, error) {
	walPath := filepath.Join(dataDir, "wal.log")
	recs, validBytes, err := readWALRecords(walPath)
	if err != nil {
		return nil, err
	}
	if int64(len(recs)) > 0 {
		// 若有撕裂尾帧则截断（validBytes 指向最后一个完整帧之后）。
		if info, statErr := os.Stat(walPath); statErr == nil && info.Size() != validBytes {
			if err := trimWAL(walPath, validBytes, disableSync); err != nil {
				return nil, err
			}
		}
	}

	st := newMetaState()
	baseSeq := int64(0)
	if cf, err := loadCheckpoint(filepath.Join(dataDir, "meta.json")); err != nil {
		return nil, err
	} else if cf != nil {
		st.Volumes = cf.Volumes
		st.Snapshots = cf.Snapshots
		st.SeenReq = cf.SeenReq
		if st.SeenReq == nil {
			st.SeenReq = map[string]bool{}
		}
		baseSeq = cf.LastSeq
		st.NextSeq = cf.LastSeq
	}

	for _, rec := range recs {
		if rec.Seq <= baseSeq {
			continue // 已包含在检查点中
		}
		if rec.Type == recCheckpoint {
			st.NextSeq = rec.Seq
			continue
		}
		// 客户端幂等：相同键的重复提交只生效一次。
		if rec.Client != "" {
			if st.SeenReq[rec.Client] {
				st.NextSeq = rec.Seq
				continue
			}
			st.SeenReq[rec.Client] = true
		}
		if err := applyRecord(st, rec); err != nil {
			return nil, err
		}
		st.NextSeq = rec.Seq
	}

	for name := range st.Volumes {
		st.pruneDeletedVolume(name)
	}
	st.recomputeRefs()
	return st, nil
}

// applyRecord 将一条已提交 WAL 记录应用到内存状态。纯函数式状态迁移，
// 因此“重放 N 次”和“重放 1 次”结果完全一致。
func applyRecord(st *metaState, rec walRecord) error {
	p := rec.Payload
	switch rec.Type {
	case recCreateVolume:
		name := p["volume"].(string)
		size := int64(p["size"].(float64))
		if _, ok := st.Volumes[name]; !ok {
			st.Volumes[name] = &volumeMeta{Name: name, Size: size, Blocks: map[uint64]string{}}
		}
	case recDeleteVolume:
		name := p["volume"].(string)
		if v, ok := st.Volumes[name]; ok {
			v.Deleted = true
			st.pruneDeletedVolume(name)
		}
	case recWrite:
		name := p["volume"].(string)
		blocks, err := asBlockMap(p["blocks"])
		if err != nil {
			return err
		}
		v, ok := st.Volumes[name]
		if !ok {
			return fmt.Errorf("%w: write to unknown volume %q", ErrCorrupt, name)
		}
		if v.Deleted {
			return fmt.Errorf("%w: write to deleted volume %q", ErrCorrupt, name)
		}
		for idx, id := range blocks {
			v.Blocks[idx] = id
		}
	case recCreateSnap:
		id := p["snapshot"].(string)
		vol := p["volume"].(string)
		if _, ok := st.Snapshots[id]; ok {
			break // 幂等
		}
		blocks, err := asBlockMap(p["blocks"])
		if err != nil {
			return err
		}
		if _, ok := st.Volumes[vol]; !ok {
			return fmt.Errorf("%w: snapshot of unknown volume %q", ErrCorrupt, vol)
		}
		st.Snapshots[id] = &snapshotMeta{
			ID:     id,
			Volume: vol,
			Name:   id,
			Blocks: blocks,
		}
	case recDeleteSnap:
		id := p["snapshot"].(string)
		if snap, ok := st.Snapshots[id]; ok {
			vol := snap.Volume
			delete(st.Snapshots, id)
			if v, ok := st.Volumes[vol]; ok && v.Deleted {
				st.pruneDeletedVolume(vol)
			}
		}
	case recRestore:
		vol := p["volume"].(string)
		snapID := p["snapshot"].(string)
		snap, ok := st.Snapshots[snapID]
		if !ok {
			return fmt.Errorf("%w: restore from missing snapshot %q", ErrCorrupt, snapID)
		}
		blocks, err := asBlockMap(p["blocks"])
		if err != nil {
			return err
		}
		v, ok := st.Volumes[vol]
		if !ok {
			// 允许还原到一个已被彻底移除元数据的卷名：以快照卷身份复活。
			v = &volumeMeta{Name: vol, Size: 0, Blocks: map[uint64]string{}}
			st.Volumes[vol] = v
		}
		size := int64(0)
		if src, ok := st.Volumes[snap.Volume]; ok {
			size = src.Size
		}
		v.Size = size
		v.Blocks = blocks
		v.Deleted = false
	case recClone:
		snapID := p["snapshot"].(string)
		name := p["volume"].(string)
		size := int64(p["size"].(float64))
		snap, ok := st.Snapshots[snapID]
		if !ok {
			return fmt.Errorf("%w: clone from missing snapshot %q", ErrCorrupt, snapID)
		}
		_ = snap
		blocks, err := asBlockMap(p["blocks"])
		if err != nil {
			return err
		}
		if _, ok := st.Volumes[name]; !ok {
			st.Volumes[name] = &volumeMeta{
				Name:    name,
				Size:    size,
				Blocks:  blocks,
				Deleted: false,
			}
		}
	default:
		return fmt.Errorf("%w: unknown wal record type %q", ErrCorrupt, rec.Type)
	}
	return nil
}

// asBlockMap 将 WAL 记录中的块映射（map[string]any，JSON 解码形态）
// 转换回 map[uint64]string；缺省视为空映射。
func asBlockMap(v any) (map[uint64]string, error) {
	if v == nil {
		return map[uint64]string{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: blocks payload is not an object", ErrCorrupt)
	}
	return mapToUint64(m)
}

func loadCheckpoint(path string) (*checkpointFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cf checkpointFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return nil, fmt.Errorf("%w: meta.json: %v", ErrCorrupt, err)
	}
	return &cf, nil
}

// writeCheckpoint 原子写入 meta.json（临时文件 + fsync + rename + 目录 fsync）。
func writeCheckpoint(dataDir string, st *metaState, disableSync bool) error {
	cf := checkpointFile{
		LastSeq:   st.NextSeq,
		Volumes:   st.Volumes,
		Snapshots: st.Snapshots,
		SeenReq:   st.SeenReq,
	}
	data, err := json.MarshalIndent(&cf, "", "  ")
	if err != nil {
		return err
	}
	final := filepath.Join(dataDir, "meta.json")
	tmp, err := os.CreateTemp(dataDir, ".meta-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if !disableSync {
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		return err
	}
	return syncParentDir(final)
}
