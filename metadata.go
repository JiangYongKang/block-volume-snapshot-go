package blocksnap

// volumeMeta 是卷的元数据。卷被删除后仍以 Deleted=true 保留，
// 只要它仍被快照（或由快照克隆出来的克隆链）引用，就还可以还原。
type volumeMeta struct {
	Name    string            `json:"name"`
	Size    int64             `json:"size"`
	Blocks  map[uint64]string `json:"blocks"` // 逻辑块序号 -> 物理块 id
	Deleted bool              `json:"deleted"`
}

// snapshotMeta 是快照元数据：保存创建时点卷的块映射副本（不可变）。
type snapshotMeta struct {
	ID     string            `json:"id"`
	Volume string            `json:"volume"`
	Name   string            `json:"name"`
	Blocks map[uint64]string `json:"blocks"`
}

// metaState 是内存中的完整元数据视图。
// 物理块引用计数不从 WAL 增量维护，而是每次恢复/检查点时由当前映射重算，
// 从根本上避免增量引用计数在中断后出现偏差。
type metaState struct {
	Volumes   map[string]*volumeMeta   `json:"volumes"`
	Snapshots map[string]*snapshotMeta `json:"snapshots"`
	RefCount  map[string]int           `json:"ref_count"` // 物理块 id -> 引用计数
	NextSeq   int64                    `json:"next_seq"`
	SeenReq   map[string]bool          `json:"seen_req"` // 已提交的客户端幂等键
}

func newMetaState() *metaState {
	return &metaState{
		Volumes:   map[string]*volumeMeta{},
		Snapshots: map[string]*snapshotMeta{},
		RefCount:  map[string]int{},
		SeenReq:   map[string]bool{},
	}
}

func cloneBlockMap(m map[uint64]string) map[uint64]string {
	if m == nil {
		return map[uint64]string{}
	}
	c := make(map[uint64]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// clone 返回元数据深拷贝（GC 安全比对快照等场景使用）。
func (m *metaState) clone() *metaState {
	c := newMetaState()
	c.NextSeq = m.NextSeq
	for name, v := range m.Volumes {
		c.Volumes[name] = &volumeMeta{
			Name:    v.Name,
			Size:    v.Size,
			Blocks:  cloneBlockMap(v.Blocks),
			Deleted: v.Deleted,
		}
	}
	for id, s := range m.Snapshots {
		c.Snapshots[id] = &snapshotMeta{
			ID:     s.ID,
			Volume: s.Volume,
			Name:   s.Name,
			Blocks: cloneBlockMap(s.Blocks),
		}
	}
	for k, v := range m.RefCount {
		c.RefCount[k] = v
	}
	for k, v := range m.SeenReq {
		c.SeenReq[k] = v
	}
	return c
}

// blockRefs 返回当前元数据中所有被引用物理块 id 的集合
// （所有存活/逻辑保留卷的块映射 + 所有快照的块映射）。
func (m *metaState) blockRefs() map[string]struct{} {
	refs := map[string]struct{}{}
	for _, v := range m.Volumes {
		for _, id := range v.Blocks {
			refs[id] = struct{}{}
		}
	}
	for _, s := range m.Snapshots {
		for _, id := range s.Blocks {
			refs[id] = struct{}{}
		}
	}
	return refs
}

// recomputeRefs 依据块映射重新计算每个物理块的引用计数。
func (m *metaState) recomputeRefs() {
	rc := make(map[string]int, len(m.RefCount))
	for _, v := range m.Volumes {
		for _, id := range v.Blocks {
			rc[id]++
		}
	}
	for _, s := range m.Snapshots {
		for _, id := range s.Blocks {
			rc[id]++
		}
	}
	m.RefCount = rc
}

func (m *metaState) refCountOf(id string) int { return m.RefCount[id] }

// pruneDeletedVolume 在卷已删除且不再被任何快照引用时移除其元数据。
func (m *metaState) pruneDeletedVolume(name string) {
	v, ok := m.Volumes[name]
	if !ok || !v.Deleted {
		return
	}
	for _, s := range m.Snapshots {
		if s.Volume == name {
			return
		}
	}
	delete(m.Volumes, name)
}
