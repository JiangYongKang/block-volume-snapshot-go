package blockvol

import (
	"context"
	"time"
)

// RunGC 执行一次标记-清除式回收：
//  1. 标记：在元数据中统计所有活引用（活卷映射 + 全部快照映射）；
//  2. 清除：列出物理块文件，删除没有任何活引用的块。
//
// 清除时重新获取写锁并逐项复查引用计数。因此：
//   - 读块路径持 RLock 贯穿“查映射 + 读文件”，GC 不会在读者读文件时删块；
//   - 写块路径持 Lock 落盘新块，GC 不会把“即将被引用”的块误删；
//   - 与标记结果相比，若期间块又被引用（内容寻址复用），复查时引用数 >0，跳过删除。
func (s *Store) RunGC(ctx context.Context) (GCStats, error) {
	// 阶段 0：在锁外枚举磁盘块（与读者无冲突）。
	ids, err := s.bs.list()
	if err != nil {
		return GCStats{}, err
	}

	// 阶段 1：标记 + 清除在写锁临界区内完成，保证引用计数与删除决策一致。
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if s.closed {
		return GCStats{}, ErrClosed
	}

	var stats GCStats
	stats.Scanned = len(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if n := s.refs[id]; n > 0 {
			stats.LiveBlocks++
			continue
		}
		// 无活引用：安全删除（remove 对“已不存在”幂等）。
		if err := s.bs.remove(id); err != nil {
			s.logf("WARN gc remove failed block=%s: %v", id, err)
			continue
		}
		stats.Reclaimed++
	}
	s.logf("gc scanned=%d live=%d reclaimed=%d", stats.Scanned, stats.LiveBlocks, stats.Reclaimed)
	return stats, nil
}

// startBackgroundGC 启动周期回收协程；返回停止函数。
func (s *Store) startBackgroundGC(interval time.Duration) func() {
	ctx, cancel := context.WithCancel(context.Background())
	s.stopGC = cancel
	s.gcWG.Add(1)
	go func() {
		defer s.gcWG.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := s.RunGC(ctx); err != nil {
					if err != ErrClosed && ctx.Err() == nil {
						s.logf("WARN background gc: %v", err)
					}
				}
			}
		}
	}()
	return cancel
}
