package blocksnap

import (
	"context"
	"errors"
	"time"
)

// gcLoop 是后台回收循环：周期性删除已无任何元数据引用的物理块。
func (s *Store) gcLoop(interval time.Duration) {
	defer s.gcWg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.gcStop:
			return
		case <-t.C:
			n, err := s.collectGC(context.Background())
			if err != nil {
				if errors.Is(err, ErrClosed) {
					return // Close 已开始，安静退出
				}
				s.logf("gc: error: %v", err)
				continue
			}
			if n > 0 {
				s.logf("gc: reclaimed %d block(s)", n)
			}
		}
	}
}

// collectGC 执行一次安全回收。
//
// 关键不变量：引用集合快照与物理删除必须在同一把写锁内完成。因为新块文件
// 总是【先落盘、后写 WAL 提交映射】，若引用快照与删除之间插入一次写入，
// 就可能删掉“块文件已存在、但映射尚未提交”的在途块。全程持写锁即可杜绝。
// 仅被已删除映射使用、但 WAL 尚未提交的块文件不会被本函数看到（引用集合
// 来自已提交元数据），等提交后引用计数 >0；若写入进程崩溃，该块则成为
// 孤儿，由下次 Open 恢复时的 sweep 回收。
func (s *Store) collectGC(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	refs := s.meta.blockRefs()
	n, err := s.blocks.sweep(func(id string) bool {
		_, keep := refs[id]
		return keep
	})
	if err != nil {
		return 0, err
	}
	if n > 0 {
		s.logf("gc: reclaimed %d unreferenced block(s); live blocks=%d", n, len(refs))
	}
	return n, nil
}
