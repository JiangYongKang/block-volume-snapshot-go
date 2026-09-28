package blocksnap

// idempotency:
//
// 每个变更操作都由调用方提供一个 clientID（幂等键）。Store 在执行前检查
// metaState.SeenReq：已提交过的键直接返回成功而不重复执行，因此“请求重试 /
// 响应丢失后重发”不会产生重复快照、错误引用计数或重复数据块。
//
// 幂等键随 WAL 记录持久化，崩溃恢复后依然有效（见 recovery.go）。
// 空 clientID 表示调用方不要求幂等（例如后台内部操作）。

// idemSeen 判断幂等键是否已提交。
func (s *Store) idemSeen(clientID string) bool {
	if clientID == "" {
		return false
	}
	return s.meta.SeenReq[clientID]
}

// idemMark 标记幂等键已提交（与 WAL append 在同一临界区内完成）。
func (s *Store) idemMark(clientID string) {
	if clientID != "" {
		s.meta.SeenReq[clientID] = true
	}
}
