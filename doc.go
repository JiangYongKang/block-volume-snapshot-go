// Package blocksnap 实现了一套本地块存储卷的快照、克隆与后台回收能力。
//
// 语义概要（详见 README）：
//   - 快照基于写时复制（COW），快照保存创建时点的块映射。
//   - 块内容寻址、不可变，按引用计数共享与回收。
//   - 所有变更先写 WAL（fsync）再应用；崩溃后重放 WAL 恢复一致性。
package blocksnap
