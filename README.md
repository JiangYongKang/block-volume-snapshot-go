# 本地块存储卷：快照 / 克隆 / 写时复制 / 崩溃一致回收

一个零外部依赖（仅 Go 标准库）的本地块存储演示：用本地磁盘文件模拟卷数据，
在**任意操作顺序**、**并发读写**、**进程中途被杀死**三种情况下，都保持

- 快照内容与创建时点逐块一致，不被后续写污染；
- 卷删除后只要还有快照/克隆引用，仍可还原；
- 物理块引用计数准确，后台回收只删“无任何引用”的块；
- 崩溃重开后：已完成的操作不丢，未完成的操作不留半成品。

## 1. 快速上手

```go
dir, _ := os.MkdirTemp("", "bvs")
s, _ := blockvol.Open(dir,
    blockvol.WithBlockSize(4096),
    blockvol.WithGCInterval(time.Second),
    blockvol.WithLogger(blockvol.StdLogger()), // 打印关键操作与引用计数
)
defer s.Close()

vol, _ := s.CreateVolume("vol", 8)                 // 8 个逻辑块
_ = vol.WriteBlock(0, data)                         // 覆盖写（COW）
snap, _ := vol.CreateSnapshot("snap-1")            // 时点快照
b, _ := snap.ReadBlock(0)                           // 只读
clone, _ := s.CloneVolume("vol", "clone-1")        // 克隆，与源共享物理块
restored, _ := s.RestoreSnapshot("snap-1", "vol2") // 还原/重建卷
```

测试里可以直接观察引用计数：

```go
s.TotalRefs()        // 当前活引用总数
s.RefsSnapshot()     // map[物理块ID]引用数
s.CheckRefConsistency() // 与元数据全量核算结果比对（返回 "" 表示一致）
s.RunGC(ctx)         // 手动回收，返回 GCStats{Scanned,Reclaimed,LiveBlocks}
```

## 2. 数据模型

| 对象 | 说明 |
| --- | --- |
| 卷 Volume | 固定大小的逻辑块数组 `Blocks[逻辑块号] -> 物理块ID`，空槽表示全零块 |
| 快照 Snapshot | 创建瞬间对卷映射做**深拷贝**，之后不可变 |
| 物理块 | `blocks/<2位分片>/<sha256>`，**内容寻址、只写一次、永不原地修改** |

因为物理块内容寻址且不可变：

- **写时复制（COW）**：写一个逻辑块 = 落盘一个新物理块 → WAL 提交 → 把该槽切到新 ID。
  旧块原封不动，快照继续指向它。
- **快照/克隆零拷贝**：只复制映射数组；相同内容（含不同卷/快照写了相同数据）天然去重。
- **全零块不占空间**：写全零等价于空槽，读空槽返回整块 0。

## 3. 一致性设计（为什么并发下也正确）

`Store.metaMu` 是一把 `sync.RWMutex`，同时承担两个职责：

1. 串行化元数据与引用计数修改（写操作/快照/删除/还原走 **Lock**）；
2. 让“读块”与“GC 删块”互斥（读走 **RLock**，覆盖“查映射 + 读物理文件”全过程）。

由此得到：

- **快照是原子的**：创建快照在写锁内拷贝映射，得到的必然是某条已提交写入边界上的完整状态，
  绝不会出现“块 A 是旧值、块 B 是新值”这种跨时点的撕裂。
- **快照期间并发写安全**：新写产生新物理块，不改旧块；快照映射不变。
- **读不撕裂**：读者要么拿到旧 ID 读旧块，要么在映射切换后拿到新 ID 读新块，二者都是整块。
- **GC 不误删**：GC 列举磁盘块后，在写锁内逐项复查引用数，`>0` 必跳过；
  新块落盘也在写锁内，排除了“刚写好但引用还没提交就被当孤儿删掉”的窗口。
- **重试幂等**：
  - 相同内容重复写：内容寻址命中原块，且映射相同时直接短路，不重复记账；
  - 卷重复删除：已删除再删返回 `nil`，不重复减计数；
  - 快照重复删除：返回可分类的 `ErrSnapshotNotFound`，引用计数不变化；
  - 物理块写文件、GC 删除文件本身都对“已存在/已消失”幂等。

引用计数规则：物理块引用 = 所有**活卷**映射中的出现次数 + 所有快照映射中的出现次数。
已删除卷的映射不再计数（其块靠快照/克隆存活）。每次提交都会在同一套
`applyRecord` 代码里增量维护计数；测试用 `CheckRefConsistency()` 把增量结果与
“从元数据全量重算”的结果逐块比对。

## 4. 崩溃恢复规则

存储目录布局：

```
dir/
├── store.lock              # flock 排他锁，进程死了内核自动释放
├── blocks/<xx>/<sha256>    # 不可变物理块；*.tmp 是未落定的临时块
├── meta/checkpoint.json    # 元数据检查点（tmp+rename 原子替换）
└── wal/wal.log             # 帧式预写日志：magic|len|crc32|json
```

提交协议（**副作用先落盘 → WAL 先提交 → 再改内存**）：

1. 写操作：新物理块先 `写临时文件 + fsync + rename + fsync目录`（对读者原子可见）；
2. 把操作以 `magic | 长度 | CRC32 | JSON` 帧 **append + fsync** 到 WAL；
3. WAL 返回成功后才修改内存映射与引用计数。

打开存储时按顺序恢复：

1. 加 `flock` 排他锁（双开直接 `ErrLocked`）；
2. `replay` 读取 WAL：**第一个损坏/截断帧即视为崩溃尾部并截断**（CRC 防 torn write），
   之前的完整帧全部有效；
3. 载入最近检查点，从 `NextSeq` 开始重放之后的 WAL（带单调序号，绝不重复应用）；
4. 删除所有遗留 `*.tmp` 物理块（它们从未被任何已提交元数据引用）；
5. 把恢复后的元数据固化成新检查点并清空 WAL，得到干净的新起点。

崩溃矩阵：

| 崩溃发生点 | 结果 |
| --- | --- |
| 物理块临时文件写一半 | 留下 `*.tmp`，重开被清理；映射从未指向它 |
| 物理块 rename 后、WAL 前 | 孤儿块，无引用，下次 GC 回收；数据语义无影响 |
| WAL 帧写一半（断电） | CRC/长度不匹配，重放时在该帧处截断，该操作视为未提交 |
| WAL 提交后、改内存前 | 重放时应用，操作不丢 |
| 检查点 tmp 写一半 | rename 未发生，继续用旧检查点 + WAL |
| 检查点 rename 后、清 WAL 前 | 重放时按 `NextSeq` 跳过已并入检查点的记录 |

持久化粒度：每次 `Open`/正常 `Close` 以及每 32 条已提交操作做一次检查点。
WAL 每条记录都已 fsync，所以检查点间隔只影响恢复时的重放量，不影响零丢失。

## 5. 错误类型（可用 `errors.Is` 区分）

| 错误 | 触发场景 |
| --- | --- |
| `ErrVolumeNotFound` | 读取/快照/克隆不存在或已删除的卷 |
| `ErrSnapshotNotFound` | 读取/删除/还原不存在的快照（重复删除也返回它） |
| `ErrVolumeExists` / `ErrSnapshotExists` | 重名创建 |
| `ErrOutOfRange` | 块号越界 |
| `ErrInvalidArgument` | 空名字、非法大小、数据长度不等于块大小、容量不匹配的还原 |
| `ErrClosed` | 关闭后继续使用 |
| `ErrLocked` | 目录已被另一个进程独占打开 |

语义约定：删除卷是**幂等成功**（墓碑仍在，便于区分“从未存在”与“删过了”）；
删除快照首次成功、再次调用返回 `ErrSnapshotNotFound`——两种重复都不会改动引用计数。

## 6. 验证方法

```bash
go test ./...            # 全部单测
go test -race ./...      # 竞态检测
go test -v ./...         # 查看关键操作序列与引用计数变化日志
```

测试覆盖：

- `TestSnapshotIsolationAndRestore`：快照隔离与还原逐块一致；
- `TestDeleteVolumeThenSnapshotRestore`：删卷后快照可读、可重建卷；
- `TestCloneSharesBlocks`：克隆共享块、源写不影响克隆、源删克隆存活；
- `TestIdempotentDeletes` / `TestRepeatedIdempotentWrites`：重复操作不产生重复数据或错误计数；
- `TestConcurrentWritersAndSnapshots`：多 writer/reader/snapshotter 并发，
  校验无撕裂读、快照单调合法、结束后引用计数一致；
- `TestGCBoundaries` / `TestGCKeepsClonedBlocks`：引用计数边界与 GC 不误删；
- `TestCrashRecoveryWrites` / `TestCrashRecoverySnapshots`：子进程执行操作时 **SIGKILL**，
  重开后数据与引用计数一致、无撕裂；
- `TestTornWalTail`：手工构造损坏 WAL 尾帧，验证截断恢复；
- `TestOrphanTempBlockCleanup`：遗留临时块清理；
- `TestInProcessCrashReplay` / `TestDeleteAllThenRecovery`：纯 WAL 重放、全删后恢复；
- `TestExclusiveLock`：同目录双开被拒，关闭后可再开。

所有测试都通过 `WithLogger` 把 `create/delete/write/snapshot/restore/gc` 操作
及每次操作后的引用总数打印到测试日志（`go test -v` 可见）。
