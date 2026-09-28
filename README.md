# block-volume-snapshot-go

一套基于本地磁盘模拟的**块存储卷快照 / 克隆 / 后台回收**库。核心目标是：在任意
操作顺序（并发读写、删除后还原、进程中途被杀）下，数据始终一致、结果可解释。

- 语言：Go 1.25，仅用标准库
- 包路径：`github.com/highcumontoa/block-volume-snapshot-go`
- 数据完全落在一个本地目录中，无外部依赖

```go
s, _ := blocksnap.Open("./data", blocksnap.Options{Logf: t.Logf})
s.CreateVolume("req-1", "vol", 4<<20)
s.WriteAt("req-2", "vol", 0, data)
s.CreateSnapshot("req-3", "vol", "snap1") // COW 快照
s.Restore("req-4", "vol", "snap1")        // 还原到快照时点
```

---

## 1. 数据模型与目录布局

```
data/
├── blocks/        # 内容寻址（sha256）的不可变数据块，文件名即块 id
│   └── <64 位十六进制 sha256>
├── wal.log        # 预写日志：8 字节大端长度前缀 + JSON 帧
└── meta.json      # 检查点（压缩后的元数据基线），原子 rename 生成
```

- 一个卷由固定大小（默认 4096B）的逻辑块序列组成，`volumeMeta.Blocks` 维护
  `逻辑块号 -> 物理块 id` 的映射；未写入的逻辑块是稀疏洞，读出来全为零。
- 快照保存其创建时点卷块映射的**深拷贝**（见 `snapshotMeta.Blocks`）。
- 物理块不可变；所有"改写"都写入一个**全新的内容寻址块**（写时复制 COW），
  再原子切换映射指针。因此快照引用的块永远不会被后来的写入修改。
- 每个物理块的**引用计数**= 它出现在"所有卷映射 + 所有快照映射"中的次数。
  计数为 0 的块由后台 GC 物理删除。

## 2. 快照与还原语义

1. **时点一致性**：`CreateSnapshot` 与前台读写共用一把写锁，并在**单条 WAL
   记录**里提交完整块映射。快照要么看到一次写入之前的全部块，要么看到之后的
   全部块，绝不出现"一个逻辑块前后两半来自不同版本"的部分可见。
2. **快照只读**：快照内容不可改。之后对卷的任何写入都经读-改-写生成新块，
   快照块 id 保持不变（测试 `TestSnapshotPointInTime` 强校验 id 不漂移）。
3. **还原是精确回滚**：`Restore(volume, snapshot)` 把卷映射替换为快照映射，
   还原后逐字节等于快照时点；被新数据占用、还原后无人引用的块计数归零。
4. **删除卷仍可还原**：`DeleteVolume` 只把卷标记为 `Deleted`。只要该卷还有
   快照存在，卷元数据与快照块都会保留，`Restore` 会把卷原样复活。当最后一个
   相关快照也被删除后，卷元数据才被清除（`pruneDeletedVolume`），其块交给 GC。
5. **克隆共享、写入分叉**：`CloneVolume(snapshot, newVolume)` 直接复用快照的
   块映射（引用计数增加，不复制数据）；克隆卷之后写入时按 COW 分叉，与源卷、
   快照互不影响（`TestCloneIndependence`）。
6. **部分块 / 跨块写**：非对齐写入走读-改-写；一次用户 `WriteAt` 涉及的多个
   块在同一条 WAL 记录中提交，具备原子性。

## 3. 引用计数与后台回收

- 引用计数**不从 WAL 增量维护**，而是每次提交后、每次恢复时由当前全部映射
  **重算**（`metaState.recomputeRefs`）。这从根上消除了"增量加减在中断后
  偏差半个计数"这类问题。
- 后台 GC（默认开启，可用 `Options.DisableGC` 关闭）周期性删除计数为 0 的块；
  也可调用 `s.ForceGC(ctx)` 同步回收。
- **GC 安全不变量**：新块文件"先落盘、后写 WAL"。为避免 GC 删掉"文件已写好、
  但映射尚未提交"的在途块，GC 的"引用集合快照 + 物理删除"全程持有写锁
  （见 `collectGC` 注释）。仍被引用的块永远不会出现在删除集合中。
- GC 只删除 64 位十六进制命名的正式块文件，绝不触碰 `.tmp-*` / `.meta-*`
  临时文件。

## 4. 崩溃恢复规则

每个变更操作的提交顺序是：

```
1. （写操作）把新内容块原子写到 blocks/（tmp 文件 + fsync + rename + 目录 fsync）
2. 向 wal.log 追加该操作的完整记录并 fsync
3. 把记录应用到内存元数据、重算引用计数
```

进程在任意时刻被杀，重新 `Open` 时执行 `recoverStore`：

| 崩溃时机 | 恢复结果 |
| --- | --- |
| 块文件已写、WAL 未提交（步骤 1 后、2 前） | 该操作**整体不存在**（无部分可见）；留下的块是孤儿，Open 时 sweep 安全回收；已提交数据完好。见 `TestCrashWriteBlockBeforeWAL` |
| WAL 已 fsync、内存未应用（步骤 2 后、3 前） | 重放后操作**完整生效**——已完成操作不丢。见 `TestCrashSnapshotAfterWAL` |
| WAL 帧只写了一半（撕裂尾） | 长度分帧读取在第一个不完整帧处停止并截断尾部，已提交记录不受影响。见 `TestTornWALTail` |
| 检查点 meta.json 已写、WAL 未截断 | 以 meta.json 为基线重放整段 WAL（记录 seq ≤ 检查点 seq 被跳过），结果一致。见 `TestCrashDuringCheckpoint` |
| 检查点截断之后崩溃 | meta.json 即最新状态，WAL 为空，同样一致 |

恢复还会：

- **校验完整性**：所有被引用块必须在磁盘上存在，缺失则返回 `ErrCorrupt`；
- **去重**：同一 `clientID` 的记录只生效一次，因此"操作已落盘但响应丢失后
  重试"不会产生重复快照 / 重复数据 / 错误计数；
- **清除无引用的已删卷元数据**并重算全部引用计数。

### 幂等与可区分的错误

- 所有写操作都接受一个 `clientID` 作为幂等键（随 WAL 持久化）。相同键的重试
  直接返回成功且不重复执行（`TestIdempotentRetry`）。
- 失败原因用哨兵错误按类型区分，调用方用 `errors.Is` 判断：
  `ErrNotFound`、`ErrAlreadyExists`、`ErrConflict`、`ErrCorrupt`、
  `ErrClosed`、`ErrInvalidArg`。

## 5. 验证方法

```bash
go build ./...
go vet ./...
go test ./...            # 全部单测
go test -race ./...      # 竞态检测（推荐）
go test -v -run TestCrash ./...   # 只看崩溃恢复用例（在子进程中以 exit 99 真实中断）
go run ./cmd/blocksnap -dir ./demo-data  # 端到端操作序列演示
```

测试覆盖与对应需求：

| 测试 | 覆盖点 |
| --- | --- |
| `TestConcurrentWritesAndSnapshot` | 8 写者 + 5 快照并发；快照无撕裂、旧快照不被污染 |
| `TestBackgroundGCConcurrentWrites` | 后台 GC 与持续读写/快照并发；引用块不误删 |
| `TestSnapshotPointInTime` | COW：覆盖写后块 id 变化而快照 id 不变；还原字节一致 |
| `TestRefCountAndGC` | 计数边界：写/快照/覆盖/克隆/删快照/删卷每步计数与 GC 回收数 |
| `TestIdempotentRetry` | 同键重试不产生重复数据/快照、计数不膨胀；错误类型判定 |
| `TestDeleteVolumeStillRestorable` / `...AcrossRestart` | 删卷后、跨重启后仍可由快照还原 |
| `TestDeleteSnapshotPrunesVolume` | 最后一个快照删除后卷元数据被清理 |
| `TestCloneIndependence` | 克隆共享块、写入后 COW 分叉、源卷不受影响 |
| `TestSparseAndPartialWrite` | 稀疏零读、非对齐与跨块部分写 |
| `TestCrashWriteBlockBeforeWAL` | 块已写 WAL 未提交时崩溃：无部分可见、孤儿被回收、可重试 |
| `TestCrashSnapshotAfterWAL` | WAL 已提交后崩溃：重放后快照完整、幂等键仍生效 |
| `TestCrashDuringCheckpoint` | 检查点两步分别崩溃，状态一致 |
| `TestTornWALTail` | 撕裂 WAL 尾部被丢弃，已提交数据完好 |
| `TestRestartRoundTrip` / `TestRepeatedRestartsNoDrift` | 正常关闭重开、多次重启数据与块数不漂移 |

所有用例通过 `Options.Logf`（测试里接 `t.Logf`）打印**关键操作序列**
（`[seq=N] 操作 细节`）和**引用计数变化**（`refs=[块前缀:旧->新 ...]`），
运行 `go test -v` 即可看到。

## 6. 设计取舍

- 单把 `sync.RWMutex` 保证元数据操作串行化；块文件本身不可变，读路径只需
  RLock。实现简单且可证明正确，吞吐不是本项目目标。
- 检查点（meta.json）只是**加速手段**：WAL 超过 1MB 或正常 `Close` 时压缩。
- 崩溃点注入（`SetCrashPoint` / `SetCrashPointN`）让测试在子进程中用
  `os.Exit(99)` 模拟真实掉电，而不是进程内的模拟回滚。
