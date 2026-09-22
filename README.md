# qlog — 本地复制日志与领导者协调（Raft）

一个**完全离线、自包含**的 Raft 复制日志实现：多个节点跑在同一进程里，
使用本地文件做持久化、使用一个可注入故障的内存网络通信。不依赖任何外部
服务、套接字、凭据或不可访问资源，可重复地模拟节点崩溃、重启、网络分区、
延迟/丢包、成员变更与日志压缩。

随附一个线性化的键值状态机，用于端到端演示“写入在足够多节点确认后才可见、
崩溃重启后已确认结果不丢、重复/乱序请求不重复生效、所有节点观察到一致的
提交顺序”。

## 目录结构（按职责拆分）

| 包 | 职责 |
| --- | --- |
| `pkg/raftproto` | 线路类型与消息：条目、硬状态、配置、四类 RPC |
| `pkg/logstore` | 带 CRC 的 WAL、原子硬状态文件、快照文件；区分损坏/截断 |
| `pkg/transport` | 进程内模拟网络：注册、可达性、分区、延迟、随机丢包 |
| `pkg/statemachine` | 线性化 KV 应用 + 基于 `(clientID, seq)` 的幂等去重表 + 快照 |
| `pkg/raft` | Raft 核心：选举、日志复制、提交、read-index、联合共识成员变更、快照追赶 |
| `pkg/cluster` | 多节点编排、崩溃/重启/分区/加删节点、客户端、只读观测入口 |
| `cmd/qlog` | 统一的本地启动与脚本化验证入口 |

所有 Raft 状态只在**单事件循环 goroutine** 上修改；网络收发在独立 goroutine
中进行并以事件形式回到循环，因此不存在对核心状态的数据竞争（`go test -race`
通过）。

## 一致性语义

- **多数派提交**：一条日志只有在当前任期被多数派复制后才推进 commit。
  追随者仅按领导者的 `leaderCommit` 推进，且在附加前做 `(prevLogIndex,
  prevLogTerm)` 一致性检查并截断冲突后缀，保证已提交内容永不回滚。
- **同一任期最多一个领导者**：每个节点每任期只投一票并持久化；选票绑定
  “竞选代次（vote generation）”，迟到/重复的在途 `RequestVote` 响应不会被
  计入新的选举；日志落后的候选者拿不到票。
- **多数派不可达时不能提交、也不能产生新任期**：选举前有 pre-vote 可达性
  保护，被隔离的少数派不会自增 term 去扰乱多数派的稳定领导。
- **提交后才可见**：客户端 `Propose` 等到条目被 apply 才返回；线性化读走
  read-index（领导者心跳获得多数派确认 + 本地 applied 追上读索引）。
- **崩溃不丢已确认结果**：term/vote 原子落盘，WAL 条目 fsync 后才确认；
  重启时从「快照 + WAL 已提交前缀」重建应用状态，已确认写入不丢。
- **重复、超时重试、乱序不重复生效**：客户端携带稳定 `clientID` 与单调
  `seq`，应用按 `(clientID, seq)` 去重，结果未知的超时安全重试，不会生效两次。

## 故障模型与本地状态损坏

WAL 记录格式：

```
0x51 0x4C | type(1) | len(4) | payload(len) | crc32(4)
```

- CRC/魔数/长度/连续性错误 → `logstore.ErrCorruptLog`；
- 尾部记录写到一半（torn write）→ `logstore.ErrTruncatedLog`；
- 快照校验失败 → `logstore.ErrCorruptSnapshot`；
- 用更旧快照覆盖更新快照 → `logstore.ErrSnapOutOfDate`。

节点启动时**急切校验**本地状态；一旦损坏即以可区分的错误拒绝启动，绝不以
不确定状态对外服务。截断仅允许发生在“尚未被快照覆盖”的尾部，已提交/已快照
区间不能被压缩或回滚。

## 动态成员变更规则

采用 Raft **联合共识（joint consensus）**两阶段提交：

```
C_old  ──append+commit──▶  C_{old,new} (joint)  ──append+commit──▶  C_new
```

- 一次只允许一个变更；配置在其日志条目**提交之后**才生效，未稳定前不是既成事实；
- joint 期间选举与提交都需要**新旧两个多数派**，因此新配置不可能在联合条目
  持久化前单方面行动，变更过程中已提交数据不丢不错位；
- 全新节点先作为**无投票权的 learner** 加入并追赶，追上领导者尾部后才提升为
  选民，避免一个空日志节点拉低集群可用性；
- 删除节点走同样的两阶段；被移除的现任领导者在最终配置提交后下台并停止竞选。

## 日志压缩与落后节点追赶

- 应用位置超过阈值后：序列化应用快照 → 原子写快照文件（含当时成员配置）→
  仅删除被快照覆盖的 WAL 前缀。顺序保证崩溃时要么旧前缀还在、要么新快照已在，
  不产生空洞。
- 长期离线节点的 `nextIndex` 落入快照区间时，领导者改发 `InstallSnapshot`；
  节点装入快照后再增量接收后续条目，无空洞、不回滚。

## 只读观测

`cluster.Observe()` 与 `raft.Node.Status()` 返回无锁缓存的只读快照
（成员、领导归属、term、commit/applied/快照位置、各节点同步进度、活跃与已提交
配置）。观测不发送任何 Raft 消息、不移动定时器、不改变状态。

## 本地验证（离线、可重复）

统一入口会启动 5 节点集群并执行：选举、初始写入、幂等重试、少数派分区、
崩溃重启、加节点、加入后继续写入、快照压缩与落后追赶、并发写入：

```
go run ./cmd/qlog
```

运行测试（含分区、故障切换、崩溃重启、损坏拒绝、成员变更、快照追赶、
并发读写/变更）：

```
go test ./...
go test -race ./...
```

关键测试：

- `TestPartitionMinorityNoCommit` / `TestPartitionMajorityElectsNewLeader`
- `TestCrashRestartPreservesCommitted` / `TestCrashLeaderBeforeAckLosesNothing`
- `TestCorruptStateRefused` / `TestTruncatedTailIsDistinguishable`
- `TestAddServerReplicatesThenServes` / `TestRemoveServerThenCommit` /
  `TestMembershipChangeSurvivesLeaderSwitch`
- `TestSnapshotCompactionAndCatchup`
- `TestConcurrentWritersAndReaders` / `TestDuplicateClientRequestIdempotent`

运行时日志以 `STATE` 打印每次角色/提交/配置状态转换及其判定依据，以 `INFO`
打印投票、复制、追赶细节，便于解释状态转换。

## 配置开关

`cluster.Options.SyncWrites`（默认 `false`）控制本地存储是否真正 `fsync`。
模拟器默认关闭 fsync 以避免宿主/沙箱文件系统卡顿阻塞事件循环；磁盘格式的
崩溃安全性仍由损坏/截断/重启测试验证。需要演练真实 fsync 持久化时设为 `true`。
