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
| `pkg/drill` | 可重复、基于证据的故障演练与机器可读审计报告 |
| `cmd/qlog` | 统一的本地启动、脚本化验证与故障演练入口 |

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
- **追赶不可回退（含重复投递/重传）**：节点判断追赶内容是否过旧时依据的是
  **已提交前沿（commit）**而不仅是本地快照点。节点装入快照后，往往会先靠增量
  `AppendEntries` 把 commit/applied 推进到更靠后的位置，之后才自己做一次压缩；
  在这个窗口内，如果一个**重复或重传的旧 `InstallSnapshot`**（重试、网络重放、
  或没看到第一次 ack 的领导者再发）到达，其 `LastIncludedIndex <= commit`，节点
  只回 ack 让领导者继续推进 `nextIndex`，**绝不**重新 `Restore()` 旧前缀——否则会
  出现“已应用位置报到最新、实际内容却丢了前面已确认写入”的分裂。同样，
  `installSnapshot` 自身对 `index <= applied` 幂等跳过，作为第二道防线；冲突的
  `AppendEntries` 若要截断 `<= commit` 的已提交前缀会被直接拒绝。位置只能单调
  向前，所有状态改动仍只在单事件循环上发生，因此并发读写不会出现半更新。

## 只读观测

`cluster.Observe()` 与 `raft.Node.Status()` 返回无锁缓存的只读快照
（成员、领导归属、term、commit/applied/快照位置、各节点同步进度、活跃与已提交
配置）。观测不发送任何 Raft 消息、不移动定时器、不改变状态。

## 本地验证（离线、可重复）

统一入口默认启动 5 节点集群并执行：选举、初始写入、幂等重试、少数派分区、
崩溃重启、加节点、加入后继续写入、快照压缩与落后追赶、并发写入：

```
go run ./cmd/qlog
```

需要**可留存、可复核的故障演练与审计报告**时使用 `-drill`，详见下文
「故障演练」一节：

```
go run ./cmd/qlog -drill all
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
- `TestSnapshotCompactionAndCatchup` /
  `TestDuplicateSnapshotAfterProgressCannotRollback`
- `TestConcurrentWritersAndReaders` / `TestDuplicateClientRequestIdempotent` /
  `TestRetransmittedAppendEntriesCannotTruncateCommitted`

运行时日志以 `STATE` 打印每次角色/提交/配置状态转换及其判定依据，以 `INFO`
打印投票、复制、追赶细节，便于解释状态转换。

## 配置开关

`cluster.Options.SyncWrites`（默认 `false`）控制本地存储是否真正 `fsync`。
模拟器默认关闭 fsync 以避免宿主/沙箱文件系统卡顿阻塞事件循环；磁盘格式的
崩溃安全性仍由损坏/截断/重启测试验证。需要演练真实 fsync 持久化时设为 `true`。

## 故障演练（可留存、可复核、可重复）

除了“跑一遍看结果”的脚本化验证，`cmd/qlog` 还内置了四套**故障演练**，全部离线
运行（不依赖网络、外部服务或人工盯日志），每套结束产出一份**机器可读 JSON
审计报告**，并且只有在“故障确实生效”且“安全性质成立”两条都有证据时才给 PASS。

### 运行方式

```
go run ./cmd/qlog -list-drills                 # 列出全部演练名
go run ./cmd/qlog -drill all                   # 顺序跑完四套，报告打到 stdout
go run ./cmd/qlog -drill majority-unreachable  # 只跑某一套
go run ./cmd/qlog -drill all -report-out out/  # 每套一个 JSON 文件
go run ./cmd/qlog -drill leader-crash-retry -report-out r.json
```

退出码非 0 表示至少一套演练结论为 FAIL；人类可读的每条判定也会同时打印。

| 演练名 | 覆盖的故障与安全性质 |
| --- | --- |
| `majority-unreachable` | 多数派整体崩溃不可达时，幸存者无法确认新写入、提交位置不前进；多数派以更高任期恢复后，被拒写入绝不被追认，全集群收敛为同一份历史 |
| `leader-crash-retry` | 写入结果未知的瞬间领导者硬崩溃；用同一 `(client,seq)` 经新领导者重试，状态机去重表证明该请求只生效一次（首次已提交则重试命中去重，首次丢失则重试恰好生效） |
| `offline-compaction-catchup` | 节点离线足够久、错过至少一次压缩（领导者快照点严格越过其最后提交位置）后返回，经 InstallSnapshot 装入快照再增量追赶，窗口前后数据无空洞、全集群收敛；并在其已追过旧快照点后**固定重放该旧快照**，断言重复/重传追赶不能回退状态 |
| `membership-leader-switch` | 加节点变更在途（观察到 learner 暂存或 joint 配置）时领导者硬崩溃；故障切换后变更收敛为唯一一致配置，绝不半更新，写入与收敛保持正确 |

### 审计报告字段（固定顺序）

- `schema`：报告格式版本（当前 `qlog-drill/v1`）。
- `scenario` / `input`：演练名与**固定输入**（节点数、各阶段写入条数、键前缀、
  崩溃/变更的固定延迟毫秒）。同一 `input` 复跑应得到等价结论。
- `passed`：总判定。它**只**由下面 `checks` 是否全部通过派生，脚本跑完不等于通过。
- `checks[]`：每条判定 `{id, passed, evidence}`。`id` 与 `passed` 是稳定事实；
  `evidence` 是给人看的细节（含当轮具体节点/任期，允许跨轮不同）。
- `leader_trace[]`：各阶段观察到的领导者与任期（谁在何时主导、是否发生切换）。
- `faults[]`：实际注入的故障（类型、阶段、涉及节点数、`took_effect`、证据）。
  报告会**逐条**区分并列出每套演练真正隔离 / 崩溃 / 重启了哪些节点，`kind` 取值：
  - `crash`：硬崩溃停机（传输层注销、磁盘保留），`nodes` 为被停掉的节点；
  - `restart`：从磁盘目录重载恢复，`nodes` 为被拉起的节点；
  - `partition`：双向网络隔离，`nodes` 为隔离两侧的全部节点；
  - `retransmit`：追赶演练专用，表示固定重放的重复追赶 RPC（旧 `InstallSnapshot`
    在节点已追过后再次送达）。

  每条故障都带 `took_effect` 和实测证据（例如崩溃后确实不可达、重启后确实可达、
  分区双向确实不通、重放确实被 ack 且位置/内容未回退）；只要有一条故障没生效，
  对应 check 失败，整场演练判 FAIL。
- `writes[]`：关键写入的命运——被接受还是被拒、去重命中次数、**生效次数**。
- `compaction`：追赶演练专用——离线时提交位置、返回时领导者快照边界、是否真的
  跨过压缩点、实际装入的快照位置。
- `membership`：成员变更演练专用——是否观察到 joint、新旧领导者、是否切换、最终选民。
- `convergence`：最终各节点 commit/applied/快照位置、选民、状态摘要，以及
  `history_hash`（规范化的已提交 KV 内容摘要）与 `reached`。
- `started_at_iso` / `duration_ms`：时间戳与耗时，**允许**每次不同。
- `content_hash`：对除时间外所有“历史事实”做的 SHA-256，用于复跑比对。

### 可重复性边界（重要）

复跑同一固定输入时**必须一致**的是：总判定、每条 check 的 `id/passed`、故障是否
生效、写入接受/拒绝与生效次数、是否跨过压缩点、是否发生领导权切换、最终 commit/
applied 位置、最终已提交历史（`history_hash`/`content_hash`）。

允许不同的是：时间戳与耗时；**具体哪台机器**被选为领导者或受害者（选举超时是
随机的）；节点的具体 term 编号；追赶演练里离线节点停下时的**具体提交位置**
（可能因首个领导者提交启动条目的心跳时序相差一）。这些身份/调度信息被刻意排除在
`content_hash` 之外（领导权“是否切换”“是否真跨过压缩点”作为布尔事实保留），因此
不同机器中选或离线位置差一都不会让历史结论漂移；该具体位置仍完整保留在 JSON 报告
的 `compaction.offline_commit_at` 中供查看。

用固定输入复跑并比对（离线、确定性）：

```
go run ./cmd/qlog -drill all -report-out run1/
go run ./cmd/qlog -drill all -report-out run2/
# 比较每个场景 JSON 的 content_hash 与每条 checks[].passed；
# started_at_iso / duration_ms / evidence 文本 / 具体节点名与 term 可忽略
```

追赶演练对“重复投递/重传”的复现也是**固定触发**而非碰运气：脚本先等追赶快慢的
节点完全收敛，再抓下领导者当时用于追赶的快照（位置固定为 64，由固定写入条数与
快照阈值 32 决定），在该节点已经追过该快照点后**同步重放 3 次**同一份
`InstallSnapshot`，断言每次都被 ack、applied/commit 与 KV 内容哈希保持不变。因此
同一固定输入重复执行，`duplicate-catchup-no-rollback` 的结论与 `content_hash`
都稳定一致。

### 证据原则：故障没真正生效就必须失败

演练判定**不看步骤有没有跑到**，只看证据。例如：

- 声称“多数派不可达”，必须实测旧领导者当前可达选民数 < 多数派，且恢复时多数派侧
  选出**更高任期**的新领导者（否则被隔离领导者的未提交尾巴可能被错误追认）；
- 声称“节点离线”，必须实测其在传输层不可达；声称“分区”，必须实测隔离两侧**双向**
  不可达；声称“节点重启/恢复”，必须实测其重启后确实重新可达并重新参与；
- 声称“跨过压缩点”，必须实测领导者快照索引严格大于该节点离线时的最后提交位置；
- 声称“领导权切换”，必须实测出现不同节点且任期严格增大；
- 声称“重复追赶未回退”，必须实测旧快照重放后节点仍 ack，且 applied/commit 位置与
  已提交 KV 内容哈希都未改变。

测试用一个**空操作故障注入器**（Crash/Restart/分区全部 no-op）、一个“只让单台
宕机”的不足范围注入器，以及一个“真崩溃但 Restart 是 no-op”的注入器验证：这些
情况下演练一律 FAIL，绝不会因为脚本走完而 PASS。

### 自动化测试

```
go test ./pkg/drill/            # 四类演练成功判定、失败判定、可重复性
go test ./...                   # 含既有正常写入/崩溃恢复/成员变更/压缩追赶不回归
go test -race ./...             # 数据竞争
```

关键用例：

- `TestAllDrillsPassWithFixedInput`：四套演练在固定输入下全部 PASS；
- `TestRepeatableVerdict`：同一输入独立复跑两次，判定与 `content_hash` 一致；
- `TestNoFault*DrillFails` / `TestInsufficientFaultScopeFails` /
  `TestCrashWithoutRestartDrillFails`：故障不生效、范围不足、或崩溃后根本没重启时
  演练必须 FAIL（异常用例不能只检查流程走到）；
- `TestReportIsMachineReadable`：报告可序列化、字段齐全、哈希可重算。

`pkg/cluster` 中与追赶/重传语义直接相关的确定性用例（固定触发、无需多跑碰运气）：

- `TestDuplicateSnapshotAfterProgressCannotRollback`：离线错过压缩后返回、装好快照
  并增量追过该点，随后**按固定位置重放旧 InstallSnapshot**（含并发读写下的连续
  重放），断言 applied/commit 不回退、KV 内容哈希不变、最终全集群一致；
- `TestStaleSnapshotOlderThanLocalSnapshotAcked`：收到比本地已装快照更旧的快照时
  只 ack 不装入；
- `TestRetransmittedAppendEntriesCannotTruncateCommitted`：迟到的冲突 AppendEntries
  不能截断已提交前缀，且正常复制不被误伤。

### 当前**未**覆盖的故障类型

演练聚焦四类最关键的协调故障，尚**未**模拟：真实网络磁盘 IO（走的是进程内传输与
本地文件，`SyncWrites=false` 时不真正 fsync）；拜占庭/恶意节点；持续随机丢包/延迟
叠加（传输层支持 `DropRate/SetLatency`，但演练用确定性故障）；时钟漂移；脑裂后双写
的人为客户端；快照文件物理损坏下的追赶；以及删除领导者（remove-leader）与故障
叠加。这些是后续可扩展的方向，新增演练只需实现 `drill.Faults` 与一个场景函数并
登记到 `drill.Scenarios()`。

