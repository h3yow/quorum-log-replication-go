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
| `pkg/drill` | 可重复、自校验的故障演练：固定输入脚本 + 证据采集 + 机器可读审计报告 |
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

## 可留存的故障演练（drill）

除“跑一遍看结果”的脚本化验证外，统一入口还提供一套**可单独选择、也可一次
跑完**、产出**机器可读审计报告**的故障演练。演练只驱动进程内模拟集群，不
访问任何外部服务、套接字或时钟之外的不确定源，也不靠人盯日志判断结果。

运行全部四套演练并在终端打印判定：

```
go run ./cmd/qlog -drill all
```

只跑其中一套：

```
go run ./cmd/qlog -drill majority-unreachable
go run ./cmd/qlog -drill leader-crash-dedup
go run ./cmd/qlog -drill snapshot-catchup
go run ./cmd/qlog -drill membership-leader-switch
```

把审计报告写到文件（`-` 表示 stdout；跑单套时输出一个对象，跑多套时输出
数组），并可保留演练用的磁盘数据目录以便事后复核：

```
go run ./cmd/qlog -drill all -report drills.json
go run ./cmd/qlog -drill snapshot-catchup -report one.json -drill-dir ./drill-data
```

进程退出码：全部 `PASS` 为 0，任一演练 `FAIL` 为 1，参数错误为 2，便于接入
CI 或交接脚本。

### 四套演练覆盖的故障

| 场景 id | 演练内容 | 必须成立的关键判定 |
| --- | --- | --- |
| `majority-unreachable` | 把在任领导者与全部对等节点隔离，使其连不到多数派；对该领导者发起一次新写入 | 隔离**确实**让其可触达选民少于多数派；新写入**未被确认**且其提交位置冻结；分区愈合后固定写入恢复提交，全员收敛 |
| `leader-crash-dedup` | 在一次写入结果未知（领导者被隔离、拿不到多数派确认）时**真正崩溃**该领导者，多数派选出更高任期的新领导者后，用**完全相同的 (client,seq) 载荷**重试，再投递第三次 | 旧领导者确实从网络注销；新领导者在更高任期产生；重试成功；第三次同载荷返回 `Dup=true`，效果恰好一次；旧领导者重启后对齐，不复活未提交分叉 |
| `snapshot-catchup` | 一个追随者在日志起点附近崩溃，集群写入足够多固定数据跨过 32 条压缩阈值并生成快照，再让该节点回归 | 节点确实离线；领导者快照边界**严格越过**该节点离线前位置（普通追加无法跨越）；恢复节点确实装入快照（快照位置越过边界），随后增量追上尾部，90 条固定写入一条不丢 |
| `membership-leader-switch` | 新增一个节点（先作为 learner 追赶），在成员变更**确实进行中**（已观察到 learner 阶段）时崩溃在任领导者 | 变更开始有据；旧领导者确实离线；存活多数派在**严格更高任期**选出不同领导者；变更经新领导者干净续完（非半更新），最终非联合选民集合在各节点一致提交，之后写入继续被确认 |

### “故障必须真的生效”，而不是“流程跑到了”

每个声称的故障都要由观测到的**证据**支撑，证据不成立整场演练判 `FAIL`：

- “多数派不可达”要在隔离后实测从该节点视角可送达的已提交选民数 `< n/2+1`；
- “节点真的离线”要确认该 id 已从传输层注销（区别于“活着但被分区”）；
- “落后者跨过压缩点”要同时证明领导者快照位置 > 离线节点停滞位置，且恢复后
  该节点的快照位置越过该边界；
- “领导权发生切换”要看到不同节点在严格更高任期成为可达领导者。

演练还内置了**故障注入（sabotage）反例**：故意让“故障”不真正生效（例如
不分区、不真崩溃、节点一直在线、不切换领导），此时演练**必须**判 `FAIL` 并
给出具体失败项。这些反例由自动化测试直接断言，确保判定不会因为“只是步骤
都跑到了”而误判通过。

### 审计报告字段（机器可读，固定顺序）

报告为 JSON：外层是 `schema`、`generated_at_ms`、`duration_ms`、`facts` 与
`digest`。`facts` 是**确定性事实主体**，`digest` 是其 canonical-JSON 的
SHA-256（十六进制）。主要字段：

- `scenario` / `nodes`：场景 id 与初始节点数；
- `isolated_roles` / `crashed_roles` / `restarted_roles`：实际被隔离、崩溃、
  重启的节点（用语义角色名，见下）；
- `leader_sequence`：`{role, term_offset}` 的有序领导/任期序列；
  `leadership_switched` 表明是否发生过不同节点在更高任期的切换；
- `write_outcomes`：每次固定写入在各阶段被接受（accepted）还是拒绝
  （rejected / 结果未知），以及“恰好一次 / 重试被去重”的判定；
- `snapshot_evidence`：离线角色、是否越过阈值、崩溃前离线位置、领导者压缩
  边界、恢复后快照位置、是否确实装入快照；
- `membership_evidence`：新增选民角色、崩溃时所处变更阶段（learner-catchup
  / joint-consensus）、最终选民集合、新旧领导者角色、新任期是否更高；
- `final_nodes`：各节点最终的 state、任期序号、commit/applied/快照位置、
  已提交选民/learner/联合选民、是否离线，按角色固定排序；
- `converged` + `history_digest`：所有在线节点是否收敛到**同一份已确认历史**
  （commit 相等且“已应用键值内容”的确定性 SHA-256 相同）；
- `checks`：有序的 `{name, pass, detail}` 判定列表；
- `verdict`：所有 check 全过为 `PASS`，否则 `FAIL`。

节点不使用物理 id，而用**按事件语义分配的角色名**：`L0` 首任领导者、
`L1` 首个更高任期的继任者、`NEWVOTER` 成员变更新增节点、`VICTIM` 长期离线
追赶节点、其余对等节点为 `P1..Pk`（它们彼此对称）。因此“哪个物理 id 恰好
赢得选举”不会影响报告内容。

### 可重复性边界与比对方法

- **可重复**：同一套场景与固定输入重跑，`facts` 与 `digest` 字节一致，
  `verdict` 与每个 `checks[i].pass` 一致，最终 `history_digest` 一致。
  `generated_at_ms` 与 `duration_ms` 允许不同，但它们**不参与** digest。
- **允许变化**：绝对 term（选期间可能有 split-vote 自增）、选举耗时、消息
  调度时刻。报告只记录**任期序号**（按领导纪元归一化后的 `term_offset`），
  不记录绝对 term，因此这些抖动不进入事实主体。
- **不可重复时的含义**：若同输入两次 `digest` 不同，说明出现了非确定性的
  历史分歧（本仓库曾借此发现并修复“相同边界的重复 InstallSnapshot 重放会
  回滚已应用条目”的缺陷，见 `TestDuplicateSnapshotMustNotRollback`），应作为
  真实问题排查，而不是掩盖。

固定输入复跑并比对（示例，用 jq/python 校验 digest）：

```
go run ./cmd/qlog -drill all -report run-1.json
go run ./cmd/qlog -drill all -report run-2.json
python3 -c 'import json; a=json.load(open("run-1.json")); b=json.load(open("run-2.json")); print([ (x["facts"]["scenario"], x["digest"]) for x in a ]); assert [x["digest"] for x in a]==[x["digest"] for x in b]; print("REPEATABLE")'
```

也可以用 `-drill-dir` 两次保留磁盘数据，直接比对各节点 `wal.log` /
`snapshot.bin` 与已应用内容。

对应自动化用例（`go test ./pkg/drill/`）：

- `TestScenario*Pass` / `TestScenario*SabotageFails`：四套场景成功判定，以及
  故障未真正生效时必须 `FAIL`；
- `TestRepeatabilitySameDigest`：同一场景同输入两次，digest、结论、最终历史
  摘要与逐项判定必须一致；
- `TestSabotageVerdictsFail`：所有反例必须以**带失败 check 的 FAIL** 收尾，
  而不是报错或假通过。

原有的正常写入、崩溃恢复、成员变更、压缩追赶路径仍由 `pkg/cluster` 下既有
测试守护（含 `TestDuplicateSnapshotMustNotRollback`），`go test ./...` 与
`go test -race ./...` 全部通过。

### 当前未覆盖的故障类型

演练刻意保持离线、确定、快速，因此下列故障**暂未**建模，报告结论不外推到
这些情形：

- 拜占庭/恶意节点、被篡改但 CRC 仍合法的应用语义（只覆盖 CRC/魔数/截断等
  本地损坏拒绝）；
- 真实网络丢包/乱序/延迟分布下的长时间不可用（传输层支持注入，但四套演练
  用固定分区/崩溃/固定延迟，不跑随机混沌）；
- 磁盘满、fsync 部分成功、整机掉电的物理时序（`SyncWrites=false`）；
- 跨地域时钟漂移、真实墙钟依赖、并发成员变更交错（实现上一次只允许一个
  变更）；
- 脑裂后双写长期并存、DNS/服务发现、外部客户端从历史陈旧副本读到脏数据等
  进程外语义；
- 大规模（多机房、上百节点、超大快照传输分批中断恢复）下的容量与性能问题。

## 配置开关

`cluster.Options.SyncWrites`（默认 `false`）控制本地存储是否真正 `fsync`。
模拟器默认关闭 fsync 以避免宿主/沙箱文件系统卡顿阻塞事件循环；磁盘格式的
崩溃安全性仍由损坏/截断/重启测试验证。需要演练真实 fsync 持久化时设为 `true`。
