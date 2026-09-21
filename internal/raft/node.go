package raft

import (
	"context"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Node 单个 Raft 节点。所有内部状态仅由一个事件循环 goroutine 触碰，
// 外部通过方法与通道交互，避免数据竞争。
type Node struct {
	id     NodeID
	logger *log.Logger

	storage Storage
	sm      StateMachine
	tr      Transport
	opts    Options

	// ---- 持久状态（权威副本在 storage，内存镜像在此）----
	currentTerm Term
	votedFor    NodeID
	log         []LogEntry // 从 baseIndex+1 开始的内存日志镜像
	baseIndex   Index
	baseTerm    Term

	// ---- 快照（内存）----
	snapMeta SnapshotMeta
	snapData []byte
	hasSnap  bool

	// ---- 易失状态 ----
	role        Role
	leaderID    NodeID
	commitIndex Index
	votes       map[NodeID]bool // 候选人本轮得票
	progress    map[NodeID]*PeerProgress
	matchVoters ConfigSet // 本轮 ReadIndex 已确认的节点

	// 成员配置：
	//   config          —— 日志中最新配置（追加即切换，决定复制目标集合）；
	//   committedConfig —— 最近一条"已提交"配置（决定选举/提交多数与快照内容）。
	// 二者分离保证：未提交的联合配置不会被压缩进快照、不会决定投票多数；
	// 配置只有在提交后才成为既成事实。
	config          Config
	committedConfig Config
	// nonVoters 已通过 EntryAddNonVoter 提交的复制目标：接收快照与日志，
	// 但不参与投票与提交多数。成员变更的正式投票权仍由 Conf 条目授予。
	nonVoters ConfigSet

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int
	rng              *rand.Rand

	// 领导租约（滑动窗口）：leaseAckAt 记录每个选民最近一次心跳确认的
	// tick 时刻。只要"一个选举超时窗口"内仍有多数派确认，租约即有效。
	// 滑动窗口可容忍个别轮次 ack 延迟，避免无谓抖动；真正多数派不可达
	// 时，租约会在一个选举超时内失效并拒绝新提案。
	leaseTicks int
	leaseAckAt map[NodeID]int
	nowTick    int
	// pendingSelfRemove 领导者通过已提交配置移除了自己：待本轮提交的
	// 提案都成功通知后再降级，避免把已提交的配置变更误报为失败。
	pendingSelfRemove bool

	// ---- 提案 / 事件循环 ----
	stepCh       chan event
	proposals    map[Index]*Proposal // 领导者本地等待提交的提案
	pendingReads []*readReq

	wg      sync.WaitGroup
	stopped atomic.Bool

	// inflight RPC 去重（避免同一 follower 堆积重复请求）
	inflightAppend map[NodeID]bool
}

// NewNode 创建节点并从 Storage 恢复。若持久化损坏/截断，
// NewFileStorage 打开时已返回 ErrCorruptStorage / ErrTruncatedStorage，
// 节点不会被构造，拒绝以不确定状态启动。
//
// bootstrap 为全新集群的初始配置：首次启动时作为 index=1 的已提交
// 配置条目（连同 index=2 的 no-op）写入；对已存在数据的目录无效。
func NewNode(id NodeID, storage Storage, sm StateMachine, tr Transport, bootstrap Config, opts Options) (*Node, error) {
	if opts.ElectionMinTicks <= 0 {
		opts = DefaultOptions()
	}
	n := &Node{
		id:              id,
		logger:          log.New(os.Stderr, logPrefix(id), log.LstdFlags|log.Lmicroseconds),
		storage:         storage,
		sm:              sm,
		tr:              tr,
		opts:            opts,
		role:            RoleFollower,
		config:          bootstrap.Clone(),
		committedConfig: bootstrap.Clone(),
		stepCh:          make(chan event, 256),
		proposals:       map[Index]*Proposal{},
		progress:        map[NodeID]*PeerProgress{},
		votes:           map[NodeID]bool{},
		inflightAppend:  map[NodeID]bool{},
		rng:             rand.New(rand.NewSource(time.Now().UnixNano() + int64(hashID(id)))),
	}
	if err := n.restore(bootstrap); err != nil {
		return nil, err
	}
	n.resetElectionTimer()
	return n, nil
}

// logPrefix 便于在混合日志中区分节点。
func logPrefix(id NodeID) string { return "[raft " + string(id) + "] " }

func hashID(id NodeID) uint32 {
	var h uint32 = 2166136261
	for _, b := range []byte(id) {
		h ^= uint32(b)
		h *= 16777619
	}
	return h
}

// restore 从持久化状态恢复：HardState、快照（含状态机数据）、日志镜像，
// 并重放快照之后、已提交的日志，使对外已确认的结果在重启后不丢。
// 对空日志的全新节点，写入引导配置 index=1 并视为已提交。
func (n *Node) restore(bootstrap Config) error {
	hs, err := n.storage.InitialState()
	if err != nil {
		return err
	}
	n.currentTerm, n.votedFor = hs.Term, hs.Vote
	n.baseIndex = n.storage.BaseIndex()
	t, err := n.storage.TermAt(n.baseIndex)
	if err != nil {
		return err
	}
	n.baseTerm = t

	// 全新节点：种子化初始配置（index=1, term=0，已提交并应用）。
	if n.baseIndex == 0 && n.storage.LastIndex() == 0 && len(bootstrap.Voters) > 0 {
		c := bootstrap.Clone()
		seed := LogEntry{Index: 1, Term: 0, Kind: EntryConf, Conf: &c}
		if err := n.storage.Append([]LogEntry{seed}); err != nil {
			return err
		}
		n.config = c
		n.committedConfig = c
		n.commitIndex = 1
	}

	// 快照恢复（状态机数据 + 成员配置 + 提交位置下界）。
	if meta, r, ok, err := n.storage.Snapshot(); err != nil {
		return err
	} else if ok {
		data, rerr := io.ReadAll(r)
		_ = r.Close()
		if rerr != nil {
			return rerr
		}
		if err := n.sm.Restore(data); err != nil {
			return err
		}
		n.hasSnap, n.snapMeta, n.snapData = true, meta, data
		n.config = meta.Config.Clone()
		n.committedConfig = meta.Config.Clone()
		n.commitIndex = meta.LastIndex
		n.logger.Printf("restore snapshot at index=%d term=%d, applied=%d config=%d voters",
			meta.LastIndex, meta.LastTerm, n.sm.AppliedIndex(), n.config.Len())
	}

	// 日志镜像。
	last := n.storage.LastIndex()
	if last > n.baseIndex {
		ents, err := n.storage.Entries(n.baseIndex+1, last+1)
		if err != nil {
			return err
		}
		n.log = ents
		// 无论是否已提交，日志中最新的配置条目决定"复制目标集合"
		// （它一定已由前领导者追加并复制到本节点）。重启后必须重建，
		// 否则会退化成引导时的单成员配置。
		for _, e := range ents {
			if e.Kind == EntryConf && e.Conf != nil {
				n.config = e.Conf.Clone()
			}
			if e.Kind == EntryAddNonVoter {
				if n.nonVoters == nil {
					n.nonVoters = ConfigSet{}
				}
				n.nonVoters[e.nonVoterTarget()] = struct{}{}
			}
		}
	}
	// commitIndex 也必须持久化（随 HardState）：无快照重启时
	// 已提交位置不能退化为 0。快照已给出更高位置时以快照为准。
	if hs.CommitIndex > n.commitIndex {
		n.commitIndex = hs.CommitIndex
	}

	// 重放快照之后已提交的日志到状态机。
	if n.commitIndex > n.sm.AppliedIndex() {
		for i := n.sm.AppliedIndex() + 1; i <= n.commitIndex; i++ {
			e, err := n.entry(i)
			if err != nil {
				return err
			}
			if _, err := n.applyEntry(e); err != nil {
				return err
			}
		}
	}
	n.logger.Printf("restored term=%d vote=%q base=%d last=%d commit=%d applied=%d",
		n.currentTerm, n.votedFor, n.baseIndex, n.lastIndex(), n.commitIndex, n.sm.AppliedIndex())
	return nil
}

// Start 启动事件循环。
func (n *Node) Start() {
	n.wg.Add(1)
	go n.run()
}

// Stop 停止节点；停止后所有方法返回 ErrStopped。
func (n *Node) Stop() error {
	select {
	case n.stepCh <- event{kind: evStop}:
	default:
	}
	n.wg.Wait()
	return n.storage.Close()
}

// Tick 驱动逻辑时钟（由上层按固定间隔调用）。
func (n *Node) Tick() {
	select {
	case n.stepCh <- event{kind: evTick}:
	default:
		// 事件循环繁忙时丢弃一个 tick 只会放慢定时器，不影响安全性。
	}
}

func (n *Node) run() {
	defer n.wg.Done()
	for ev := range n.stepCh {
		switch ev.kind {
		case evTick:
			n.onTick()
		case evPropose:
			n.onPropose(ev.propose)
		case evReadIndex:
			n.onReadIndex(ev.read)
		case evVoteResult:
			n.onVoteResult(ev.vote)
		case evAppendResult:
			n.onAppendResult(ev.append)
		case evInstallResult:
			n.onInstallResult(ev.install)
		case evSync:
			if ev.syncFn != nil {
				ev.syncFn()
			}
		case evStop:
			n.onStop()
			return
		}
	}
}

func (n *Node) onStop() {
	if n.stopped.Load() {
		return
	}
	n.stopped.Store(true)
	for _, p := range n.proposals {
		closeWith(p, ErrStopped)
	}
	n.proposals = map[Index]*Proposal{}
	for _, r := range n.pendingReads {
		r.ch <- readReply{err: ErrStopped}
	}
	n.pendingReads = nil
	n.logger.Printf("node stopped at term=%d role=%s", n.currentTerm, n.role)
}

// ===== 日志镜像辅助 =====

func (n *Node) lastIndex() Index {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Index
	}
	return n.baseIndex
}

func (n *Node) lastTerm() Term {
	if len(n.log) > 0 {
		return n.log[len(n.log)-1].Term
	}
	return n.baseTerm
}

func (n *Node) logOffset(i Index) int { return int(i - n.baseIndex - 1) }

func (n *Node) hasLogIndex(i Index) bool {
	return i > n.baseIndex && i <= n.lastIndex()
}

func (n *Node) termAt(i Index) Term {
	if i == 0 {
		return 0
	}
	if i == n.baseIndex {
		return n.baseTerm
	}
	if n.hasLogIndex(i) {
		return n.log[n.logOffset(i)].Term
	}
	return 0 // 未知下标按"无该日志"处理，由调用方判定
}

func (n *Node) entry(i Index) (LogEntry, error) {
	if i <= n.baseIndex {
		return LogEntry{}, ErrGap
	}
	if !n.hasLogIndex(i) {
		return LogEntry{}, ErrGap
	}
	return n.log[n.logOffset(i)], nil
}

func (n *Node) resetElectionTimer() {
	n.randomizeElectionTimeout()
	n.electionElapsed = 0
}

// randomizeElectionTimeout 只重掷超时长度，不触碰已累计时间。
// 范围取 [1.5x, 3x] 心跳基准的宽抖动：所有节点共享同一 tick 源，
// 宽范围可显著降低多节点同拍超时导致的平票。
func (n *Node) randomizeElectionTimeout() {
	base := n.opts.ElectionMinTicks
	if base <= 0 {
		base = 12
	}
	lo := base
	hi := n.opts.ElectionMaxTicks
	if hi <= lo {
		hi = lo * 2
	}
	n.electionTimeout = lo + n.rng.Intn(hi-lo+1)
}

// saveHardState 持久化任期/投票，同时带上当前 commitIndex。
func (n *Node) saveHardState(hs HardState) error {
	hs.CommitIndex = n.commitIndex
	return n.storage.SaveHardState(hs)
}

// ===== 任期与角色转换（全部带日志说明判定依据）=====

// stepDown 收到更高任期：退回跟随者，清投票与领导状态。
func (n *Node) stepDown(term Term, reason string) {
	if term > n.currentTerm {
		n.logger.Printf("term %d -> %d, become follower (%s)", n.currentTerm, term, reason)
		n.currentTerm = term
		n.votedFor = ""
		if err := n.saveHardState(HardState{Term: term, Vote: ""}); err != nil {
			n.logger.Printf("FATAL persist term: %v", err)
		}
	}
	if n.role != RoleFollower {
		n.logger.Printf("role %s -> follower at term=%d (%s)", n.role, n.currentTerm, reason)
	}
	n.role = RoleFollower
	n.leaderID = ""
	n.votes = map[NodeID]bool{}
	n.leaseAckAt = nil
	// 卸任时所有挂起提案都不能再由本节点确认：通知调用方领导权易主，
	// 由其重试到新领导者；条目本身可能在新任期被提交，重试凭借
	// (ClientID, Seq) 幂等不会重复生效。
	for idx, p := range n.proposals {
		closeWith(p, ErrLeadershipLost)
		delete(n.proposals, idx)
	}
	for _, r := range n.pendingReads {
		select {
		case r.ch <- readReply{err: ErrLeadershipLost}:
		default:
		}
	}
	n.pendingReads = nil
	n.resetElectionTimer()
}

func (n *Node) becomeCandidate() {
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.id
	n.votes = map[NodeID]bool{n.id: true}
	n.leaderID = ""
	if err := n.saveHardState(HardState{Term: n.currentTerm, Vote: n.id}); err != nil {
		n.logger.Printf("FATAL persist vote: %v", err)
	}
	// 开启新一轮选举：清零已流逝时间，超时长度由 startElection 随机化。
	// （触发时刻的那一个 tick 即新任期的第 0 拍。）
	n.electionElapsed = 0
	n.logger.Printf("become candidate at term=%d, lastIndex=%d lastTerm=%d", n.currentTerm, n.lastIndex(), n.lastTerm())
	n.startElection()
}

func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = n.id
	last := n.lastIndex()
	n.progress = map[NodeID]*PeerProgress{}
	// 上一任期（可能是跟随者）残留的在途标记必须清空，否则新领导者
	// 会误判"已有 RPC 在途"而永远不向某些 follower 发送心跳。
	n.inflightAppend = map[NodeID]bool{}
	// 初始化 nextIndex=last+1（乐观），冲突响应再快速回退；
	// matchIndex 对自己初始化为 last。
	for id := range n.currentReplicationSet() { // 复制目标=最新配置并集（含未提交新成员）
		ni := last + 1
		mi := Index(0)
		if id == n.id {
			ni, mi = last+1, last
		}
		n.progress[id] = &PeerProgress{NextIndex: ni, MatchIndex: mi}
	}
	n.matchVoters = ConfigSet{n.id: {}}
	// 租约时钟从当前任期 0 开始：自己当然始终确认自己；授票者授票时
	// 都已重置各自选举定时器，在最小选举超时内不会另选，故同样以
	// tick 0 播种，给首批心跳往返留出初始窗口。
	n.nowTick = 0
	n.leaseTicks = 0
	n.leaseAckAt = map[NodeID]int{n.id: 0}
	for id, granted := range n.votes {
		if granted && id != n.id {
			n.leaseAckAt[id] = 0
		}
	}
	n.logger.Printf("become leader at term=%d, lastIndex=%d committed-voters=%d replication=%d",
		n.currentTerm, last, n.committedConfig.Len(), len(n.progress))
	// 上任立即追加一条本任期 no-op：它被多数派复制后，
	// 领导者才能安全提交之前任期遗留的条目（Raft Figure 8 约束）。
	noop := LogEntry{Index: last + 1, Term: n.currentTerm, Kind: EntryNormal}
	if err := n.appendEntriesLocal([]LogEntry{noop}); err != nil {
		n.logger.Printf("leader no-op persist failed: %v", err)
	}
	n.broadcastAppend()
}

// currentReplicationSet 复制目标集合（最新配置并集 + 已预告的非投票成员，
// 可能包含尚未提交的新投票成员，供其提前追赶）。
func (n *Node) currentReplicationSet() ConfigSet {
	out := n.config.replicationSet()
	for id := range n.nonVoters {
		out[id] = struct{}{}
	}
	return out
}

// committedReplicationSet 已提交配置的投票并集（联合期为 C_old∪C_new）。
func (n *Node) committedReplicationSet() ConfigSet {
	return n.committedConfig.replicationSet()
}

// replicationSet 配置条目所覆盖的全部节点：联合期取 C_old∪C_new。
func (c Config) replicationSet() ConfigSet {
	out := ConfigSet{}
	for id := range c.Voters {
		out[id] = struct{}{}
	}
	if c.Joint != nil {
		for id := range c.Joint {
			out[id] = struct{}{}
		}
	}
	return out
}

func (n *Node) onTick() {
	if n.role == RoleLeader {
		n.nowTick++
		// 领导者自己始终确认自己：每拍刷新自我确认时刻，
		// 否则自己会先于 follower 掉出租约窗口。
		if n.leaseAckAt == nil {
			n.leaseAckAt = map[NodeID]int{}
		}
		n.leaseAckAt[n.id] = n.nowTick
		n.heartbeatElapsed++
		if !n.leaseValid() {
			// 仅在从"有效"变为"失效"时打印一次判定依据。
			if n.leaseTicks == 0 {
				n.leaseTicks = 1 // 标记已记录过期
				n.logger.Printf("leadership lease expired: no quorum ack within %d ticks (term=%d)",
					n.leaseWindowTicks(), n.currentTerm)
			}
		} else {
			n.leaseTicks = 0
		}
		if n.heartbeatElapsed >= n.opts.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		n.maybeCompact()
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		// 非投票成员（刚启动、尚未通过成员变更加入配置的新节点）
		// 不允许发起选举，否则其落后日志会干扰既有集群；它只被动
		// 接收快照与日志，等待配置条目提交后成为投票成员。
		if _, voter := n.committedConfig.Voters[n.id]; !voter {
			n.resetElectionTimer()
			return
		}
		// 注意：不在这里清零——becomeCandidate 只重新随机化超时长度，
		// 已累计的时间自然开启新一轮计时；否则平票后候选人会失去
		// 再次超时重试的机会。
		n.becomeCandidate()
	}
}

// ===== 选举 =====

func (n *Node) startElection() {
	// 每轮选举独立随机超时：平票后各候选人在不同时长后重试，快速打破僵局。
	n.randomizeElectionTimeout()
	n.votes[n.id] = true
	req := RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	// 联合期间需向已提交联合配置 C_old∪C_new 全部拉票，双多数同时满足才胜选。
	for id := range n.committedReplicationSet() {
		if id == n.id {
			continue
		}
		go func(to NodeID) {
			ctx, cancel := context.WithTimeout(context.Background(), n.opts.RPCTimeout)
			defer cancel()
			resp, err := n.tr.SendRequestVote(ctx, to, req)
			n.queue(event{kind: evVoteResult, vote: newVoteResult(to, req, resp, err)})
		}(id)
	}
	n.maybeWinElection()
}

// HandleRequestVote 入站投票 RPC。通过同步往返事件循环执行，
// 保证与其它状态变更串行，无需给内部状态加锁。
func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	var resp RequestVoteResponse
	n.runSync(func() { resp = n.handleRequestVote(req) })
	return resp
}

func (n *Node) handleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	if req.Term < n.currentTerm {
		n.logger.Printf("reject vote from %s: stale term %d < %d", req.CandidateID, req.Term, n.currentTerm)
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}
	if req.Term > n.currentTerm {
		n.stepDown(req.Term, "RequestVote higher term")
	}
	canVote := n.votedFor == "" || n.votedFor == req.CandidateID
	logUpToDate := req.LastLogTerm > n.lastTerm() ||
		(req.LastLogTerm == n.lastTerm() && req.LastLogIndex >= n.lastIndex())
	if !canVote {
		n.logger.Printf("reject vote from %s: already voted for %q at term=%d",
			req.CandidateID, n.votedFor, n.currentTerm)
	}
	if !logUpToDate {
		n.logger.Printf("reject vote from %s: candidate log (%d,%d) not at least (%d,%d)",
			req.CandidateID, req.LastLogIndex, req.LastLogTerm, n.lastIndex(), n.lastTerm())
	}
	granted := canVote && logUpToDate
	if granted {
		n.votedFor = req.CandidateID
		if err := n.saveHardState(HardState{Term: n.currentTerm, Vote: n.votedFor}); err != nil {
			n.logger.Printf("FATAL persist vote: %v", err)
		}
		n.resetElectionTimer()
		n.logger.Printf("grant vote to %s at term=%d", req.CandidateID, n.currentTerm)
	}
	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: granted}
}

func (n *Node) onVoteResult(r *rpcResult[RequestVoteRequest, RequestVoteResponse]) {
	if n.role != RoleCandidate || r.req.Term != n.currentTerm {
		return // 旧选举结果
	}
	if r.err != nil {
		n.logger.Printf("vote rpc to %s failed: %v (no quorum progress)", r.to, r.err)
		return
	}
	if r.resp.Term > n.currentTerm {
		n.stepDown(r.resp.Term, "vote response higher term")
		return
	}
	if r.resp.VoteGranted {
		n.votes[r.to] = true
		n.maybeWinElection()
	}
}

// maybeWinElection 按当前配置多数派判定当选（联合期要求 C_old、C_new 双多数）。
func (n *Node) maybeWinElection() {
	if n.role != RoleCandidate {
		return
	}
	alive := ConfigSet{}
	for id, ok := range n.votes {
		if ok {
			alive[id] = struct{}{}
		}
	}
	if n.committedConfig.HasQuorum(alive) {
		n.becomeLeader()
	}
}

// runSync 让网络回调线程同步等待事件循环处理，避免对内部状态加锁。
// 事件循环不会反向等待回调线程，因此不会死锁。
func (n *Node) runSync(f func()) {
	done := make(chan struct{})
	wrapped := func() {
		defer close(done)
		f()
	}
	select {
	case n.stepCh <- event{kind: evSync, syncFn: wrapped}:
		<-done
	case <-time.After(5 * time.Second):
		// 事件循环已停止：回调不应悬挂，返回零值由调用方处理。
	}
}
