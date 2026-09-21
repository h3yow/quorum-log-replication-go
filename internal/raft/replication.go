package raft

import (
	"context"
	"fmt"
)

func closeWith(p *Proposal, err error) {
	select {
	case <-p.resultCh:
		return // 已有结果
	default:
		p.resultCh <- err
	}
}

// queue 把事件投递给事件循环；节点停止后静默丢弃（结果以 ErrStopped 兜底）。
func (n *Node) queue(ev event) {
	if n.stopped.Load() {
		return
	}
	select {
	case n.stepCh <- ev:
	default:
		// stepCh 有缓冲且由循环持续排空，正常不会满；满时退化为阻塞短时等待。
		n.stepCh <- ev
	}
}

// Propose 写入一条普通命令。非领导者立即返回 ErrNotLeader；
// 重复的 (ClientID, Seq) 幂等：跟随日志复制/去重逻辑，只生效一次。
func (n *Node) Propose(ctx context.Context, clientID string, seq uint64, data []byte) error {
	ch := make(chan error, 1)
	p := &Proposal{
		entry:    LogEntry{Kind: EntryNormal, ClientID: clientID, Seq: seq, Data: append([]byte(nil), data...)},
		resultCh: ch,
	}
	select {
	case n.stepCh <- event{kind: evPropose, propose: p}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		// 调用方超时不代表写入一定失败：提案可能仍在提交路上。
		// 后续以相同 (ClientID,Seq) 重试时由状态机去重，保证不重复生效。
		return ctx.Err()
	}
}

func (n *Node) onPropose(p *Proposal) {
	if n.stopped.Load() {
		closeWith(p, ErrStopped)
		return
	}
	if n.role != RoleLeader {
		closeWith(p, ErrNotLeader)
		return
	}
	if !n.leaseValid() {
		// 多数派当前不可达：本条写入不可能被确认，立即拒绝，
		// 调用方可重试到新领导者；不会产生新的提交。
		closeWith(p, ErrNoQuorum)
		return
	}
	e := p.entry
	e.Index = n.lastIndex() + 1
	e.Term = n.currentTerm
	if err := n.appendEntriesLocal([]LogEntry{e}); err != nil {
		closeWith(p, err)
		return
	}
	n.proposals[e.Index] = p
	n.logger.Printf("propose index=%d term=%d client=%s seq=%d queued", e.Index, e.Term, e.ClientID, e.Seq)
	n.broadcastAppend()
}

// appendEntriesLocal 持久化并加入内存日志，冲突时按 Raft 截断本地尾段。
func (n *Node) appendEntriesLocal(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0]
	if first.Index <= n.baseIndex {
		// 不应发生：调用方应先走 InstallSnapshot 路径。
		return fmt.Errorf("%w: cannot append %d <= base %d", ErrGap, first.Index, n.baseIndex)
	}
	if cut := n.logOffset(first.Index); cut >= 0 && cut < len(n.log) {
		if n.log[cut].Term != first.Term {
			// 截断冲突后缀（仅未提交部分允许被覆盖；Raft 安全性保证
			// 已提交条目不会以不同任期出现在此下标）。
			n.log = append([]LogEntry(nil), n.log[:cut]...)
		}
	}
	if err := n.storage.Append(entries); err != nil {
		return err
	}
	n.log = nil
	last := n.storage.LastIndex()
	if last > n.baseIndex {
		ents, err := n.storage.Entries(n.baseIndex+1, last+1)
		if err != nil {
			return err
		}
		n.log = ents
	}
	// 配置条目在追加后立即影响复制目标集合（但提交/投票语义仍按联合规则）。
	for _, e := range entries {
		if e.Kind == EntryConf && e.Conf != nil {
			n.config = e.Conf.Clone()
			n.ensureProgressForConfig()
			n.logger.Printf("config appended at index=%d: voters=%d joint=%d",
				e.Index, e.Conf.Len(), e.Conf.JointLen())
		}
	}
	return nil
}

func (n *Node) ensureProgressForConfig() {
	if n.role != RoleLeader {
		return
	}
	last := n.lastIndex()
	for id := range n.currentReplicationSet() {
		if _, ok := n.progress[id]; !ok {
			n.progress[id] = &PeerProgress{NextIndex: last + 1, MatchIndex: 0}
		}
	}
}

// HandleAppendEntries 入站复制 RPC（同步进入事件循环）。
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	var resp AppendEntriesResponse
	n.runSync(func() { resp = n.handleAppendEntries(req) })
	return resp
}

func (n *Node) handleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false, ConflictIndex: n.lastIndex() + 1}
	}
	if req.Term > n.currentTerm || n.role != RoleFollower {
		n.stepDown(req.Term, "AppendEntries from leader "+string(req.LeaderID))
	}
	n.leaderID = req.LeaderID
	n.resetElectionTimer()

	// 任期一致性检查：PrevLogIndex 处的任期必须匹配。
	if req.PrevLogIndex < n.baseIndex {
		// 我们的快照已覆盖该点：要求领导者从快照点之后补发。
		return AppendEntriesResponse{Term: n.currentTerm, Success: false,
			ConflictIndex: n.baseIndex + 1, ConflictTerm: n.baseTerm}
	}
	if req.PrevLogIndex > n.lastIndex() || n.termAt(req.PrevLogIndex) != req.PrevLogTerm {
		// 快速冲突回退：返回本节点在冲突任期的最早下标。
		ci, ct := n.conflictHint(req.PrevLogIndex)
		n.logger.Printf("append reject: prev=(%d,%d) local last=%d hint=(%d,%d)",
			req.PrevLogIndex, req.PrevLogTerm, n.lastIndex(), ci, ct)
		return AppendEntriesResponse{Term: n.currentTerm, Success: false, ConflictIndex: ci, ConflictTerm: ct}
	}

	if err := n.appendEntriesLocal(req.Entries); err != nil {
		n.logger.Printf("append persist failed: %v", err)
		return AppendEntriesResponse{Term: n.currentTerm, Success: false, ConflictIndex: n.baseIndex + 1}
	}

	// 提交位置取领导者已告知与本地日志末尾的较小值，绝不回退 commitIndex。
	if req.LeaderCommit > n.commitIndex {
		newCommit := req.LeaderCommit
		if newCommit > n.lastIndex() {
			newCommit = n.lastIndex()
		}
		if err := n.advanceCommit(newCommit); err != nil {
			n.logger.Printf("apply up to %d failed: %v", newCommit, err)
		}
	}
	return AppendEntriesResponse{Term: n.currentTerm, Success: true, ConflictIndex: n.lastIndex() + 1}
}

// conflictHint 计算让领导者快速回退 nextIndex 的提示下标。
func (n *Node) conflictHint(prev Index) (Index, Term) {
	if prev > n.lastIndex() {
		return n.lastIndex() + 1, 0
	}
	conflictTerm := n.termAt(prev)
	i := prev
	for i > n.baseIndex && n.termAt(i-1) == conflictTerm {
		i--
	}
	return i, conflictTerm
}

// ===== 领导者复制 =====

func (n *Node) broadcastAppend() {
	if n.role != RoleLeader {
		return
	}
	for id := range n.currentReplicationSet() {
		if id == n.id {
			continue
		}
		if n.inflightAppend[id] {
			continue // 上一轮 RPC 未返回，不堆积重复请求
		}
		n.sendToFollower(id)
	}
	n.advanceLeaderCommit()
}

func (n *Node) sendToFollower(id NodeID) {
	pr := n.progress[id]
	if pr == nil {
		pr = &PeerProgress{NextIndex: n.lastIndex() + 1}
		n.progress[id] = pr
	}
	if pr.NextIndex <= n.baseIndex {
		// 跟随者落后到已被快照覆盖：改发 InstallSnapshot 追赶，避免日志空洞。
		if err := n.sendInstallSnapshot(id); err != nil {
			n.logger.Printf("snapshot dispatch to %s failed: %v", id, err)
		}
		return
	}
	// 同一 follower 上一轮 RPC 未完成时不堆积新请求；
	// 在途标记在此处统一置位，由成功/失败/快照路径统一释放。
	if n.inflightAppend[id] {
		return
	}
	n.inflightAppend[id] = true
	prevIndex := pr.NextIndex - 1
	prevTerm := n.termAt(prevIndex)
	var entries []LogEntry
	if pr.NextIndex <= n.lastIndex() {
		ents, err := n.storage.Entries(pr.NextIndex, n.lastIndex()+1)
		if err != nil {
			n.logger.Printf("read entries for %s: %v", id, err)
			return
		}
		entries = ents
	}
	req := AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.inflightAppend[id] = true
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.opts.RPCTimeout)
		defer cancel()
		resp, err := n.tr.SendAppendEntries(ctx, id, req)
		n.queue(event{kind: evAppendResult, append: newAppendResult(id, req, resp, err)})
	}()
}

func (n *Node) onAppendResult(r *rpcResult[AppendEntriesRequest, AppendEntriesResponse]) {
	// 无论成功失败都释放在途标记：失败路径靠下一轮心跳重发，
	// 若不释放则该 follower 在后续分区恢复等场景下可能再也收不到重试。
	if _, ok := n.progress[r.to]; ok {
		n.inflightAppend[r.to] = false
	}
	if r.err == nil && n.role == RoleLeader && r.resp.Success && r.resp.Term == n.currentTerm {
		// 即便该响应来自选举期间发出的旧任期心跳（req.Term 较低），
		// 只要它确认的是"当前任期"的领导权（resp.Term 匹配），
		// 就可用于租约续期：这正是当选后首批多数派确认。
		n.renewLease(r.to)
	}
	if n.role != RoleLeader || r.req.Term != n.currentTerm {
		return
	}
	if r.err != nil {
		n.logger.Printf("append to %s failed: %v", r.to, r.err)
		return // 超时/分区：下一轮心跳重试，不改变任何已提交状态
	}
	if r.resp.Term > n.currentTerm {
		n.stepDown(r.resp.Term, "append response higher term")
		return
	}
	pr := n.progress[r.to]
	if pr == nil {
		return
	}
	if r.resp.Success {
		newMatch := r.req.PrevLogIndex + Index(len(r.req.Entries))
		if newMatch > pr.MatchIndex {
			pr.MatchIndex = newMatch
		}
		if pr.NextIndex <= newMatch {
			pr.NextIndex = newMatch + 1
		}
		pr.readAck = true // 本任期心跳到达一个承认该领导权的 follower
		n.advanceLeaderCommit()
		n.resolveReads()
		// 成功后立即补发可能在 RPC 在途期间新产生的条目。
		if pr.NextIndex <= n.lastIndex() {
			n.sendToFollower(r.to)
		}
		return
	}
	// 快速冲突回退（Raft 论文 §5.3 优化）：
	//   - follower 报出其本地冲突任期：领导者把 nextIndex 跳到
	//     "自己日志中该任期的最后位置 +1"；
	//   - 自己没有该任期（follower 落后超过快照点，或 ConflictTerm=0）：
	//     直接采用 follower 给出的 ConflictIndex；
	//   - follower 也无该下标（follower 日志更短）：用其 lastIndex+1。
	hint := r.resp.ConflictIndex
	if r.resp.ConflictTerm > 0 {
		var lastOfTerm Index
		found := false
		for i := n.lastIndex(); i > n.baseIndex; i-- {
			if n.termAt(i) == r.resp.ConflictTerm {
				lastOfTerm, found = i, true
				break
			}
		}
		if found {
			hint = lastOfTerm + 1
		}
		// found=false 时保留 follower 的 ConflictIndex。
	}
	if hint < 1 {
		hint = 1
	}
	if pr.NextIndex > 0 && hint >= pr.NextIndex {
		hint = pr.NextIndex - 1
	}
	pr.NextIndex = hint
	// follower 的日志缺口已落在本节点快照覆盖范围内
	// （它需要的 PrevLogIndex 是已被压缩的条目）：再发 AppendEntries
	// 永远无法匹配，必须改发 InstallSnapshot 让其直接跳到快照点。
	if hint <= n.baseIndex+1 {
		n.logger.Printf("follower %s behind snapshot base=%d (hint=%d), installing snapshot", r.to, n.baseIndex, hint)
		n.sendInstallSnapshot(r.to)
		return
	}
	n.logger.Printf("follower %s conflict, backoff nextIndex=%d", r.to, hint)
	n.sendToFollower(r.to)
}

// leaseWindowTicks 租约确认有效窗口。取若干个心跳周期且严格小于跟随者
// 最小选举超时：既给心跳往返留出充足余量（容忍个别轮次延迟），又保证
// 旧领导者在任何新领导者可能当选前就已停止服务新提案。
func (n *Node) leaseWindowTicks() int {
	hb := n.opts.HeartbeatTicks
	if hb <= 0 {
		hb = 2
	}
	// 窗口 = 选举超时下限 - 1 个心跳周期；同时至少容纳 3 个心跳周期，
	// 避免初始心跳 RTT 未完成就误判过期。
	w := n.opts.ElectionMinTicks - hb
	if w < 3*hb {
		w = 3 * hb
	}
	if w >= n.opts.ElectionMinTicks {
		w = n.opts.ElectionMinTicks - 1
	}
	return w
}

// leaseValidWithin 在最近 window 个 tick 内是否仍有已提交配置多数派确认。
func (n *Node) leaseValidWithin(window int) bool {
	if n.role != RoleLeader {
		return false
	}
	alive := ConfigSet{}
	for id, at := range n.leaseAckAt {
		if n.nowTick-at <= window {
			alive[id] = struct{}{}
		}
	}
	return n.committedConfig.HasQuorum(alive)
}

// renewLease 记录 follower 的心跳确认时刻（滑动窗口，无需逐轮清零）。
func (n *Node) renewLease(from NodeID) {
	if n.role != RoleLeader {
		return
	}
	if n.leaseAckAt == nil {
		n.leaseAckAt = map[NodeID]int{}
	}
	n.leaseAckAt[from] = n.nowTick
}

// leaseValid 当前租约是否有效。
func (n *Node) leaseValid() bool { return n.leaseValidWithin(n.leaseWindowTicks()) }

// advanceLeaderCommit 多数派复制完成后推进提交点。
// 规则（Raft Figure 8 约束）：领导者只直接提交"本任期"条目；
// 更早任期的条目借由本任期 no-op/配置条目一起被间接提交，
// 从机制上避免提交已知可能被覆盖的旧任期日志。
func (n *Node) advanceLeaderCommit() {
	if n.role != RoleLeader {
		return
	}
	for i := n.lastIndex(); i > n.commitIndex; i-- {
		if n.termAt(i) != n.currentTerm {
			continue // 不直接计算旧任期条目的多数
		}
		// 提交法定人数按"日志中最新配置"（n.config）计算，而不是已提交配置：
		// 联合条目一旦被追加（尚未提交），其提交就必须同时满足
		// C_old 与 C_new 双多数；落到 C_new 的条目只需新配置多数。
		// 这保证配置未稳定前绝不可能只靠单方多数产生提交。
		oldCount, newCount := 0, 0
		oldSize, newSize := n.config.Len(), n.config.JointLen()
		for id, pr := range n.progress {
			repl := pr.MatchIndex >= i
			if id == n.id {
				repl = n.lastIndex() >= i
			}
			if _, inOld := n.config.Voters[id]; inOld && repl {
				oldCount++
			}
			if n.config.Joint != nil {
				if _, inNew := n.config.Joint[id]; inNew && repl {
					newCount++
				}
			}
		}
		okOld := oldCount >= quorum(oldSize)
		okNew := n.config.Joint == nil || newCount >= quorum(newSize)
		if okOld && okNew {
			if err := n.advanceCommit(i); err != nil {
				n.logger.Printf("commit advance to %d failed: %v", i, err)
			}
			break
		}
	}
}

// advanceCommit 把 commitIndex 推进到 i 并按序应用全部新提交条目，
// commitIndex 单调不减，绝不回滚已提交内容。
func (n *Node) advanceCommit(i Index) error {
	if i <= n.commitIndex {
		return nil
	}
	if i > n.lastIndex() {
		i = n.lastIndex()
	}
	for idx := n.commitIndex + 1; idx <= i; idx++ {
		e, err := n.entry(idx)
		if err != nil {
			return err
		}
		// 应用前先取出提案句柄：应用配置条目可能导致领导者自我移除而
		// 降级，届时必须以"成功"而不是"领导权丢失"通知该提案，
		// 因为配置条目确已提交。
		p := n.proposals[idx]
		delete(n.proposals, idx)
		dup, err := n.applyEntry(e)
		if err != nil {
			return err
		}
		if p != nil {
			closeWith(p, nil)
		}
		_ = dup
		if n.pendingSelfRemove {
			n.pendingSelfRemove = false
			n.stepDown(n.currentTerm, "leader removed by committed config (after ack)")
		}
	}
	n.commitIndex = i
	// 提交位置必须在应用后立即落盘：崩溃重启时据此恢复已确认结果，
	// 不依赖快照是否恰好覆盖到该点。
	if err := n.saveHardState(HardState{Term: n.currentTerm, Vote: n.votedFor}); err != nil {
		n.logger.Printf("FATAL persist commit index %d: %v", i, err)
	}
	n.logger.Printf("commit advanced to %d (term=%d)", i, n.termAt(i))
	return nil
}

// applyEntry 确定性应用一条已提交日志：普通条目进状态机，配置条目
// 在此刻才把成员变更当作既成事实（提交点之后生效）。
func (n *Node) applyEntry(e LogEntry) (bool, error) {
	switch e.Kind {
	case EntryAddNonVoter:
		id := e.nonVoterTarget()
		if n.nonVoters == nil {
			n.nonVoters = ConfigSet{}
		}
		if _, voter := n.committedConfig.Voters[id]; !voter {
			n.nonVoters[id] = struct{}{}
			if n.role == RoleLeader {
				n.ensureProgressForConfig()
			}
			n.logger.Printf("non-voter %s added to replication set at index=%d", id, e.Index)
		}
		_, err := n.sm.Apply(e)
		return false, err
	case EntryConf:
		// 配置在此刻（提交并应用）才成为既成事实：更新已提交配置，
		// 此后选举/提交多数与快照内容都以它为准。
		n.committedConfig = e.Conf.Clone()
		// 联合期间：把出现在 C_new 的待加入节点保留为复制目标；
		// 落到普通配置后：新选民已在配置中，从非投票集合清除已成为选民者。
		if e.Conf.Joint == nil {
			for id := range n.nonVoters {
				if _, voter := e.Conf.Voters[id]; voter {
					delete(n.nonVoters, id)
				}
			}
		}
		n.logger.Printf("config committed at index=%d: voters=%d joint=%d",
			e.Index, e.Conf.Len(), e.Conf.JointLen())
		if e.Conf.Joint == nil && n.role == RoleLeader && n.id != "" {
			// 落到 C_new：自己已不在新配置。标记延迟降级，
			// 待当前提交批次（含本配置提案的成功通知）处理完再交出领导权。
			if _, ok := e.Conf.Voters[n.id]; !ok {
				n.logger.Printf("leader %s removed by committed config; will step down after ack", n.id)
				n.pendingSelfRemove = true
			}
		}
		_, err := n.sm.Apply(e)
		return false, err
	default:
		return n.sm.Apply(e)
	}
}
