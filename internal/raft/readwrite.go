package raft

import (
	"context"
)

// ReadIndex 线性一致读协议：
//  1. 只有当前领导者可服务；记录发起时任期；
//  2. 向集群发一轮心跳，收到当前配置多数确认（仍是本任期领导者）后，
//     取本地 commitIndex 作为读位置；
//  3. 调用方等待状态机 applied >= 读位置后再读本地数据。
//
// 心跳期间发现更高任期 => 请求失败，防止分区中的旧领导者响应读。
func (n *Node) ReadIndex(ctx context.Context) (Index, error) {
	ch := make(chan readReply, 1)
	req := &readReq{ctx: ctx, ch: ch, term: n.currentTerm}
	select {
	case n.stepCh <- event{kind: evReadIndex, read: req}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case r := <-ch:
		return r.index, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (n *Node) onReadIndex(r *readReq) {
	if n.stopped.Load() {
		r.ch <- readReply{err: ErrStopped}
		return
	}
	if n.role != RoleLeader {
		r.ch <- readReply{err: ErrNotLeader}
		return
	}
	if !n.leaseValid() {
		// 无法确认多数派仍承认本领导权：返回 ErrNoQuorum，
		// 调用方应重定向到新领导者，绝不以可能过期的数据响应读。
		r.ch <- readReply{err: ErrNoQuorum}
		return
	}
	r.term = n.currentTerm
	n.pendingReads = append(n.pendingReads, r)
	// 新一轮领导权确认：仅本节点自身先记票，待 follower 心跳响应后置位。
	for id, pr := range n.progress {
		pr.readAck = id == n.id
	}
	n.matchVoters = ConfigSet{n.id: {}}
	n.broadcastAppend()
}

// resolveReads 在一轮心跳得到多数确认后放行所有挂起读请求。
func (n *Node) resolveReads() {
	if len(n.pendingReads) == 0 {
		return
	}
	if n.role != RoleLeader {
		for _, r := range n.pendingReads {
			r.ch <- readReply{err: ErrNotLeader}
		}
		n.pendingReads = nil
		return
	}
	alive := ConfigSet{}
	for id, pr := range n.progress {
		if pr.readAck {
			alive[id] = struct{}{}
		}
	}
	if !n.committedConfig.HasQuorum(alive) {
		return
	}
	idx := n.commitIndex
	kept := n.pendingReads[:0]
	for _, r := range n.pendingReads {
		if r.term != n.currentTerm {
			r.ch <- readReply{err: ErrLeadershipLost}
			continue
		}
		select {
		case r.ch <- readReply{index: idx}:
		default:
		}
	}
	n.pendingReads = kept
}

// HandleInstallSnapshot 入站快照安装 RPC（同步进入事件循环）。
func (n *Node) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	var resp InstallSnapshotResponse
	n.runSync(func() { resp = n.handleInstallSnapshot(req) })
	return resp
}

func (n *Node) handleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	if req.Term < n.currentTerm {
		return InstallSnapshotResponse{Term: n.currentTerm}
	}
	if req.Term > n.currentTerm {
		n.stepDown(req.Term, "InstallSnapshot higher term")
	}
	n.leaderID = req.LeaderID
	n.resetElectionTimer()

	m := req.Meta
	// 快照落后于本地 commit：忽略（不能回滚已提交内容）。
	if m.LastIndex <= n.commitIndex {
		n.logger.Printf("ignore stale snapshot at %d <= commit %d", m.LastIndex, n.commitIndex)
		return InstallSnapshotResponse{Term: n.currentTerm}
	}

	// 1) 恢复状态机数据；2) 持久化快照（含截断旧日志）；
	// 3) 以快照点为新的 base 重建内存日志。
	if err := n.sm.Restore(req.Data); err != nil {
		n.logger.Printf("snapshot restore sm failed: %v", err)
		return InstallSnapshotResponse{Term: n.currentTerm}
	}
	if err := n.storage.InstallRemoteSnapshot(m, req.Data); err != nil {
		// 快照安装失败不修改磁盘既有权威状态；返回失败让领导者重试。
		n.logger.Printf("snapshot persist failed: %v", err)
		return InstallSnapshotResponse{Term: n.currentTerm}
	}
	n.hasSnap, n.snapMeta, n.snapData = true, m, req.Data
	n.baseIndex, n.baseTerm = m.LastIndex, m.LastTerm
	n.log = nil // 存储已把日志段重建为以快照点为 base
	n.config = m.Config.Clone()
	n.committedConfig = m.Config.Clone()
	n.nonVoters = ConfigSet{}
	if m.LastIndex > n.commitIndex {
		n.commitIndex = m.LastIndex
	}
	_ = n.saveHardState(HardState{Term: n.currentTerm, Vote: n.votedFor})
	n.logger.Printf("installed snapshot at index=%d term=%d, new base=%d config=%d voters",
		m.LastIndex, m.LastTerm, n.baseIndex, n.config.Len())
	return InstallSnapshotResponse{Term: n.currentTerm}
}

func (n *Node) onInstallResult(r *rpcResult[InstallSnapshotRequest, InstallSnapshotResponse]) {
	if n.role != RoleLeader || r.req.Term != n.currentTerm {
		return
	}
	pr := n.progress[r.to]
	if r.err != nil {
		n.logger.Printf("install snapshot to %s failed: %v", r.to, r.err)
		if pr != nil {
			pr.snapshotting = false
			n.inflightAppend[r.to] = false
		}
		return
	}
	if r.resp.Term > n.currentTerm {
		n.stepDown(r.resp.Term, "install snapshot response higher term")
		return
	}
	if pr != nil {
		pr.snapshotting = false
		n.inflightAppend[r.to] = false
		if r.req.Meta.LastIndex >= pr.MatchIndex {
			pr.MatchIndex = r.req.Meta.LastIndex
			pr.NextIndex = r.req.Meta.LastIndex + 1
			n.logger.Printf("follower %s caught up via snapshot to %d", r.to, r.req.Meta.LastIndex)
			n.advanceLeaderCommit()
			n.sendToFollower(r.to) // 继续发送快照之后的增量日志
		}
	}
}
