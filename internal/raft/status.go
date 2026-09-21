package raft

// Status 返回只读观测快照。纯内存读取并做深拷贝；观测入口不修改任何
// 状态，也不影响选举/复制行为。读取在唯一事件循环内执行，避免数据竞争。
func (n *Node) Status() NodeStatus {
	st := make(chan NodeStatus, 1)
	n.runSync(func() { st <- n.statusLocked() })
	return <-st
}

// statusLocked 构造观测快照；调用者必须运行在事件循环内。
func (n *Node) statusLocked() NodeStatus {
	s := NodeStatus{
		ID:           n.id,
		Role:         n.role,
		Term:         n.currentTerm,
		Leader:       n.leaderID,
		CommitIndex:  n.commitIndex,
		LastIndex:    n.lastIndex(),
		AppliedIndex: n.sm.AppliedIndex(),
		Config:       n.committedConfig.Clone(),
		LatestConfig: n.config.Clone(),
		Progress:     map[NodeID]PeerProgress{},
	}
	_, s.Voter = n.committedConfig.Voters[n.id]
	if s.Role == RoleLeader {
		s.Leader = n.id
		for id, pr := range n.progress {
			s.Progress[id] = PeerProgress{NextIndex: pr.NextIndex, MatchIndex: pr.MatchIndex}
		}
	}
	return s
}
