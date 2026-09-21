package raft

import (
	"context"
	"time"
)

// Membership 提供动态成员变更的提案入口（Raft 联合共识两阶段）：
//
//	C_old --EntryConf(联合: Voters=C_old, Joint=C_new, 已提交)--> C_old,new
//	      --EntryConf(普通: Voters=C_new, 已提交)--> C_new
//
// 新节点先通过 EntryAddNonVoter 进入复制目标完成追赶（无投票权），
// 再进入联合配置。任一阶段未提交前，新配置都不被当作既成事实；
// 联合期间所有提交与选举都要求 C_old、C_new 双多数。
type Membership struct {
	node *Node
}

// NewMembership 包装一个节点的成员变更能力。
func NewMembership(n *Node) *Membership { return &Membership{node: n} }

// AddVoter 加入一个投票成员（需先通过 AddNonVoter 完成追赶）。
// AddVoter 加入一个投票成员（通常集群层已先用 AddNonVoter 完成追赶）。
func (m *Membership) AddVoter(ctx context.Context, id NodeID) error {
	var next ConfigSet
	var already bool
	m.node.runSync(func() {
		next = m.node.committedVoters()
		if _, isVoter := next[id]; isVoter {
			already = true
			return
		}
		next[id] = struct{}{}
	})
	if already {
		return nil // 幂等：已是投票成员
	}
	return m.node.ChangeConfig(ctx, next)
}

// RemoveVoter 移除一个投票成员。
func (m *Membership) RemoveVoter(ctx context.Context, id NodeID) error {
	var next ConfigSet
	var missing bool
	m.node.runSync(func() {
		next = m.node.committedVoters()
		if _, ok := next[id]; !ok {
			missing = true
			return
		}
		delete(next, id)
	})
	if missing {
		return ErrInvalidConfig
	}
	return m.node.ChangeConfig(ctx, next)
}

// ApplyConfig 以任意目标配置发起一次联合变更（供配置更新）。
func (m *Membership) ApplyConfig(ctx context.Context, target ConfigSet) error {
	return m.node.ChangeConfig(ctx, target)
}

// committedVoters 返回最近一次已提交的普通配置成员集合；
// 联合期返回 C_old（联合条目的 Voters 字段即旧配置）。
func (n *Node) committedVoters() ConfigSet {
	return cloneSet(n.committedConfig.Voters)
}

// ChangeConfig 在事件循环中执行两阶段成员变更，并等待两条配置条目
// 都提交后才返回；期间出现领导权易主则返回错误，由上层重试整个变更。
// 重试是幂等的：若目标联合配置已在日志中（上次尝试已追加/提交），
// 则直接续接后续阶段，不会重复制造配置条目。
func (n *Node) ChangeConfig(ctx context.Context, target ConfigSet) error {
	// 在事件循环内读取配置状态，判断两阶段各自应"新提案/等待提交/已完成"。
	type state struct {
		old            ConfigSet
		jointAppended  bool
		jointCommitted bool
		jointIndex     Index
		finalAppended  bool
		finalCommitted bool
		err            error
	}
	var st state
	n.runSync(func() {
		st.old = n.committedVoters()
		if setEqual(st.old, target) {
			return
		}
		if err := ValidateChange(st.old, target); err != nil {
			st.err = err
			return
		}
		// 联合配置是否已在日志中（追加/提交）。
		if n.config.Joint != nil &&
			setEqual(n.config.Voters, st.old) && setEqual(n.config.Joint, target) {
			st.jointAppended = true
			st.jointIndex = n.lastIndex()
			st.jointCommitted = n.commitIndex >= st.jointIndex
		}
		// 最终配置是否已在日志中（追加/提交）。
		if n.config.Joint == nil && setEqual(n.config.Voters, target) {
			st.finalAppended = true
			st.finalCommitted = n.commitIndex >= n.lastIndex()
		}
	})
	if st.err != nil {
		return st.err
	}
	if setEqual(st.old, target) {
		return nil
	}
	joint := Config{Voters: cloneSet(st.old), Joint: cloneSet(target)}
	final := Config{Voters: cloneSet(target)}

	// 阶段一：确保联合配置"已提交"（不是仅追加）。
	if !st.jointAppended {
		if err := n.proposeConf(ctx, joint); err != nil {
			return err
		}
		n.logger.Printf("joint config committed: old=%d new=%d", len(joint.Voters), len(joint.Joint))
	} else if !st.jointCommitted {
		// 续接：联合条目已追加但尚未提交，必须等待其提交——
		// 绝不能在双多数确认前追加最终配置。
		n.logger.Printf("wait for in-flight joint config to commit")
		if err := n.waitConfCommitted(ctx, joint); err != nil {
			return err
		}
	}

	// 阶段二：落到 C_new。
	if !st.finalAppended {
		if err := n.proposeConf(ctx, final); err != nil {
			return err
		}
	} else if !st.finalCommitted {
		if err := n.waitConfCommitted(ctx, final); err != nil {
			return err
		}
	}
	n.logger.Printf("membership change settled: voters=%d", len(final.Voters))
	return nil
}

// waitConfCommitted 等待与给定配置相同的最新配置条目提交（事件循环轮询）。
func (n *Node) waitConfCommitted(ctx context.Context, want Config) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var done bool
		var notLeader bool
		n.runSync(func() {
			if n.role != RoleLeader {
				notLeader = true
				return
			}
			if EqualConfig(n.config, want) && n.commitIndex >= n.lastIndex() {
				done = true
			}
		})
		if done {
			return nil
		}
		if notLeader {
			return ErrLeadershipLost
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// enqueueEntry 在事件循环内做领导权/租约校验、构造并持久化条目、
// 登记等待句柄。所有状态读取都发生在循环内，避免数据竞争。
func (n *Node) enqueueEntry(kind func(nextIndex Index, term Term) (LogEntry, error)) (Index, <-chan error, error) {
	ch := make(chan error, 1)
	var idx Index
	var preErr error
	n.runSync(func() {
		if n.stopped.Load() {
			preErr = ErrStopped
			return
		}
		if n.role != RoleLeader {
			preErr = ErrNotLeader
			return
		}
		if !n.leaseValid() {
			preErr = ErrNoQuorum
			return
		}
		e, err := kind(n.lastIndex()+1, n.currentTerm)
		if err != nil {
			preErr = err
			return
		}
		if err := n.appendEntriesLocal([]LogEntry{e}); err != nil {
			preErr = err
			return
		}
		idx = e.Index
		n.proposals[idx] = &Proposal{entry: e, resultCh: ch}
		n.broadcastAppend()
	})
	if preErr != nil {
		return 0, nil, preErr
	}
	return idx, ch, nil
}

// AddNonVoter 追加并提交一条复制集预告条目：提交后（applyEntry 中）
// 领导者立即开始向 id 推送快照/日志，其无投票权。重复调用幂等。
func (n *Node) AddNonVoter(ctx context.Context, id NodeID) error {
	ch := make(chan error, 1)
	n.runSync(func() {
		if n.stopped.Load() {
			ch <- ErrStopped
			return
		}
		if n.role != RoleLeader {
			ch <- ErrNotLeader
			return
		}
		if _, isVoter := n.committedConfig.Voters[id]; isVoter {
			ch <- nil // 已是投票成员，无需预告
			return
		}
		if _, queued := n.nonVoters[id]; queued {
			// 已预告但尚未成为选民：确保领导者开始/继续复制即可。
			n.ensureProgressForConfig()
			n.broadcastAppend()
			ch <- nil
			return
		}
		e := LogEntry{
			Index: n.lastIndex() + 1,
			Term:  n.currentTerm,
			Kind:  EntryAddNonVoter,
			Data:  []byte(id),
		}
		if err := n.appendEntriesLocal([]LogEntry{e}); err != nil {
			ch <- err
			return
		}
		n.proposals[e.Index] = &Proposal{entry: e, resultCh: ch}
		n.broadcastAppend()
	})
	return waitChannel(ctx, ch)
}

// proposeConf 追加一条配置条目并等待其提交/应用。
func (n *Node) proposeConf(ctx context.Context, c Config) error {
	// 并发成员变更互斥检查放在循环内，避免与状态竞争。
	joint := c.Joint != nil
	_, ch, err := n.enqueueEntry(func(next Index, term Term) (LogEntry, error) {
		if joint && n.config.Joint != nil {
			// 日志中最新配置仍是联合态（可能已追加但尚未提交）：
			// 配置未稳定前不得开启下一次变更。
			return LogEntry{}, ErrJointChangeInProgress
		}
		conf := c.Clone()
		return LogEntry{Index: next, Term: term, Kind: EntryConf, Conf: &conf}, nil
	})
	if err != nil {
		return err
	}
	return waitChannel(ctx, ch)
}

func waitChannel(ctx context.Context, ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
