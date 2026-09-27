package raft

import "github.com/h3yow/quorum-log-replication-go/pkg/raftproto"

// applyTo advances the applied index up to target, executing each committed
// entry exactly once on this node. It runs on the event loop right after the
// commit index moves (leader or follower), so every node applies the same
// prefix in the same order.
//
// applyTo may be entered re-entrantly: while a membership change is parked
// inside waitCommit it pumps events, and an inbound AppendEntries can apply
// further entries. State-machine application and waiter resolution are
// intrinsically idempotent (guarded by the applied watermark), and the
// configuration commit side-effects are driven by the separate
// configHookIdx watermark in runConfigHooks.
func (n *Node) applyTo(target uint64) {
	if target > n.commit {
		target = n.commit
	}
	for n.applied < target {
		idx := n.applied + 1
		ents, err := n.log.Entries(idx, idx+1)
		if err != nil || len(ents) != 1 {
			// The committed entry is not present locally yet (it can briefly
			// land in the snapshot/WAL boundary window while a snapshot is
			// being installed). STOP without advancing n.applied: the post
			// increment belongs only to a successfully applied entry,
			// otherwise the index would be skipped forever and the applied
			// watermark would over-report the real state. A later
			// AppendEntries/InstallSnapshot re-enters applyTo and resumes here.
			n.logger.warnf("applyTo: committed entry %d not yet available: %v", idx, err)
			return
		}
		e := ents[0]
		switch e.Type {
		case raftproto.EntryConfig:
			if _, derr := decodeConfig(e.Data); derr != nil {
				n.logger.warnf("applyTo: bad config at %d: %v", idx, derr)
				continue
			}
			n.commCfg = n.committedConfigAt(idx)
			n.finishConfigWaiters(idx, applyResult{})
		case raftproto.EntryNormal:
			res, aerr := n.app.Apply(e.Data)
			n.finishConfigWaiters(idx, applyResult{res: res, err: aerr})
		}
		n.applied++
	}
	if n.applied > 0 {
		// Persist the applied watermark so a crash/restart cannot replay
		// already-applied committed entries against the state machine.
		if err := n.log.SaveHardState(raftproto.HardState{
			Term: n.term, VoteFor: n.vote, Commit: n.commit, Apply: n.applied,
		}); err != nil {
			n.logger.warnf("persist applied: %v", err)
		}
	}
	n.runConfigHooks()
	n.pumpChange()
	n.checkPendingReads()
	n.maybeSnapshot()
	n.refreshObs()
}

// committedConfigAt derives the configuration that governed at idx by
// scanning committed config entries from the snapshot onward.
func (n *Node) committedConfigAt(idx uint64) raftproto.Config {
	cfg := n.snapMeta.Config
	start := n.snapMeta.Index + 1
	for i := start; i <= idx; i++ {
		ents, err := n.log.Entries(i, i+1)
		if err != nil {
			break
		}
		if len(ents) == 1 && ents[0].Type == raftproto.EntryConfig {
			if c, err := decodeConfig(ents[0].Data); err == nil {
				cfg = c
			}
		}
	}
	return cfg
}

// runConfigHooks performs each committed config entry's side-effect exactly
// once, regardless of how many nested applyTo calls observed it.
func (n *Node) runConfigHooks() {
	for idx := n.configHookIdx + 1; idx <= n.applied; idx++ {
		ents, err := n.log.Entries(idx, idx+1)
		if err != nil || len(ents) != 1 {
			return
		}
		if ents[0].Type != raftproto.EntryConfig {
			n.configHookIdx = idx
			continue
		}
		cfg, derr := decodeConfig(ents[0].Data)
		if derr != nil {
			n.configHookIdx = idx
			continue
		}
		n.onConfigCommitted(cfg, idx)
		n.configHookIdx = idx
	}
}

// finishConfigWaiters resolves every client proposal parked on idx.
func (n *Node) finishConfigWaiters(idx uint64, r applyResult) {
	ws := n.waiters[idx]
	if len(ws) == 0 {
		return
	}
	for _, w := range ws {
		w <- r
	}
	delete(n.waiters, idx)
}

// onConfigCommitted reacts when a config reaches the commit index:
//   - joint config committed: the leader immediately appends the final config
//     once (guarded by configHookIdx so it never happens twice);
//   - final config committed: the membership change is now a fact; progress
//     tables are reset and, if this node was removed, it steps down.
func (n *Node) onConfigCommitted(cfg raftproto.Config, idx uint64) {
	n.logger.statef("config committed at %d: voters=%v learners=%v joint=%v",
		idx, cfg.Voters, cfg.Learners, cfg.JointVoters)
	n.reinitProgress()
	if n.state == raftproto.StateLeader && len(cfg.JointVoters) > 0 {
		final := raftproto.Config{
			Voters:   cfg.Voters,
			Learners: withoutLearnersPromoted(cfg),
		}
		n.logger.statef("joint config stable; proposing final config voters=%v", final.Voters)
		if _, err := n.appendConfigEntry(final); err != nil {
			n.logger.warnf("append final config: %v", err)
		}
	}
	if len(cfg.JointVoters) == 0 {
		n.changeInProgress = false
		// A final config commit completes any matching in-flight change.
		if n.mchange != nil && sameConfig(n.mchange.final, cfg) {
			pc := n.mchange
			n.mchange = nil
			n.logger.statef("membership change finalized via commit: voters=%v", cfg.Voters)
			select {
			case pc.reply <- nil:
			default:
			}
		}
		if n.state == raftproto.StateLeader &&
			!isVoter(n.cfg.ID, cfg) && !isLearner(n.cfg.ID, cfg) {
			// The final config entry just committed, which already required a
			// quorum of BOTH the old and new voter sets (joint consensus).
			// Refresh the active config and step down so this removed node
			// never again campaigns or counts in future quorums.
			n.config = cfg
			n.commCfg = cfg
			n.reinitProgress()
			n.logger.statef("leader %s removed by committed config; stepping down and ceasing to campaign",
				n.cfg.ID)
			n.becomeFollower(n.term, "")
		}
	}
}

func withoutLearnersPromoted(cfg raftproto.Config) []string {
	out := make([]string, 0, len(cfg.Learners))
	for _, l := range cfg.Learners {
		if !contains(cfg.Voters, l) {
			out = append(out, l)
		}
	}
	return out
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
