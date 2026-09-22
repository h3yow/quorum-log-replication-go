package raft

import (
	"context"
	"sort"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

const maxAppendEntriesBytes = 1 << 20

// onPropose handles a client write: only the current leader may append.
// The entry is fsync'd locally before replication, so a crash after
// acknowledging can never lose it.
func (n *Node) onPropose(ev proposeEvent) {
	if n.state != raftproto.StateLeader {
		ev.reply <- applyResult{err: n.notLeaderErr()}
		return
	}
	idx := n.lastIndex() + 1
	e := raftproto.Entry{
		Term:  n.term,
		Index: idx,
		Type:  raftproto.EntryNormal,
		Data:  append([]byte(nil), ev.cmd...),
	}
	if err := n.log.Append([]raftproto.Entry{e}); err != nil {
		ev.reply <- applyResult{err: err}
		return
	}
	// leader's own progress
	if p := n.peers[n.cfg.ID]; p != nil {
		p.matchIndex, p.nextIndex = idx, idx+1
	}
	n.waiters[idx] = append(n.waiters[idx], ev.reply)
	n.logger.infof("proposal appended at index %d term %d", idx, n.term)
	n.advanceCommit()
	n.broadcastAppend(0)
}

func (n *Node) notLeaderErr() error {
	return ErrNotLeader
}

// appendNoop writes the leader's term-start empty entry.
func (n *Node) appendNoop() {
	idx := n.lastIndex() + 1
	e := raftproto.Entry{Term: n.term, Index: idx, Type: raftproto.EntryNormal}
	if err := n.log.Append([]raftproto.Entry{e}); err != nil {
		n.logger.warnf("appendNoop: %v", err)
		return
	}
	if p := n.peers[n.cfg.ID]; p != nil {
		p.matchIndex, p.nextIndex = idx, idx+1
	}
}

// broadcastAppend sends one AppendEntries (heartbeat or data) to every peer.
// gen>0 tags the broadcast as a read-index probe; responses ack the gen.
func (n *Node) broadcastAppend(gen uint64) {
	if n.state != raftproto.StateLeader {
		return
	}
	ids := make([]string, 0, len(n.peers))
	for id := range n.peers {
		if id != n.cfg.ID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		n.sendAppend(id, gen)
	}
}

// sendAppend builds one AppendEntries; it falls back to InstallSnapshot when
// the peer's nextIndex is covered by the local snapshot (so no log hole is
// ever exposed).
func (n *Node) sendAppend(peer string, gen uint64) {
	p := n.peers[peer]
	if p == nil {
		return
	}
	if p.nextIndex <= n.snapMeta.Index {
		n.sendSnapshot(peer)
		return
	}
	prevIndex := p.nextIndex - 1
	prevTerm := n.termAt(prevIndex)
	ents := n.entriesAfter(prevIndex, maxAppendEntriesBytes)
	req := &raftproto.AppendEntriesRequest{
		Term:         n.term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		LeaderCommit: n.commit,
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
		defer cancel()
		resp, err := n.tr.SendAppendEntries(ctx, n.cfg.ID, peer, req)
		n.enqueue(context.Background(), appendRespEvent{
			peer: peer, req: req, resp: resp, err: err, gen: gen,
		})
	}()
}

// handleAppendEntries validates an inbound AppendEntries as follower/candidate.
func (n *Node) handleAppendEntries(from string, req *raftproto.AppendEntriesRequest) *raftproto.AppendEntriesResponse {
	resp := &raftproto.AppendEntriesResponse{Term: n.term}
	if req.Term < n.term {
		n.logger.infof("reject AE from %s: stale term %d < %d", from, req.Term, n.term)
		return resp
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, req.LeaderID)
		resp.Term = n.term
	}
	// Current-term leader: acknowledge leadership even as candidate.
	if n.state == raftproto.StateCandidate {
		n.becomeFollower(req.Term, req.LeaderID)
	}
	n.lead = req.LeaderID
	n.electionElapsed = 0
	n.resetElectionTimeout()

	// Log consistency check at prevLogIndex/prevLogTerm.
	if req.PrevLogIndex > n.lastIndex() {
		resp.ConflictIndex = n.lastIndex() + 1
		resp.ConflictTerm = 0
		n.logger.infof("reject AE: missing prev index %d (have %d)", req.PrevLogIndex, n.lastIndex())
		return resp
	}
	if req.PrevLogIndex > 0 && req.PrevLogIndex >= n.firstIndex() {
		if have := n.termAt(req.PrevLogIndex); have != req.PrevLogTerm {
			resp.ConflictTerm = have
			// backtrack to the first index of that conflicting term.
			ci := req.PrevLogIndex
			for ci > n.snapMeta.Index && n.termAt(ci-1) == have {
				ci--
			}
			resp.ConflictIndex = ci
			n.logger.infof("reject AE: term conflict at %d have=%d want=%d -> hint %d",
				req.PrevLogIndex, have, req.PrevLogTerm, ci)
			return resp
		}
	}
	if req.PrevLogIndex > 0 && req.PrevLogIndex < n.firstIndex() {
		// Leader's history predates our snapshot window: only acceptable if
		// our snapshot sits inside its prefix (index<=prev+snapshot).
		if n.snapMeta.Index > req.PrevLogIndex+uint64(len(req.Entries)) {
			n.logger.infof("reject AE: stale leader prefix prev=%d snap=%d",
				req.PrevLogIndex, n.snapMeta.Index)
			return resp
		}
	}

	// Append / repair local log.
	if len(req.Entries) > 0 {
		newLast, err := n.appendEntries(req.Entries)
		if err != nil {
			n.logger.warnf("appendEntries failed: %v", err)
			return resp
		}
		_ = newLast
		// Entries may contain configs: recompute the active configuration.
		n.rebuildActiveConfig()
	}
	resp.Success = true
	resp.ConflictIndex = n.lastIndex() + 1

	// Advance commit index to min(leaderCommit, our last).
	if req.LeaderCommit > n.commit {
		target := req.LeaderCommit
		if target > n.lastIndex() {
			target = n.lastIndex()
		}
		if target > n.commit {
			n.logger.statef("commit advances %d -> %d (leader %s)", n.commit, target, req.LeaderID)
			n.commit = target
			if err := n.persistHardState(); err != nil {
				n.logger.warnf("persist commit: %v", err)
			}
			n.rebuildCommittedConfig()
		}
	}
	n.applyTo(n.commit)
	return resp
}

// onAppendResponse handles one AppendEntries reply on the leader loop.
func (n *Node) onAppendResponse(ev appendRespEvent) {
	if n.state != raftproto.StateLeader || ev.req == nil {
		n.ackRead(ev)
		return
	}
	if ev.err != nil {
		n.logger.infof("AE to %s failed: %v", ev.peer, ev.err)
		n.ackRead(ev)
		return
	}
	r := ev.resp
	if r.Term > n.term {
		n.becomeFollower(r.Term, "")
		return
	}
	if r.Term < n.term {
		return
	}
	p := n.peers[ev.peer]
	if p == nil {
		n.ackRead(ev)
		return
	}
	if r.Success {
		matched := ev.req.PrevLogIndex + uint64(len(ev.req.Entries))
		if matched > p.matchIndex {
			p.matchIndex = matched
		}
		p.nextIndex = matched + 1
		n.logger.infof("peer %s matched index %d", ev.peer, matched)
		n.advanceCommit()
		n.pumpChange()
		n.ackRead(ev)
		// send any newer entries that accumulated while this was in flight
		if p.nextIndex <= n.lastIndex() {
			n.sendAppend(ev.peer, 0)
		}
		return
	}
	// Rejection: backtrack using the conflict hint.
	if r.ConflictTerm > 0 {
		// search backward for the last index of that term, else jump to hint
		ci := uint64(0)
		for i := n.lastIndex(); i > n.snapMeta.Index; i-- {
			if n.termAt(i) == r.ConflictTerm {
				ci = i + 1
				break
			}
		}
		if ci == 0 {
			ci = r.ConflictIndex
		}
		if ci < p.nextIndex {
			p.nextIndex = ci
		}
	} else if r.ConflictIndex > 0 {
		if r.ConflictIndex < p.nextIndex {
			p.nextIndex = r.ConflictIndex
		}
	} else {
		if p.nextIndex > 1 {
			p.nextIndex--
		}
	}
	n.logger.infof("peer %s rejected; backtrack nextIndex -> %d", ev.peer, p.nextIndex)
	n.sendAppend(ev.peer, 0)
}

// advanceCommit recomputes the committable index from match positions. Under
// joint consensus, quorums of BOTH voter sets must hold the entry; this is
// what prevents a new majority committing before the joint entry itself.
func (n *Node) advanceCommit() {
	if n.state != raftproto.StateLeader {
		return
	}
	// Count replication against the active voter sets. The leader is a member
	// of n.config.Voters and must be counted exactly once; prepending it again
	// would double-count and let a lone leader fabricate a majority.
	voters := n.config.Voters
	joint := n.config.JointVoters

	for cand := n.commit + 1; cand <= n.lastIndex(); cand++ {
		if n.termAt(cand) != n.term {
			continue // Raft: only current-term entries are advanced directly
		}
		oldHold := 0
		for _, id := range voters {
			if id == n.cfg.ID {
				oldHold++ // leader holds its own appended entry
				continue
			}
			if p := n.peers[id]; p != nil && p.matchIndex >= cand {
				oldHold++
			}
		}
		if oldHold < quorum(len(voters)) {
			break
		}
		if len(joint) > 0 {
			jh := 0
			for _, id := range joint {
				if id == n.cfg.ID {
					jh++
					continue
				}
				if p := n.peers[id]; p != nil && p.matchIndex >= cand {
					jh++
				}
			}
			if jh < quorum(len(joint)) {
				break
			}
		}
		n.logger.statef("leader commit advances %d -> %d (term %d quorum confirmed)",
			n.commit, cand, n.term)
		n.commit = cand
	}
	if err := n.persistHardState(); err != nil {
		n.logger.warnf("persist commit: %v", err)
	}
	n.applyTo(n.commit)
}
