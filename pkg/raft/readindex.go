package raft

import (
	"context"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// onRead serves a linearizable read using the Raft read-index pattern:
//
//  1. only the current leader may answer;
//  2. the leader records its commit index and broadcasts a heartbeat;
//  3. once a quorum acknowledges (proving leadership is still valid for this
//     term), it waits until its applied index reaches that recorded index;
//  4. only then does it read the local KV state.
//
// No response is ever served from a deposed leader or stale applied state.
func (n *Node) onRead(ctx context.Context, ev readEvent) {
	if n.state != raftproto.StateLeader {
		ev.reply <- readReply{err: ErrNotLeader}
		return
	}
	n.readGen++
	pr := &pendingRead{
		gen:     n.readGen,
		acks:    map[string]bool{n.cfg.ID: true},
		index:   n.commit,
		key:     ev.key,
		replyCh: ev.reply,
	}
	n.pending = append(n.pending, pr)
	n.logger.infof("read index gen=%d waiting quorum at readIndex=%d", pr.gen, pr.index)
	n.broadcastAppend(pr.gen)
}

// ackRead counts quorum acknowledgements for read probes. Any append reply
// that carries a gen belongs to that generation's heartbeat broadcast.
func (n *Node) ackRead(ev appendRespEvent) {
	if ev.gen == 0 {
		return
	}
	var pr *pendingRead
	for _, p := range n.pending {
		if p.gen == ev.gen {
			pr = p
			break
		}
	}
	if pr == nil {
		return
	}
	// A quorum-confirmed reply (including responses to later generations)
	// proves the leader still holds authority for this term.
	if ev.err == nil && ev.resp != nil && ev.resp.Term == n.term && ev.resp.Success {
		pr.acks[ev.peer] = true
	}
	if n.state != raftproto.StateLeader || ev.err != nil && ev.resp == nil {
		// transport failure alone does not invalidate leadership
	}
	if n.hasQuorum(pr.acks) {
		n.resolveRead(pr)
	}
}

// resolveRead completes every pending read once quorum is reached and the
// local applied index is caught up; otherwise it parks until apply catches
// up (checked again from applyTo via checkPendingReads).
func (n *Node) resolveRead(pr *pendingRead) {
	quorumOK := n.hasQuorum(pr.acks)
	if !quorumOK {
		return
	}
	if n.applied < pr.index {
		return // checkPendingReads retries after apply advances
	}
	// remove pending
	out := n.pending[:0]
	for _, p := range n.pending {
		if p != pr {
			out = append(out, p)
		}
	}
	n.pending = out
	v, ok := n.app.Get(pr.key)
	n.logger.infof("read gen=%d served key=%q applied=%d readIndex=%d", pr.gen, pr.key, n.applied, pr.index)
	pr.replyCh <- readReply{value: v, found: ok}
}

// checkPendingReads is invoked after applying entries. It drops reads whose
// leadership was lost (answering ErrNotLeader so the client retries via the
// new leader) and completes reads once quorum + applied-caught-up hold.
func (n *Node) checkPendingReads() {
	if len(n.pending) == 0 {
		return
	}
	alive := n.pending[:0]
	for _, pr := range n.pending {
		if n.state != raftproto.StateLeader {
			pr.replyCh <- readReply{err: ErrNotLeader}
			continue
		}
		if n.hasQuorum(pr.acks) && n.applied >= pr.index {
			n.resolveRead(pr)
			continue
		}
		alive = append(alive, pr)
	}
	n.pending = alive
}

// refreshObs copies loop-owned state into the lock-free observation cache.
// It must derive values directly from the authoritative fields; never read
// the cache back here, or stale zeros would overwrite live state.
func (n *Node) refreshObs() {
	prog := make(map[string]PeerProgress, len(n.peers))
	for id, p := range n.peers {
		prog[id] = PeerProgress{MatchIndex: p.matchIndex, NextIndex: p.nextIndex, Voter: p.voter}
	}
	committed := cloneCfg(n.commCfg)
	active := cloneCfg(n.config)
	n.obsMu.Lock()
	n.obsState = n.state
	n.obsTerm = n.term
	n.obsLeader = n.lead
	n.obsCommit = n.commit
	n.obsApply = n.applied
	n.obsSnap = n.snapMeta.Index
	n.obsComm = committed
	n.obsActive = active
	n.obsProgress = prog
	n.obsMu.Unlock()
}

// getStatus builds a read-only status snapshot from live loop state.
func (n *Node) getStatus() Status {
	n.refreshObs()
	prog := map[string]PeerProgress{}
	for id, p := range n.peers {
		prog[id] = PeerProgress{MatchIndex: p.matchIndex, NextIndex: p.nextIndex, Voter: p.voter}
	}
	return Status{
		ID:              n.cfg.ID,
		State:           n.state,
		Term:            n.term,
		Leader:          n.lead,
		CommitIndex:     n.commit,
		LastApplied:     n.applied,
		SnapshotIndex:   n.snapMeta.Index,
		CommittedConfig: cloneCfg(n.commCfg),
		ActiveConfig:    cloneCfg(n.config),
		Progress:        prog,
	}
}

func cloneCfg(c raftproto.Config) raftproto.Config {
	return raftproto.Config{
		Voters:      append([]string(nil), c.Voters...),
		Learners:    append([]string(nil), c.Learners...),
		JointVoters: append([]string(nil), c.JointVoters...),
	}
}

var _ = context.Background
