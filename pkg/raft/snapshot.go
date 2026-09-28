package raft

import (
	"context"

	"github.com/h3yow/quorum-log-replication-go/pkg/logstore"
	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// maybeSnapshot compacts the applied state once the log grows well beyond
// the last snapshot. Order is: serialize app state -> fsync snapshot file ->
// truncate only the covered WAL prefix. A crash at any point leaves either
// the old prefix intact or the new snapshot present, never a hole.
func (n *Node) maybeSnapshot() {
	threshold := n.cfg.SnapshotThreshold
	if threshold <= 0 {
		threshold = 64
	}
	if n.applied-n.snapMeta.Index < threshold {
		return
	}
	if n.applied <= n.snapMeta.Index {
		return
	}
	data, err := n.app.Snapshot()
	if err != nil {
		n.logger.warnf("snapshot: app serialize: %v", err)
		return
	}
	cfg := n.configAt(n.applied)
	term := n.termAt(n.applied)
	meta := raftproto.SnapshotMeta{Index: n.applied, Term: term, Config: cfg}
	if err := n.snaps.Save(meta, data); err != nil {
		n.logger.warnf("snapshot: save: %v", err)
		return
	}
	if err := n.log.Compact(n.applied); err != nil {
		n.logger.warnf("snapshot: compact: %v", err)
	}
	n.snapMeta = meta
	n.logger.statef("snapshot created at index=%d term=%d; log window now [%d..%d]",
		meta.Index, meta.Term, n.firstIndex(), n.lastIndex())
}

// configAt returns the config that governed index idx by scanning committed
// entries (used to embed membership state inside snapshots).
func (n *Node) configAt(idx uint64) raftproto.Config {
	cfg := n.snapMeta.Config
	start := n.firstIndex()
	if start > n.snapMeta.Index+1 {
		start = n.snapMeta.Index + 1
	}
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

// sendSnapshot streams the current snapshot to a follower whose log has been
// compacted away. We model the snapshot as one message.
func (n *Node) sendSnapshot(peer string) {
	if n.snapMeta.Index == 0 {
		// Nothing to snapshot: simply retry from index 1.
		if p := n.peers[peer]; p != nil && p.nextIndex > 1 {
			p.nextIndex = 1
		}
		n.sendAppend(peer, 0)
		return
	}
	_, data, err := n.snaps.Load()
	if err != nil {
		n.logger.warnf("sendSnapshot: load: %v", err)
		return
	}
	req := &raftproto.InstallSnapshotRequest{
		Term:              n.term,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: n.snapMeta.Index,
		LastIncludedTerm:  n.snapMeta.Term,
		Data:              data,
		Config:            n.snapMeta.Config,
		Done:              true,
	}
	n.logger.infof("sending snapshot index=%d to lagging peer %s", n.snapMeta.Index, peer)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
		defer cancel()
		resp, err := n.tr.SendInstallSnapshot(ctx, n.cfg.ID, peer, req)
		n.enqueue(context.Background(), snapRespEvent{peer: peer, req: req, resp: resp, err: err})
	}()
}

// handleInstallSnapshot validates and installs a leader snapshot.
func (n *Node) handleInstallSnapshot(from string, req *raftproto.InstallSnapshotRequest) *raftproto.InstallSnapshotResponse {
	resp := &raftproto.InstallSnapshotResponse{Term: n.term}
	if req.Term < n.term {
		return resp
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, req.LeaderID)
		resp.Term = n.term
	}
	n.lead = req.LeaderID
	n.electionElapsed = 0
	n.resetElectionTimeout()

	// Never let an older catch-up payload overwrite a position the follower
	// has already moved past. This guard must be against the COMMITTED frontier
	// (not merely snapMeta.Index): after installing a snapshot the follower
	// usually advances commit/applied by incremental AppendEntries well before
	// it compacts its own log, and during that window a duplicated or
	// retransmitted InstallSnapshot from a retry/leader change would otherwise
	// re-enter installSnapshot and Restore() the state machine back to an
	// older prefix while commit/applied kept reporting the newer position.
	if req.LastIncludedIndex <= n.commit {
		// The content up to this index is already part of this node's committed
		// prefix, so the request is stale-but-valid, not an error: ack it without
		// touching any state. Acking (rather than rejecting) lets the leader
		// advance this peer's nextIndex and continue streaming newer entries;
		// rejecting would strand it resending the same snapshot forever.
		n.logger.infof("ignore duplicate/stale snapshot %d (commit=%d applied=%d); acking without rollback",
			req.LastIncludedIndex, n.commit, n.applied)
		resp.Success = true
		return resp
	}
	if req.LastIncludedIndex < n.snapMeta.Index {
		n.logger.infof("reject stale snapshot %d (have %d)",
			req.LastIncludedIndex, n.snapMeta.Index)
		return resp
	}
	if err := n.installSnapshot(req.Data, req.LastIncludedIndex, req.LastIncludedTerm, req.Config); err != nil {
		n.logger.warnf("installSnapshot: %v", err)
		return resp
	}
	resp.Success = true
	if req.LastIncludedIndex > n.commit {
		n.commit = req.LastIncludedIndex
	}
	if err := n.persistHardState(); err != nil {
		n.logger.warnf("installSnapshot persist: %v", err)
	}
	n.applyTo(n.commit)
	return resp
}

// installSnapshot replaces local state with the snapshot contents. The
// durable operation is: save snapshot file, discard superseded WAL entries,
// restore application. Any failure leaves a distinguishable store error.
//
// It is idempotent and never rolls state backwards: a snapshot at or below the
// applied frontier is a no-op (the content is already present), so a
// duplicated or retransmitted request cannot Restore() the application to an
// older prefix while the applied watermark keeps reporting a newer position.
func (n *Node) installSnapshot(data []byte, index, term uint64, cfg raftproto.Config) error {
	if index <= n.applied {
		n.logger.infof("installSnapshot: skip snapshot at %d already applied at %d",
			index, n.applied)
		return nil
	}
	meta := raftproto.SnapshotMeta{Index: index, Term: term, Config: cfg}
	if err := n.snaps.Save(meta, data); err != nil && err != logstore.ErrSnapOutOfDate {
		return err
	}
	// Drop the whole old log window; it is fully covered by the snapshot.
	if err := n.log.Compact(index); err != nil {
		return err
	}
	if err := n.app.Restore(data); err != nil {
		return err
	}
	n.snapMeta = meta
	if n.applied < index {
		n.applied = index
	}
	n.config = cfg
	n.commCfg = cfg
	n.logger.statef("snapshot installed index=%d term=%d voters=%v",
		index, term, cfg.Voters)
	return nil
}

// onSnapResponse updates leader-side progress after a snapshot install.
func (n *Node) onSnapResponse(ev snapRespEvent) {
	if n.state != raftproto.StateLeader {
		return
	}
	if ev.err != nil {
		n.logger.infof("snapshot to %s failed: %v", ev.peer, ev.err)
		return
	}
	if ev.resp.Term > n.term {
		n.becomeFollower(ev.resp.Term, "")
		return
	}
	if !ev.resp.Success {
		return
	}
	p := n.peers[ev.peer]
	if p == nil {
		return
	}
	idx := ev.req.LastIncludedIndex
	if idx > p.matchIndex {
		p.matchIndex = idx
	}
	p.nextIndex = idx + 1
	n.logger.infof("peer %s installed snapshot up to %d", ev.peer, idx)
	n.advanceCommit()
	if p.nextIndex <= n.lastIndex() {
		n.sendAppend(ev.peer, 0)
	}
}
