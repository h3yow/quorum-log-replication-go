package raft

import (
	"fmt"
	"math/rand/v2"

	"github.com/h3yow/quorum-log-replication-go/pkg/logstore"
	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// loadState rehydrates all node state from disk at startup.
//
// Distinguishable failure rules:
//   - CRC/framing errors come back as logstore.ErrCorruptLog and the node is
//     not constructed (it will not serve uncertain state).
//   - A torn trailing write comes back as logstore.ErrTruncatedLog; the
//     caller decides whether that tail was already committed. Here we fail
//     closed as well, surfacing the reason explicitly.
func (n *Node) loadState() error {
	hs, err := n.log.HardState()
	if err != nil {
		return err
	}
	n.term = hs.Term
	n.vote = hs.VoteFor
	n.commit = hs.Commit
	n.applied = hs.Apply

	// Snapshot window.
	if meta, data, err := n.snaps.Load(); err != nil {
		return err
	} else if meta != nil {
		n.snapMeta = *meta
		n.applied = max64(n.applied, meta.Index)
		n.commit = max64(n.commit, meta.Index)
		if err := n.app.Restore(data); err != nil {
			return err
		}
		n.logger.statef("restored snapshot index=%d term=%d", meta.Index, meta.Term)
	}

	// Rehydrate the application state for committed entries that were already
	// applied but are not covered by a snapshot. Without a snapshot the state
	// machine starts empty on restart, so replay the committed prefix from the
	// durable WAL in order into that fresh state machine.
	//
	// Upper bound is min(persisted-applied, persisted-commit). Every entry in
	// this prefix is part of the committed, majority-confirmed history and is
	// therefore safe to apply exactly once. Empty EntryNormal records are
	// legitimate leader no-ops that may appear anywhere, so they are skipped
	// (not treated as a boundary). A divergent, uncommitted suffix is never in
	// this range: commit only advances after a quorum match, and a follower's
	// AppendEntries handler truncates any conflicting suffix before adopting
	// the leader's commit index.
	replayTo := n.applied
	if n.commit < replayTo {
		replayTo = n.commit
	}
	if replayTo > n.snapMeta.Index {
		lo := n.snapMeta.Index + 1
		ents, err := n.log.Entries(lo, replayTo+1)
		if err != nil {
			return fmt.Errorf("reload committed entries [%d,%d]: %w", lo, replayTo, err)
		}
		if uint64(len(ents)) != replayTo-lo+1 {
			return fmt.Errorf("%w: committed prefix %d..%d not fully present on disk (got %d)",
				logstore.ErrCompacted, lo, replayTo, len(ents))
		}
		dataEntries := 0
		for _, e := range ents {
			if e.Type == raftproto.EntryNormal && len(e.Data) > 0 {
				if _, aerr := n.app.Apply(e.Data); aerr != nil {
					return fmt.Errorf("replay entry %d: %w", e.Index, aerr)
				}
				dataEntries++
			}
		}
		// The whole contiguous prefix was reconstructed; the applied
		// watermark is the prefix end, not a count of data records (config
		// and no-op entries occupy indices too).
		n.applied = replayTo
		n.logger.statef("replayed %d entries (%d data) from WAL [%d..%d]",
			len(ents), dataEntries, lo, replayTo)
	}

	// Empty store: bootstrap the initial configuration as entry #1 so the
	// whole cluster starts from one committed-by-agreement config.
	if n.log.LastIndex() == 0 && n.snapMeta.Index == 0 && n.cfg.InitialConfig != nil {
		e := raftproto.Entry{
			Term:  1,
			Index: 1,
			Type:  raftproto.EntryConfig,
			Data:  encodeConfig(*n.cfg.InitialConfig),
		}
		if err := n.log.Append([]raftproto.Entry{e}); err != nil {
			return err
		}
		hs := raftproto.HardState{Term: 1, Commit: 1}
		if err := n.log.SaveHardState(hs); err != nil {
			return err
		}
		n.term = 1
		n.commit = 1
		n.applied = 1
		n.logger.statef("bootstrapped initial config at index 1: voters=%v", n.cfg.InitialConfig.Voters)
	}

	n.rebuildActiveConfig()
	n.rebuildCommittedConfig()
	n.resetElectionTimeout()

	// Begin as follower. If the persisted vote was for ourselves we still
	// wait for either heartbeat or an election timeout; no split-brain.
	n.state = raftproto.StateFollower
	n.lead = ""
	// Entries already applied before the crash must not re-run config commit
	// side-effects; only genuinely new applied entries drive the hook.
	n.configHookIdx = n.applied
	n.applyTo(n.commit)
	n.getStatus()
	n.logger.statef("loadState done: term=%d commit=%d applied=%d voters=%v",
		n.term, n.commit, n.applied, n.config.Voters)
	return nil
}

func (n *Node) resetElectionTimeout() {
	lo, hi := n.cfg.ElectionTickMin, n.cfg.ElectionTickMax
	if lo <= 0 {
		lo = 6
	}
	if hi <= lo {
		hi = lo * 2
	}
	n.electionTimeout = lo + rand.IntN(hi-lo+1)
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
