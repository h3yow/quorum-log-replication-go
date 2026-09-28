package raft

import (
	"fmt"

	"github.com/h3yow/quorum-log-replication-go/pkg/logstore"
	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// Thin, on-loop wrappers over the durable store plus the snapshot window.
// The store returns ErrCompacted for indexes covered by the snapshot.

func (n *Node) lastIndex() uint64 {
	if i := n.log.LastIndex(); i > 0 {
		return i
	}
	return n.snapMeta.Index
}

func (n *Node) firstIndex() uint64 {
	if n.snapMeta.Index > 0 {
		return n.snapMeta.Index + 1
	}
	return n.log.FirstIndex()
}

func (n *Node) termAt(index uint64) uint64 {
	t, err := n.log.Term(index)
	if err != nil {
		// snapshot boundary term
		if index == n.snapMeta.Index {
			return n.snapMeta.Term
		}
		if index < n.snapMeta.Index {
			n.logger.warnf("termAt(%d) below snapshot %d", index, n.snapMeta.Index)
			return 0
		}
	}
	return t
}

func (n *Node) lastTerm() uint64 { return n.termAt(n.lastIndex()) }

// entriesAfter returns entries with index > since (bounded for transport).
func (n *Node) entriesAfter(since uint64, maxBytes int) []raftproto.Entry {
	lo := since + 1
	hi := n.lastIndex() + 1
	if lo >= hi {
		return nil
	}
	ents, err := n.log.Entries(lo, hi)
	if err != nil {
		if err == logstore.ErrCompacted {
			return nil // caller must fall back to InstallSnapshot
		}
		n.logger.warnf("entriesAfter(%d): %v", since, err)
		return nil
	}
	if maxBytes <= 0 {
		return ents
	}
	total := 0
	for i := range ents {
		total += len(ents[i].Data)
		if total > maxBytes {
			return ents[:i]
		}
	}
	return ents
}

// appendEntries truncates any conflicting suffix and appends; returns the
// new last index. Entries already present (same index+term) are kept.
func (n *Node) appendEntries(ents []raftproto.Entry) (uint64, error) {
	if len(ents) == 0 {
		return n.lastIndex(), nil
	}
	first := ents[0].Index
	if first <= n.snapMeta.Index {
		// discard entries already snapshotted; keep only the fresh tail
		off := n.snapMeta.Index + 1 - first
		if off >= uint64(len(ents)) {
			return n.lastIndex(), nil
		}
		ents = ents[off:]
		first = ents[0].Index
	}
	// Find the conflict point against the local durable log.
	localLast := n.log.LastIndex()
	switch {
	case first > localLast+1:
		// gap: caller violated contiguity; reject upstream
		return 0, logstore.ErrBadRange
	case first <= localLast:
		// inspect overlap; if a term differs we truncate from there
		cut := uint64(0)
		for i := range ents {
			idx := ents[i].Index
			if idx > localLast {
				break
			}
			lt, err := n.log.Term(idx)
			if err != nil {
				break
			}
			if lt != ents[i].Term {
				cut = idx
				break
			}
		}
		if cut > 0 {
			// A conflicting entry at or below the commit index can only come
			// from a stale/retransmitted request: Raft guarantees committed
			// entries are never overwritten with a different term. Refuse to
			// truncate them so a late duplicate can never roll the committed
			// (and possibly already applied) prefix backwards.
			if cut <= n.commit {
				return 0, fmt.Errorf("%w: refusing to truncate committed index %d (commit=%d) from a conflicting append",
					logstore.ErrCompacted, cut, n.commit)
			}
			// truncate from cut, replace with the new suffix
			var fresh []raftproto.Entry
			for i := range ents {
				if ents[i].Index >= cut {
					fresh = append(fresh, ents[i])
				}
			}
			if err := n.log.Truncate(cut, fresh); err != nil {
				return 0, err
			}
			n.afterLogRewrite(first)
			return n.lastIndex(), nil
		}
		// overlapping prefix identical: append only what is new
		var fresh []raftproto.Entry
		for i := range ents {
			if ents[i].Index > localLast {
				fresh = append(fresh, ents[i])
			}
		}
		if len(fresh) == 0 {
			return n.lastIndex(), nil
		}
		if err := n.log.Append(fresh); err != nil {
			return 0, err
		}
		return n.lastIndex(), nil
	default:
		// first == localLast+1: pure extension
		if err := n.log.Append(ents); err != nil {
			return 0, err
		}
		return n.lastIndex(), nil
	}
}

// afterLogRewrite refreshes in-memory bookkeeping after a truncation.
func (n *Node) afterLogRewrite(firstNew uint64) {
	if n.commit >= firstNew {
		n.logger.warnf("committed index %d rolled back to %d-1", n.commit, firstNew)
		n.commit = firstNew - 1
	}
	n.rebuildActiveConfig()
}
