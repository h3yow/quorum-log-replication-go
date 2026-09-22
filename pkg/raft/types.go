// Package raft implements the replicated log and leader coordination core:
// leader election, log replication with majority commit, persistence, joint
// consensus membership changes, snapshots/catch-up and read-index queries.
package raft

// progress tracks one peer's replication bookkeeping. All fields are accessed
// only from the owning node's event loop.
type progress struct {
	matchIndex uint64 // highest index known to be replicated on the peer
	nextIndex  uint64 // next index to send to the peer
	voter      bool   // false for non-voting learners
}
