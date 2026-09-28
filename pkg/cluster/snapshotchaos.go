package cluster

import (
	"context"
	"errors"
	"sync"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
	"github.com/h3yow/quorum-log-replication-go/pkg/transport"
)

// Snapshot-chaos sentinel errors.
var (
	// ErrNoSnapshotObserved is returned by a replay call before any
	// InstallSnapshot RPC was captured for the target.
	ErrNoSnapshotObserved = errors.New("cluster: no InstallSnapshot observed to replay")

	errSnapshotTargetNotRegistered = errors.New("cluster: snapshot interception target not registered")
)

// SnapshotInterceptor deterministically observes and replays InstallSnapshot
// RPCs delivered to one node. It is the test/drill hook for "duplicate delivery
// / network retransmission of a catch-up snapshot after the node has already
// advanced past it": all state-machine effects still run on the target's raft
// event loop, the interceptor only duplicates a wire message.
type SnapshotInterceptor struct {
	c      *Cluster
	target string

	mu     sync.Mutex
	oldest *raftproto.InstallSnapshotRequest
	newest *raftproto.InstallSnapshotRequest
	count  int
}

// InterceptSnapshots wraps targetID's transport handler so subsequent
// InstallSnapshot RPCs are observed. Install it AFTER a Restart (restart
// re-registers the real handler). It does not alter message delivery.
func (c *Cluster) InterceptSnapshots(targetID string) (*SnapshotInterceptor, error) {
	if _, ok := c.Transport().Handler(targetID); !ok {
		return nil, errSnapshotTargetNotRegistered
	}
	si := &SnapshotInterceptor{c: c, target: targetID}
	si.attach()
	return si, nil
}

func (si *SnapshotInterceptor) attach() {
	inner, ok := si.c.Transport().Handler(si.target)
	if !ok {
		return
	}
	si.c.Transport().Register(si.target, snapshotInterceptHandler{inner: inner, si: si})
}

func (si *SnapshotInterceptor) record(req *raftproto.InstallSnapshotRequest) {
	cp := *req
	cp.Data = append([]byte(nil), req.Data...)
	si.mu.Lock()
	defer si.mu.Unlock()
	si.count++
	if si.oldest == nil || req.LastIncludedIndex < si.oldest.LastIncludedIndex {
		si.oldest = &cp
	}
	if si.newest == nil || req.LastIncludedIndex >= si.newest.LastIncludedIndex {
		si.newest = &cp
	}
}

// Observed reports how many InstallSnapshot RPCs were observed and the index of
// the newest one captured (0 if none yet).
func (si *SnapshotInterceptor) Observed() (count int, newestIndex uint64) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.newest != nil {
		newestIndex = si.newest.LastIncludedIndex
	}
	return si.count, newestIndex
}

// OldestIndex / NewestIndex expose the captured snapshot boundaries.
func (si *SnapshotInterceptor) OldestIndex() (uint64, bool) {
	si.mu.Lock()
	defer si.mu.Unlock()
	if si.oldest == nil {
		return 0, false
	}
	return si.oldest.LastIncludedIndex, true
}

// ReplayOldest synchronously re-delivers the OLDEST captured snapshot (the
// stale catch-up message). It returns ErrNoSnapshotObserved if no snapshot has
// been seen yet. Delivery is inline so callers observe a deterministic result.
func (si *SnapshotInterceptor) ReplayOldest(ctx context.Context) error {
	si.mu.Lock()
	req := si.oldest
	si.mu.Unlock()
	return si.replay(ctx, req)
}

// ReplayNewest synchronously re-delivers the NEWEST captured snapshot. This is
// the duplicate-of-the-just-installed-snapshot case that must be a no-op once
// the node applied anything beyond it.
func (si *SnapshotInterceptor) ReplayNewest(ctx context.Context) error {
	si.mu.Lock()
	req := si.newest
	si.mu.Unlock()
	return si.replay(ctx, req)
}

func (si *SnapshotInterceptor) replay(ctx context.Context, req *raftproto.InstallSnapshotRequest) error {
	if req == nil {
		return ErrNoSnapshotObserved
	}
	h, ok := si.c.Transport().Handler(si.target)
	if !ok {
		return errSnapshotTargetNotRegistered
	}
	cp := *req
	cp.Data = append([]byte(nil), req.Data...)
	_, err := h.HandleInstallSnapshot(ctx, cp.LeaderID, &cp)
	return err
}

type snapshotInterceptHandler struct {
	inner transport.Handler
	si    *SnapshotInterceptor
}

func (h snapshotInterceptHandler) HandleAppendEntries(ctx context.Context, from string, req *raftproto.AppendEntriesRequest) (*raftproto.AppendEntriesResponse, error) {
	return h.inner.HandleAppendEntries(ctx, from, req)
}

func (h snapshotInterceptHandler) HandleRequestVote(ctx context.Context, from string, req *raftproto.RequestVoteRequest) (*raftproto.RequestVoteResponse, error) {
	return h.inner.HandleRequestVote(ctx, from, req)
}

func (h snapshotInterceptHandler) HandleInstallSnapshot(ctx context.Context, from string, req *raftproto.InstallSnapshotRequest) (*raftproto.InstallSnapshotResponse, error) {
	h.si.record(req)
	return h.inner.HandleInstallSnapshot(ctx, from, req)
}
