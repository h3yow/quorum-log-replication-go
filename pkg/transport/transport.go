// Package transport is an in-process simulated network between raft nodes.
// It models partitions, per-link latency and message dropping without using
// real sockets, so all verification can run offline and deterministically.
package transport

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// ErrUnreachable indicates the target is crashed or partitioned away.
var ErrUnreachable = errors.New("transport: destination unreachable")

// Handler is implemented by a raft node; the transport dispatches into it.
type Handler interface {
	HandleAppendEntries(ctx context.Context, from string, req *raftproto.AppendEntriesRequest) (*raftproto.AppendEntriesResponse, error)
	HandleRequestVote(ctx context.Context, from string, req *raftproto.RequestVoteRequest) (*raftproto.RequestVoteResponse, error)
	HandleInstallSnapshot(ctx context.Context, from string, req *raftproto.InstallSnapshotRequest) (*raftproto.InstallSnapshotResponse, error)
}

type linkRule struct {
	latency  time.Duration
	dropRate float64
}

// Transport simulates a fully-connected network with injectable failures.
type Transport struct {
	mu sync.RWMutex

	handlers map[string]Handler
	alive    map[string]bool

	// blocked[a] contains b when a->b delivery is forbidden.
	blocked map[string]map[string]bool
	rules   map[[2]string]linkRule
}

// New creates an empty transport.
func New() *Transport {
	return &Transport{
		handlers: map[string]Handler{},
		alive:    map[string]bool{},
		blocked:  map[string]map[string]bool{},
		rules:    map[[2]string]linkRule{},
	}
}

// Register connects a node id to its handler.
func (t *Transport) Register(id string, h Handler) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = h
	t.alive[id] = true
	return nil
}

// Unregister removes a crashed node from the network entirely.
func (t *Transport) Unregister(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handlers, id)
	t.alive[id] = false
}

// SetAlive flips the reachability flag of a registered node without
// deleting its handler (used by hard-crash simulation).
func (t *Transport) SetAlive(id string, alive bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.handlers[id]; ok {
		t.alive[id] = alive
	}
}

func (t *Transport) pick(a, b string) (Handler, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.alive[a] || !t.alive[b] {
		return nil, false
	}
	if t.blocked[a] != nil && t.blocked[a][b] {
		return nil, false
	}
	h, ok := t.handlers[b]
	if !ok {
		return nil, false
	}
	return h, true
}

func (t *Transport) ruleFor(a, b string) linkRule {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if r, ok := t.rules[[2]string{a, b}]; ok {
		return r
	}
	if r, ok := t.rules[[2]string{"", b}]; ok {
		return r
	}
	if r, ok := t.rules[[2]string{a, ""}]; ok {
		return r
	}
	return t.rules[[2]string{"", ""}]
}

// deliver applies latency then invokes the peer handler, honoring context
// cancellation and the unreachable contract (no response at all).
func (t *Transport) deliver(ctx context.Context, from, to string, fn func(Handler) error) error {
	h, ok := t.pick(from, to)
	if !ok {
		return ErrUnreachable
	}
	r := t.ruleFor(from, to)
	if r.latency > 0 {
		select {
		case <-time.After(r.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Re-check partition after the delay (partitions may move during wait).
	if h2, ok2 := t.pick(from, to); ok2 {
		h = h2
	} else {
		return ErrUnreachable
	}
	if r.dropRate > 0 && randFloat() < r.dropRate {
		return ErrUnreachable // dropped request looks like a timeout
	}
	return fn(h)
}

// SendAppendEntries delivers one AppendEntries RPC.
func (t *Transport) SendAppendEntries(ctx context.Context, from, to string, req *raftproto.AppendEntriesRequest) (*raftproto.AppendEntriesResponse, error) {
	var resp *raftproto.AppendEntriesResponse
	err := t.deliver(ctx, from, to, func(h Handler) error {
		var derr error
		resp, derr = h.HandleAppendEntries(ctx, from, req)
		return derr
	})
	return resp, err
}

// SendRequestVote delivers one RequestVote RPC.
func (t *Transport) SendRequestVote(ctx context.Context, from, to string, req *raftproto.RequestVoteRequest) (*raftproto.RequestVoteResponse, error) {
	var resp *raftproto.RequestVoteResponse
	err := t.deliver(ctx, from, to, func(h Handler) error {
		var derr error
		resp, derr = h.HandleRequestVote(ctx, from, req)
		return derr
	})
	return resp, err
}

// SendInstallSnapshot delivers one InstallSnapshot RPC.
func (t *Transport) SendInstallSnapshot(ctx context.Context, from, to string, req *raftproto.InstallSnapshotRequest) (*raftproto.InstallSnapshotResponse, error) {
	var resp *raftproto.InstallSnapshotResponse
	err := t.deliver(ctx, from, to, func(h Handler) error {
		var derr error
		resp, derr = h.HandleInstallSnapshot(ctx, from, req)
		return derr
	})
	return resp, err
}

// IsReachable reports whether messages from a to b are currently delivered.
func (t *Transport) IsReachable(a, b string) bool {
	_, ok := t.pick(a, b)
	return ok
}

// ReachableVoters reports, from the perspective of node `from`, which of the
// given peer ids currently accept delivered messages (registered, alive, not
// partitioned). It sends no traffic and changes no state.
func (t *Transport) ReachableVoters(from string, peers []string) []string {
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		if p == from {
			out = append(out, p)
			continue
		}
		if _, ok := t.pick(from, p); ok {
			out = append(out, p)
		}
	}
	return out
}
