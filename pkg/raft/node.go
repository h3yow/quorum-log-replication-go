package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/logstore"
	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
	"github.com/h3yow/quorum-log-replication-go/pkg/statemachine"
	"github.com/h3yow/quorum-log-replication-go/pkg/transport"
)

// Public, distinguishable errors.
var (
	ErrNotLeader     = errors.New("raft: not the leader")
	ErrStopped       = errors.New("raft: node stopped")
	ErrNoQuorum      = errors.New("raft: quorum unavailable")
	ErrRejected      = errors.New("raft: request rejected")
	ErrChangePending = errors.New("raft: a membership change is already in progress")
	ErrAlreadyMember = errors.New("raft: server already a voting member")
	ErrUnknownMember = errors.New("raft: server is not a member")
)

// Config tunes one node's timers and identity.
type Config struct {
	ID string
	// InitialConfig is durably written as index 1 only when the store is
	// still empty (cluster bootstrap).
	InitialConfig     *raftproto.Config
	ElectionTickMin   int
	ElectionTickMax   int
	HeartbeatTick     int
	TickInterval      time.Duration
	SnapshotThreshold uint64
	RPCTimeout        time.Duration
}

// ProposeResult is delivered to a client once an entry is committed/applied.
type ProposeResult struct {
	Index uint64
	Term  uint64
	Dup   bool
	Value string
}

// PeerProgress is one peer's observed sync position.
type PeerProgress struct {
	MatchIndex uint64
	NextIndex  uint64
	Voter      bool
}

// Status is the read-only observation of a node. Collecting it never changes
// timers, roles or log state.
type Status struct {
	ID              string
	State           raftproto.NodeState
	Term            uint64
	Leader          string
	CommitIndex     uint64
	LastApplied     uint64
	SnapshotIndex   uint64
	CommittedConfig raftproto.Config
	ActiveConfig    raftproto.Config
	Progress        map[string]PeerProgress
	// StateDigest is the deterministic digest of the applied key/value
	// contents, computed on the event loop and cached like the other fields.
	// Equal digests on every node mean equal confirmed histories.
	StateDigest string
}

// Node is one raft server. All state mutation happens on its single event
// loop goroutine; network sends run in detached goroutines and report back
// as events, so no raft state is touched off-loop.
type Node struct {
	cfg    Config
	log    *logstore.Store
	snaps  *logstore.SnapshotStore
	tr     *transport.Transport
	app    *statemachine.KVStore
	logger *Logger

	// --- event-loop-owned state ---
	state   raftproto.NodeState
	term    uint64
	vote    string
	lead    string
	config  raftproto.Config // latest config present in the log
	commCfg raftproto.Config // config that has reached the commit index

	snapMeta raftproto.SnapshotMeta // zero Index when no snapshot
	commit   uint64
	applied  uint64
	// configHookIdx is the highest applied index whose EntryConfig commit
	// side-effect has already run. It makes the hook idempotent across the
	// nested applyTo calls that occur while waitCommit pumps events.
	configHookIdx uint64

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int

	peers map[string]*progress
	votes map[string]bool
	// voteGen tags the current election round. Vote responses from an earlier
	// round (including a same-term retry or a stale in-flight request) carry a
	// stale gen and can never be counted toward the current election.
	voteGen uint64
	// refusedInTerm / refusedCandidates remember candidates whose
	// RequestVote was rejected for a stale log in a given term, so a repeated
	// identical request is rejected deterministically instead of ever granting
	// a vote to a candidate once proven behind.
	refusedInTerm     uint64
	refusedCandidates []string

	// waiters for committed indices (client proposals + internal config)
	waiters map[uint64][]chan applyResult

	// read-index bookkeeping
	readGen uint64
	pending []*pendingRead

	// membership change serialization
	changeInProgress bool
	mchange          *pendingChange

	eventCh  chan event
	wakeCh   chan struct{}
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once

	// read-only cache for observation/status: observing must never block on
	// or perturb the event loop.
	obsMu       sync.RWMutex
	obsTerm     uint64
	obsCommit   uint64
	obsApply    uint64
	obsSnap     uint64
	obsState    raftproto.NodeState
	obsLeader   string
	obsComm     raftproto.Config
	obsActive   raftproto.Config
	obsProgress map[string]PeerProgress
	obsDigest   string
}

type applyResult struct {
	res statemachine.Result
	err error
}

type pendingRead struct {
	gen     uint64
	acks    map[string]bool
	index   uint64 // commit index to wait applied for
	key     string
	replyCh chan readReply
}

type readReply struct {
	value string
	found bool
	err   error
}

// New constructs a node, eagerly validates persisted state and registers
// the node with the simulated transport. Corrupt/truncated local state makes
// construction fail with a distinguishable logstore error: the node never
// serves from uncertain state.
func New(cfg Config, store *logstore.Store, snaps *logstore.SnapshotStore, tr *transport.Transport, app *statemachine.KVStore) (*Node, error) {
	n := &Node{
		cfg:     cfg,
		log:     store,
		snaps:   snaps,
		tr:      tr,
		app:     app,
		logger:  NewLogger(cfg.ID),
		state:   raftproto.StateFollower,
		peers:   map[string]*progress{},
		votes:   map[string]bool{},
		waiters: map[uint64][]chan applyResult{},
		eventCh: make(chan event, 256),
		wakeCh:  make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	if err := n.loadState(); err != nil {
		return nil, err
	}
	if err := tr.Register(cfg.ID, n); err != nil {
		return nil, err
	}
	return n, nil
}

// Start launches the event loop.
func (n *Node) Start(ctx context.Context) error {
	go n.run(ctx)
	return nil
}

// Stop halts the node; durable files are retained for a later restart.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
		<-n.doneCh
		// Make read-only observation report a non-leader, stopped node so a
		// client never keeps selecting it after a crash.
		n.obsMu.Lock()
		n.obsState = raftproto.StateFollower
		n.obsLeader = ""
		n.obsMu.Unlock()
	})
}

// Propose appends a client command and waits until it is committed/applied.
func (n *Node) Propose(ctx context.Context, cmd []byte) (ProposeResult, error) {
	reply := make(chan applyResult, 1)
	e := proposeEvent{cmd: cmd, reply: reply}
	if !n.enqueue(ctx, e) {
		return ProposeResult{}, ErrStopped
	}
	select {
	case r := <-reply:
		if r.err != nil {
			return ProposeResult{}, r.err
		}
		return ProposeResult{
			Dup:   r.res.Dup,
			Value: r.res.Value,
		}, nil
	case <-ctx.Done():
		// The entry may still commit later; the (client,seq) dedupe in the
		// state machine makes a retry idempotent, never double-applied.
		return ProposeResult{}, ctx.Err()
	case <-n.stopCh:
		return ProposeResult{}, ErrStopped
	}
}

// ReadIndex serves a linearizable read via the read-index protocol.
func (n *Node) ReadIndex(ctx context.Context, key string) (string, bool, error) {
	reply := make(chan readReply, 1)
	if !n.enqueue(ctx, readEvent{key: key, reply: reply}) {
		return "", false, ErrStopped
	}
	select {
	case r := <-reply:
		return r.value, r.found, r.err
	case <-ctx.Done():
		return "", false, ctx.Err()
	case <-n.stopCh:
		return "", false, ErrStopped
	}
}

// AddServer stages a new node, syncs it as a learner and promotes it through
// a committed joint-consensus change. Safe to retry: a present voter is a
// no-op success.
func (n *Node) AddServer(ctx context.Context, id string) error {
	return n.changeMembership(ctx, id, true)
}

// RemoveServer removes a voter through a committed joint change.
func (n *Node) RemoveServer(ctx context.Context, id string) error {
	return n.changeMembership(ctx, id, false)
}

// Status returns a read-only snapshot without mutating cluster behavior.
// It reads a small atomic cache and never queues onto the event loop.
func (n *Node) Status() Status {
	n.obsMu.RLock()
	defer n.obsMu.RUnlock()
	prog := make(map[string]PeerProgress, len(n.obsProgress))
	for k, v := range n.obsProgress {
		prog[k] = v
	}
	return Status{
		ID:              n.cfg.ID,
		State:           n.obsState,
		Term:            n.obsTerm,
		Leader:          n.obsLeader,
		CommitIndex:     n.obsCommit,
		LastApplied:     n.obsApply,
		SnapshotIndex:   n.obsSnap,
		CommittedConfig: cloneCfg(n.obsComm),
		ActiveConfig:    cloneCfg(n.obsActive),
		Progress:        prog,
		StateDigest:     n.obsDigest,
	}
}

// enqueue submits an event to the loop; it fails (false) when the node has
// stopped or the caller's context is already done.
func (n *Node) enqueue(ctx context.Context, e event) bool {
	select {
	case n.eventCh <- e:
		return true
	case <-n.stopCh:
		return false
	case <-ctx.Done():
		return false
	}
}

func (n *Node) signal() {
	select {
	case n.wakeCh <- struct{}{}:
	default:
	}
}

// ID returns the node id.
func (n *Node) ID() string { return n.cfg.ID }
