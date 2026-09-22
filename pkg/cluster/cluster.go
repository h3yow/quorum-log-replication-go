// Package cluster wires multiple raft nodes, a simulated transport, local
// on-disk stores and a KV client into one in-process test/demo harness.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/logstore"
	"github.com/h3yow/quorum-log-replication-go/pkg/raft"
	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
	"github.com/h3yow/quorum-log-replication-go/pkg/statemachine"
	"github.com/h3yow/quorum-log-replication-go/pkg/transport"
)

type nodeHandle struct {
	node *raft.Node
	app  *statemachine.KVStore
	log  *logstore.Store
	snap *logstore.SnapshotStore
}

// Cluster is a group of co-located simulated nodes sharing one transport.
// mu guards the nodes map across concurrent membership changes, crashes,
// restarts and read-only observations.
type Cluster struct {
	rootDir    string
	tr         *transport.Transport
	mu         sync.RWMutex
	nodes      map[string]*nodeHandle
	tickMS     int
	syncWrites bool
}

// Options configures a bootstrapped cluster.
type Options struct {
	DataDir string
	NodeIDs []string
	TickMS  int
	// SyncWrites controls fsync on the durable stores. It defaults to false
	// for the in-process simulator (offline, deterministic, and the sandbox
	// filesystem can stall fsync badly). Crash-safety of the on-disk format is
	// still verified by the corruption/truncation/restart tests. Set true to
	// exercise real fsync durability.
	SyncWrites bool
}

// New bootstraps an initial cluster whose first log entry is the committed
// initial configuration containing all voters.
func New(ctx context.Context, opt Options) (*Cluster, error) {
	if len(opt.NodeIDs) < 1 {
		return nil, fmt.Errorf("cluster: at least one node required")
	}
	if opt.TickMS == 0 {
		opt.TickMS = 25
	}
	c := &Cluster{
		rootDir:    opt.DataDir,
		tr:         transport.New(),
		nodes:      map[string]*nodeHandle{},
		tickMS:     opt.TickMS,
		syncWrites: opt.SyncWrites,
	}
	initCfg := &raftproto.Config{Voters: append([]string(nil), opt.NodeIDs...)}
	for _, id := range opt.NodeIDs {
		if err := c.openNode(ctx, id, initCfg); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) nodeConfig(id string) raft.Config {
	tick := time.Duration(c.tickMS) * time.Millisecond
	return raft.Config{
		ID:                id,
		ElectionTickMin:   16,
		ElectionTickMax:   30,
		HeartbeatTick:     1,
		TickInterval:      tick,
		SnapshotThreshold: 32,
		RPCTimeout:        400 * time.Millisecond,
	}
}

// openNode creates/opens the durable files for id and starts the node.
// When bootstrapCfg is non-nil the node bootstraps it as entry #1; an
// existing node ignores it (its log is already initialized).
func (c *Cluster) openNode(ctx context.Context, id string, bootstrapCfg *raftproto.Config) error {
	dir := filepath.Join(c.rootDir, "node-"+id)
	store, err := logstore.New(dir)
	if err != nil {
		return fmt.Errorf("node %s: open store: %w", id, err)
	}
	snaps, err := logstore.NewSnapshotStore(dir)
	if err != nil {
		return fmt.Errorf("node %s: snapshots: %w", id, err)
	}
	store.SetSync(c.syncWrites)
	snaps.SetSync(c.syncWrites)
	app := statemachine.NewKVStore()
	cfg := c.nodeConfig(id)
	cfg.InitialConfig = bootstrapCfg
	n, err := raft.New(cfg, store, snaps, c.tr, app)
	if err != nil {
		return fmt.Errorf("node %s: raft init: %w", id, err)
	}
	if err := n.Start(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	c.nodes[id] = &nodeHandle{node: n, app: app, log: store, snap: snaps}
	c.mu.Unlock()
	return nil
}

// snapshotNodes returns a stable copy of the current raft node pointers under
// the lock. Callers may use a pointer after the lock is released; if a node is
// concurrently restarted they receive the pre-restart (stopped) pointer and
// retry on ErrStopped/ErrNotLeader.
func (c *Cluster) snapshotNodes() map[string]*raft.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*raft.Node, len(c.nodes))
	for id, h := range c.nodes {
		out[id] = h.node
	}
	return out
}

// Close stops every node and closes its files.
func (c *Cluster) Close() {
	c.mu.RLock()
	handles := make([]*nodeHandle, 0, len(c.nodes))
	for _, h := range c.nodes {
		handles = append(handles, h)
	}
	c.mu.RUnlock()
	for _, h := range handles {
		h.node.Stop()
		_ = h.log.Close()
	}
}

// Leader returns the observed current leader id, or "". Crashed (stopped or
// transport-unregistered) nodes are never reported as leader.
func (c *Cluster) Leader() string {
	term := uint64(0)
	leader := ""
	for id, node := range c.snapshotNodes() {
		s := node.Status()
		if s.State != raftproto.StateLeader {
			continue
		}
		// A node we cannot even reach cannot serve a client; skip it even if
		// its last cached role was leader (it may have just crashed).
		if !c.tr.IsReachable(id, id) {
			continue
		}
		if s.Term >= term {
			term, leader = s.Term, id
		}
	}
	return leader
}

// Node returns one node by id.
func (c *Cluster) Node(id string) *raft.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if h, ok := c.nodes[id]; ok {
		return h.node
	}
	return nil
}

// Partition isolates id bidirectionally from peers (all if empty).
func (c *Cluster) Partition(id string, peers ...string) {
	c.tr.Partition(id, peers...)
}

// Heal removes partitions involving id.
func (c *Cluster) Heal(id string) { c.tr.Heal(id) }

// HealAll removes every partition.
func (c *Cluster) HealAll() { c.tr.HealAll() }

// Crash stops the node and removes it from the transport; disk is retained.
func (c *Cluster) Crash(ctx context.Context, id string) error {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("cluster: unknown node %s", id)
	}
	h.node.Stop()
	c.tr.Unregister(id)
	if err := h.log.Close(); err != nil {
		return err
	}
	return nil
}

// Restart reloads persisted state from disk and rejoins the transport.
func (c *Cluster) Restart(ctx context.Context, id string) error {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("cluster: unknown node %s", id)
	}
	dir := filepath.Join(c.rootDir, "node-"+id)
	store, err := logstore.New(dir)
	if err != nil {
		return fmt.Errorf("node %s restart: %w", id, err)
	}
	snaps, err := logstore.NewSnapshotStore(dir)
	if err != nil {
		return err
	}
	// A real crash loses all in-memory application state. Restart from a
	// fresh KV store and let raft reconstruct it from the snapshot plus a
	// WAL replay, rather than trusting the stale pre-crash object.
	app := statemachine.NewKVStore()
	n, err := raft.New(c.nodeConfig(id), store, snaps, c.tr, app)
	if err != nil {
		return err
	}
	if err := n.Start(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	h.node = n
	h.app = app
	h.log = store
	h.snap = snaps
	c.mu.Unlock()
	return nil
}

// AddNode creates a new on-disk node and adds it through the leader.
func (c *Cluster) AddNode(ctx context.Context, id string) error {
	c.mu.RLock()
	_, exists := c.nodes[id]
	c.mu.RUnlock()
	if exists {
		return fmt.Errorf("cluster: node %s exists", id)
	}
	if err := c.openNode(ctx, id, nil); err != nil {
		return err
	}
	return c.proposeToLeader(ctx, func(n *raft.Node) error { return n.AddServer(ctx, id) })
}

// RemoveNode performs a committed removal and stops the removed node. It is
// safe to retry/redirect: if the server is already absent from the observed
// configuration (a previous attempt already committed it), it is a success.
func (c *Cluster) RemoveNode(ctx context.Context, id string) error {
	c.mu.RLock()
	_, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("cluster: unknown node %s", id)
	}
	err := c.proposeToLeader(ctx, func(n *raft.Node) error { return n.RemoveServer(ctx, id) })
	if err != nil && !errors.Is(err, raft.ErrUnknownMember) {
		return err
	}
	// Whether the removal committed on this attempt or an earlier redirected
	// one, converge the local handle to stopped/removed.
	_ = c.Crash(ctx, id)
	c.mu.Lock()
	delete(c.nodes, id)
	c.mu.Unlock()
	return nil
}

// proposeToLeader locates the leader (retrying across elections) and runs f.
func (c *Cluster) proposeToLeader(ctx context.Context, f func(*raft.Node) error) error {
	var lastErr error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		id := c.Leader()
		if id == "" {
			time.Sleep(30 * time.Millisecond)
			continue
		}
		leader := c.Node(id)
		if leader == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err := f(leader); err != nil {
			lastErr = err
			time.Sleep(30 * time.Millisecond)
			continue
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return raft.ErrNoQuorum
}

// Client returns a fresh client with a unique id.
func (c *Cluster) Client() *Client {
	return newClient(c)
}

// Transport exposes the transport for test chaos configuration.
func (c *Cluster) Transport() *transport.Transport { return c.tr }
