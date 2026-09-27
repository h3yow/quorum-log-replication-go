package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raft"
	"github.com/h3yow/quorum-log-replication-go/pkg/statemachine"
)

// ProposeOutcome is the raw, single-attempt result of an ExplicitPropose.
type ProposeOutcome struct {
	Accepted bool
	// Dup is true when the state machine already held the (client,seq) pair:
	// the request took no new effect.
	Dup bool
	// Err is the transport/leader/quorum error, if the attempt did not apply.
	Err error
}

// ExplicitPropose performs ONE bounded proposal attempt through whatever node
// is currently observed as leader, using an explicit client id and sequence
// number without any internal retry. It exists for failure drills that must
// observe the fate of a single ambiguous attempt and then retry the identical
// payload themselves. Normal client traffic must keep using Client.
func (c *Cluster) ExplicitPropose(ctx context.Context, clientID string, seq uint64, cmd statemachine.Command) ProposeOutcome {
	cmd.ClientID = clientID
	cmd.Seq = seq
	data, err := cmd.Encode()
	if err != nil {
		return ProposeOutcome{Err: err}
	}
	leaderID := c.Leader()
	if leaderID == "" {
		return ProposeOutcome{Err: raft.ErrNoQuorum}
	}
	leader := c.Node(leaderID)
	if leader == nil {
		return ProposeOutcome{Err: raft.ErrNotLeader}
	}
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	res, err := leader.Propose(pctx, data)
	if err != nil {
		return ProposeOutcome{Err: err}
	}
	return ProposeOutcome{Accepted: true, Dup: res.Dup}
}

// NodeStateSnapshot is one node's status and applied-state digests captured
// together as a single observation for failure-drill evidence.
type NodeStateSnapshot struct {
	Status    raft.Status
	StateHash string
	KVHash    string
}

// NodeState returns one node's raft status together with its applied-state
// digests. The values are collected close together so convergence checks do
// not compare a status from one instant with contents from another while a
// node is mid catch-up.
func (c *Cluster) NodeState(id string) (NodeStateSnapshot, bool) {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return NodeStateSnapshot{}, false
	}
	sh, _ := c.StateHash(id)
	kh, _ := c.KVHash(id)
	return NodeStateSnapshot{Status: h.node.Status(), StateHash: sh, KVHash: kh}, true
}

// StateHash returns a stable hex digest of one node's applied state machine
// snapshot (KV contents plus the dedup table). Nodes that converged to the
// same applied history produce the same digest.
func (c *Cluster) StateHash(id string) (string, error) {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return "", errors.New("cluster: unknown node " + id)
	}
	b, err := h.app.Snapshot()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// IsDown reports whether a node is currently unreachable from every peer:
// crashed/unregistered nodes, or a node isolated by a full partition.
func (c *Cluster) IsDown(id string) bool {
	c.mu.RLock()
	_, registered := c.nodes[id]
	c.mu.RUnlock()
	if !registered {
		return true
	}
	if !c.tr.IsReachable(id, id) {
		return true
	}
	for _, other := range c.snapshotNodes() {
		if other.ID() == id {
			continue
		}
		if c.tr.IsReachable(id, other.ID()) {
			return false
		}
	}
	return true
}

// LogEntry is one entry observed in a node's retained log window.
type LogEntry struct {
	Index  uint64
	Term   uint64
	Data   []byte
	Config bool
}

// RetainedEntries returns the entries still held in id's WAL window
// (post-snapshot). It is a read-only evidence path for failure drills;
// snapshot-covered prefixes are intentionally absent here.
func (c *Cluster) RetainedEntries(id string) ([]LogEntry, error) {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return nil, errors.New("cluster: unknown node " + id)
	}
	lo := h.log.FirstIndex()
	hi := h.log.LastIndex() + 1
	if hi <= lo {
		return nil, nil
	}
	ents, err := h.log.Entries(lo, hi)
	if err != nil {
		return nil, err
	}
	out := make([]LogEntry, 0, len(ents))
	for _, e := range ents {
		out = append(out, LogEntry{Index: e.Index, Term: e.Term, Data: e.Data, Config: e.Type == 2})
	}
	return out, nil
}

// ReachableVoters reports, from node from's perspective, which of the given
// peers currently accept messages. It sends no traffic and changes no state.
func (c *Cluster) ReachableVoters(from string, peers []string) []string {
	return c.tr.ReachableVoters(from, peers)
}

// NodeIDs returns a sorted-independent snapshot of currently known node ids.
func (c *Cluster) NodeIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		out = append(out, id)
	}
	return out
}

// ClientSeqEvidence is the read-only applied-frontier fact for one client.
type ClientSeqEvidence struct {
	Seq   uint64
	Known bool
}

// ClientSeq returns the highest applied sequence recorded on id's state
// machine for clientID. Drills use it to prove a retried ambiguous request
// advanced the applied frontier exactly once.
func (c *Cluster) ClientSeq(id, clientID string) ClientSeqEvidence {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return ClientSeqEvidence{}
	}
	seq, known := h.app.ClientSeq(clientID)
	return ClientSeqEvidence{Seq: seq, Known: known}
}

// KVHash returns a canonical digest of just the applied key/value contents
// (sorted), independent of the per-run client-id dedup table. Two runs that
// converged to the same committed history produce the same KVHash even though
// their generated client ids differ.
func (c *Cluster) KVHash(id string) (string, error) {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return "", errors.New("cluster: unknown node " + id)
	}
	b, err := h.app.Snapshot()
	if err != nil {
		return "", err
	}
	var sd struct {
		KV map[string]string `json:"kv"`
	}
	if err := json.Unmarshal(b, &sd); err != nil {
		return "", err
	}
	keys := make([]string, 0, len(sd.KV))
	for k := range sd.KV {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	hsh := sha256.New()
	for _, k := range keys {
		hsh.Write([]byte(k))
		hsh.Write([]byte{0})
		hsh.Write([]byte(sd.KV[k]))
		hsh.Write([]byte{0})
	}
	return hex.EncodeToString(hsh.Sum(nil)), nil
}
