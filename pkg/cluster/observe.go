package cluster

import "github.com/h3yow/quorum-log-replication-go/pkg/raft"

// Observation is the read-only cluster-wide view entry point. Collecting it
// never sends Raft messages, never moves timers, and never changes state.
type Observation struct {
	Leader string
	Nodes  map[string]raft.Status
}

// Observe snapshots every node's local status plus the observed leader. It is
// safe to call concurrently with membership changes, crashes and restarts.
func (c *Cluster) Observe() Observation {
	o := Observation{Leader: c.Leader(), Nodes: map[string]raft.Status{}}
	for id, node := range c.snapshotNodes() {
		o.Nodes[id] = node.Status()
	}
	return o
}
