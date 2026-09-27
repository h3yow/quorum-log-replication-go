package cluster

import "github.com/h3yow/quorum-log-replication-go/pkg/raftproto"

// AppliedKV returns a copy of one node's currently applied key/value map. It
// is a read-only observation intended for offline verification; during live
// operation the raft status digest (StateDigest) is the non-perturbing way to
// compare applied histories. After a node has stopped/converged, dumping the
// applied map lets a test identify exactly which key diverged.
func (c *Cluster) AppliedKV(id string) (map[string]string, bool) {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return h.app.SnapshotKV()
}

// SnapshotBytes loads one node's persisted snapshot payload and metadata.
// It is a read-only verification helper (offline/quiesced use only).
func (c *Cluster) SnapshotBytes(id string) ([]byte, raftproto.SnapshotMeta, bool) {
	c.mu.RLock()
	h, ok := c.nodes[id]
	c.mu.RUnlock()
	if !ok {
		return nil, raftproto.SnapshotMeta{}, false
	}
	meta, data, err := h.snap.Load()
	if err != nil || meta == nil {
		return nil, raftproto.SnapshotMeta{}, false
	}
	return data, *meta, true
}
