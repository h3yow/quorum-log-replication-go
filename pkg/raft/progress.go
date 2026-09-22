package raft

// reinitProgress rebuilds per-peer replication bookkeeping from the active
// config: leaders start followers at the local tail (the backtrack logic in
// onAppendResponse walks them backward, switching to snapshot if needed).
func (n *Node) reinitProgress() {
	next := map[string]*progress{}
	last := n.lastIndex()
	add := func(id string, voter bool) {
		if id == n.cfg.ID {
			next[id] = &progress{matchIndex: last, nextIndex: last + 1, voter: voter}
			return
		}
		if old, ok := n.peers[id]; ok {
			next[id] = old
			old.voter = voter
			return
		}
		next[id] = &progress{matchIndex: 0, nextIndex: last + 1, voter: voter}
	}
	for _, id := range n.config.Voters {
		add(id, true)
	}
	for _, id := range n.config.JointVoters {
		if _, ok := peerOf(next, id); !ok {
			add(id, true)
		}
	}
	for _, id := range n.config.Learners {
		add(id, false)
	}
	n.peers = next
}

func peerOf(m map[string]*progress, id string) (*progress, bool) {
	p, ok := m[id]
	return p, ok
}
