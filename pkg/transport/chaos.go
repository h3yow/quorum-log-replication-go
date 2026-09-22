package transport

import "time"

// Partition installs a bidirectional partition between id and the given
// peers. An empty peer list isolates id from every other registered node.
// Partitions are symmetric: neither side can deliver to the other, which
// models a real network split for the majority/minority scenarios.
func (t *Transport) Partition(id string, peers ...string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(peers) == 0 {
		for other := range t.handlers {
			if other == id {
				continue
			}
			t.block(id, other)
		}
		return
	}
	for _, p := range peers {
		t.block(id, p)
	}
}

func (t *Transport) block(a, b string) {
	if t.blocked[a] == nil {
		t.blocked[a] = map[string]bool{}
	}
	if t.blocked[b] == nil {
		t.blocked[b] = map[string]bool{}
	}
	t.blocked[a][b] = true
	t.blocked[b][a] = true
}

// Heal removes all partitions involving id and revives its alive flag.
func (t *Transport) Heal(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.blocked, id)
	for _, m := range t.blocked {
		delete(m, id)
	}
	if _, ok := t.handlers[id]; ok {
		t.alive[id] = true
	}
}

// HealAll removes every installed partition and revives all nodes.
func (t *Transport) HealAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blocked = map[string]map[string]bool{}
	for id := range t.handlers {
		t.alive[id] = true
	}
}

// SetLatency sets a fixed per-message delivery delay on a directed link
// ("" from/to acts as wildcard).
func (t *Transport) SetLatency(from, to string, millis int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rules[[2]string{from, to}] = linkRule{latency: time.Duration(millis) * time.Millisecond}
}

// DropRate configures random message loss probability [0,1] on a link.
func (t *Transport) DropRate(from, to string, rate float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.rules[[2]string{from, to}]
	r.dropRate = rate
	t.rules[[2]string{from, to}] = r
}
