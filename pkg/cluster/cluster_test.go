package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

func newTestCluster(t *testing.T, ids ...string) (*Cluster, context.Context, context.CancelFunc) {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	c, err := New(ctx, Options{DataDir: dir, NodeIDs: ids, TickMS: 15})
	if err != nil {
		cancel()
		t.Fatalf("New cluster: %v", err)
	}
	t.Cleanup(func() { c.Close(); cancel() })
	return c, ctx, cancel
}

func waitLeader(t *testing.T, c *Cluster, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != "" {
			return l
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return ""
}

func TestElectLeader(t *testing.T) {
	c, _, _ := newTestCluster(t, "n1", "n2", "n3")
	leader := waitLeader(t, c, 3*time.Second)
	t.Logf("elected leader: %s", leader)

	// Wait for the cluster view to stabilize on exactly one leader.
	var leaders int
	var term uint64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leaders, term = 0, 0
		for _, s := range c.Observe().Nodes {
			if s.State == raftproto.StateLeader {
				leaders++
				term = s.Term
			}
		}
		if leaders == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leaders != 1 {
		t.Fatalf("expected exactly one leader, got %d (term %d)", leaders, term)
	}
	// Across all observed statuses, no two leaders may share one term.
	byTerm := map[uint64]int{}
	for _, s := range c.Observe().Nodes {
		if s.State == raftproto.StateLeader {
			byTerm[s.Term]++
		}
	}
	for t2, n := range byTerm {
		if n > 1 {
			t.Fatalf("two leaders observed in term %d", t2)
		}
	}
}

func TestBasicReplication(t *testing.T) {
	c, ctx, _ := newTestCluster(t, "n1", "n2", "n3")
	waitLeader(t, c, 3*time.Second)
	cl := c.Client()
	if err := cl.Put(ctx, "k1", "v1"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := cl.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || got != "v1" {
		t.Fatalf("Get = (%q,%v), want (v1,true)", got, ok)
	}
}
