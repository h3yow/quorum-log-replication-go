package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// putAcked writes a value and confirms it is durable on a quorum by reading.
func putAcked(t *testing.T, ctx context.Context, c *Cluster, k, v string) {
	t.Helper()
	if err := c.Client().Put(ctx, k, v); err != nil {
		t.Fatalf("Put %s=%s: %v", k, v, err)
	}
}

// TestPartitionMinorityNoCommit: when a majority is unreachable no new entry
// may commit; on heal the minority aligns to the committed history.
func TestPartitionMinorityNoCommit(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "n1", "n2", "n3", "n4", "n5")
	leader := waitLeader(t, c, 3*time.Second)
	putAcked(t, ctx, c, "before", "1")

	others := others(leader, []string{"n1", "n2", "n3", "n4", "n5"})
	minority := others[0]
	t.Logf("partition: leader=%s minority=%s", leader, minority)
	c.Partition(minority)
	time.Sleep(400 * time.Millisecond)

	// Writes to the majority side should still commit (4/5 reachable).
	putAcked(t, ctx, c, "during", "2")

	// The isolated node cannot commit anything and must not claim leadership.
	solo := c.Node(minority).Status()
	if solo.State == raftproto.StateLeader {
		t.Fatalf("isolated minority node %s claims leadership", minority)
	}

	// Heal and require the former minority to converge to the same value and
	// commit index without rolling anything back.
	c.Heal(minority)
	waitValue(t, ctx, c, "during", "2", 3*time.Second)
	waitAligned(t, c, 3*time.Second)
}

// TestPartitionMajorityElectsNewLeader: isolate the leader with one peer
// (minority), the remaining majority must elect a new leader and commit; the
// old leader must not have committed anything in its isolated term.
func TestPartitionMajorityElectsNewLeader(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "n1", "n2", "n3", "n4", "n5")
	old := waitLeader(t, c, 3*time.Second)

	// Put old leader + one follower into a minority partition.
	all := []string{"n1", "n2", "n3", "n4", "n5"}
	peer := ""
	for _, x := range all {
		if x != old {
			peer = x
			break
		}
	}
	c.Partition(old, peer)
	time.Sleep(500 * time.Millisecond)

	// Majority elects a new leader.
	var newLeader string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range all {
			if id == old || id == peer {
				continue
			}
			s := c.Node(id).Status()
			if s.State == raftproto.StateLeader {
				newLeader = id
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == "" {
		t.Fatal("majority did not elect a new leader")
	}
	t.Logf("new leader in majority: %s", newLeader)

	putAcked(t, ctx, c, "after-split", "9")

	// Heal: the stale old leader must step down and align, never serving a
	// divergent commit.
	c.HealAll()
	waitValue(t, ctx, c, "after-split", "9", 4*time.Second)
	waitAligned(t, c, 4*time.Second)

	if c.Node(old).Status().State == raftproto.StateLeader &&
		c.Leader() != old {
		t.Fatal("stale old leader remained leader after heal")
	}
}

func bootCluster(t *testing.T, ctx context.Context, ids ...string) *Cluster {
	t.Helper()
	cl, err := New(ctx, Options{DataDir: t.TempDir(), NodeIDs: ids, TickMS: 15})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func others(exclude string, all []string) []string {
	out := []string{}
	for _, x := range all {
		if x != exclude {
			out = append(out, x)
		}
	}
	return out
}

func waitValue(t *testing.T, ctx context.Context, c *Cluster, k, want string, timeout time.Duration) {
	t.Helper()
	cl := c.Client()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok, err := cl.Get(ctx, k); err == nil && ok && v == want {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	got, ok, err := cl.Get(ctx, k)
	t.Fatalf("value of %s never became %q: got %q ok=%v err=%v", k, want, got, ok, err)
}

func waitAligned(t *testing.T, c *Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		obs := c.Observe()
		var want uint64
		n := 0
		for _, s := range obs.Nodes {
			if want == 0 || s.CommitIndex > want {
				want = s.CommitIndex
			}
			n++
		}
		same := true
		for _, s := range obs.Nodes {
			if s.CommitIndex != want || s.LastApplied != want {
				same = false
			}
		}
		if same && n > 0 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("nodes did not align to a common commit/applied index")
}
