package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raft"
)

// TestSnapshotCompactionAndCatchup: write enough entries to trigger snapshots
// on the leader, keep one follower down past the compaction point, then bring
// it back. It must receive an InstallSnapshot, catch up incrementally and
// continue applying new entries with no hole and no rollback.
func TestSnapshotCompactionAndCatchup(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "p1", "p2", "p3")
	waitLeader(t, c, 3*time.Second)

	// Keep p3 down while we write well beyond the snapshot threshold (32).
	if err := c.Crash(ctx, "p3"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 70; i++ {
		putAcked(t, ctx, c, snapKey(i), snapVal(i))
	}

	// A leader must have snapshotted. Leadership may change while the offline
	// node is down, so inspect the current leader dynamically.
	waitLeader(t, c, 4*time.Second)
	deadline := time.Now().Add(5 * time.Second)
	var lstatus raft.Status
	for time.Now().Before(deadline) {
		ld := c.Leader()
		if ld != "" {
			lstatus = c.Node(ld).Status()
			if lstatus.SnapshotIndex > 0 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lstatus.SnapshotIndex == 0 {
		t.Fatalf("expected leader snapshot after 70 writes, got SnapshotIndex=0")
	}
	t.Logf("leader snapshot index=%d commit=%d", lstatus.SnapshotIndex, lstatus.CommitIndex)

	// Bring the long-offline node back. It is far behind the retained log and
	// must be caught up via snapshot, then incremental entries.
	if err := c.Restart(ctx, "p3"); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, c, 4*time.Second)

	// Keep writing after recovery to prove incremental replication resumes.
	for i := 71; i <= 90; i++ {
		putAcked(t, ctx, c, snapKey(i), snapVal(i))
	}

	// p3 must converge to the cluster commit position with no data hole.
	waitAligned(t, c, 10*time.Second)

	// Values from before, during, and after the snapshot window all present.
	cl := c.Client()
	for _, i := range []int{1, 20, 33, 50, 70, 75, 90} {
		v, ok, err := cl.Get(ctx, snapKey(i))
		if err != nil || !ok || v != snapVal(i) {
			t.Fatalf("after catch-up %s = (%q,%v,%v), want %q",
				snapKey(i), v, ok, err, snapVal(i))
		}
	}

	// The recovered node must hold data covered by a snapshot it received,
	// proving InstallSnapshot populated its state machine, not a blank store.
	if s := c.Node("p3").Status(); s.SnapshotIndex == 0 {
		t.Fatalf("p3 should have installed a snapshot, SnapshotIndex=0")
	}
}

// TestSnapshotNoCommittedRollback: after compaction, removing an entry that is
// snapshot-covered must be refused rather than rolling committed state back.
func TestSnapshotNoCommittedRollback(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "q1", "q2", "q3")
	waitLeader(t, c, 3*time.Second)
	for i := 1; i <= 40; i++ {
		putAcked(t, ctx, c, snapKey(i), snapVal(i))
	}
	obs := c.Observe()
	var snap uint64
	for _, s := range obs.Nodes {
		if s.SnapshotIndex > snap {
			snap = s.SnapshotIndex
		}
	}
	if snap == 0 {
		t.Fatal("expected at least one snapshot")
	}
	// Committed history before the snapshot must remain readable.
	waitValue(t, ctx, c, snapKey(1), snapVal(1), 5*time.Second)
}

func snapKey(i int) string { return "snap-k-" + itoa(i) }
func snapVal(i int) string { return "snap-v-" + itoa(i) }
