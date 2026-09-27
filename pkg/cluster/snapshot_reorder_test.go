package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// TestDuplicateSnapshotMustNotRollback drives a follower through snapshot
// catch-up, then delivers a DUPLICATE InstallSnapshot carrying the same
// boundary index after the follower has already applied later entries — the
// exact reordered/delayed-RPC condition found by the failure drills. The
// equal-index snapshot must be treated as idempotent and must never
// Restore-wipe entries applied after the boundary.
func TestDuplicateSnapshotMustNotRollback(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "z1", "z2", "z3")
	waitLeader(t, c, 3*time.Second)
	leader := c.Leader()

	if err := c.Crash(ctx, "z3"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 70; i++ {
		putAcked(t, ctx, c, snapKey(i), snapVal(i))
	}
	if err := c.Restart(ctx, "z3"); err != nil {
		t.Fatal(err)
	}
	for i := 71; i <= 90; i++ {
		putAcked(t, ctx, c, snapKey(i), snapVal(i))
	}
	waitAligned(t, c, 10*time.Second)

	// z3 now holds snapshot at boundary B and applied entries B+1..90 on top.
	z3 := c.Node("z3")
	st := z3.Status()
	boundary := st.SnapshotIndex
	if boundary == 0 {
		t.Fatal("z3 did not install a snapshot")
	}
	appliedAfter := st.LastApplied
	if appliedAfter <= boundary {
		t.Fatalf("expected applied %d well past snapshot %d", appliedAfter, boundary)
	}

	// Obtain the leader's snapshot bytes for that boundary and redeliver the
	// identical snapshot to z3 directly (a duplicate, same-index RPC).
	snapReq := buildSnapshotAt(t, c, leader, boundary)
	if _, err := z3.HandleInstallSnapshot(ctx, leader, snapReq); err != nil {
		t.Fatalf("duplicate InstallSnapshot: %v", err)
	}
	// Give the loop a moment to process any resulting apply.
	time.Sleep(100 * time.Millisecond)

	// Inspect z3's LOCAL applied state (not a read-index query, which the
	// healthy leader would answer): the duplicate must not have wiped keys.
	after := z3.Status()
	if after.LastApplied < appliedAfter {
		t.Fatalf("applied rolled back %d -> %d after duplicate snapshot",
			appliedAfter, after.LastApplied)
	}
	kv, ok := c.AppliedKV("z3")
	if !ok {
		t.Fatal("cannot read z3 applied kv")
	}
	if len(kv) != 90 {
		t.Fatalf("duplicate snapshot changed z3 history: %d keys want 90", len(kv))
	}
	for _, i := range []int{63, 64, 65, 70, 73, 80, 90} {
		if v, have := kv[snapKey(i)]; !have || v != snapVal(i) {
			t.Fatalf("duplicate snapshot lost/changed %s on z3: got %q have=%v want %q",
				snapKey(i), v, have, snapVal(i))
		}
	}
	if z3.Status().StateDigest != c.Node(leader).Status().StateDigest {
		t.Fatal("z3 applied-history digest diverged from leader after duplicate snapshot")
	}
}

// buildSnapshotAt asks the current leader's transport to expose the snapshot
// by constructing an InstallSnapshot request for the boundary index the
// follower already installed. The simulator stores the exact request the
// leader sends; reconstruct an equivalent request by reading the leader's
// own snapshot store through its replicated status is not possible, so we
// capture it from a fresh catch-up instead: this helper returns the request
// the leader would send by snapshotting the leader node directly.
func buildSnapshotAt(t *testing.T, c *Cluster, leaderID string, boundary uint64) *raftproto.InstallSnapshotRequest {
	t.Helper()
	// Faithful reproduction of a reordered duplicate RPC: use the exact
	// snapshot bytes the follower already installed (identical to the bytes
	// the leader originally sent for that boundary).
	data, meta, ok := c.SnapshotBytes("z3")
	if !ok {
		t.Fatal("z3 snapshot unavailable")
	}
	if meta.Index != boundary {
		t.Fatalf("z3 snapshot index %d != boundary %d", meta.Index, boundary)
	}
	return &raftproto.InstallSnapshotRequest{
		Term:              meta.Term,
		LeaderID:          leaderID,
		LastIncludedIndex: meta.Index,
		LastIncludedTerm:  meta.Term,
		Data:              data,
		Config:            meta.Config,
		Done:              true,
	}
}
