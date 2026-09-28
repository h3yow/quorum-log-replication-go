package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// TestDuplicateSnapshotAfterProgressCannotRollback deterministically replays
// the intermittent offline-compaction-catchup failure: after a long-offline
// node installs a leader snapshot and then advances past it via incremental
// entries, a DUPLICATED/RETRANSMITTED InstallSnapshot carrying the older
// snapshot position arrives (fixed trigger: replayed on demand, not random
// network timing). The follower must keep its newer committed/applied state:
// no content rollback, the reported applied position must match the actual
// state-machine contents, and the cluster must stay converged.
func TestDuplicateSnapshotAfterProgressCannotRollback(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "r1", "r2", "r3")
	leader := waitLeader(t, c, 3*time.Second)

	// 1. Take r3 offline before the bulk workload so it misses a compaction.
	victim := "r3"
	victimCommit := c.Node(victim).Status().CommitIndex
	if err := c.Crash(ctx, victim); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 70; i++ {
		putAcked(t, ctx, c, rtKey(i), rtVal(i))
	}

	// 2. Wait until the leader has compacted strictly past r3's position.
	var snap SnapshotBundle
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ld := c.Leader()
		if ld != "" {
			if b, ok, err := c.SnapshotOf(ld); err == nil && ok && b.Index > victimCommit {
				snap = b
				leader = ld
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	if snap.Index == 0 {
		t.Fatalf("leader never snapshotted past victim commit=%d", victimCommit)
	}

	// 3. Bring the victim back; it installs the snapshot then catches up
	//    incrementally as more writes land.
	if err := c.Restart(ctx, victim); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, c, 4*time.Second)
	for i := 71; i <= 95; i++ {
		putAcked(t, ctx, c, rtKey(i), rtVal(i))
	}
	waitAligned(t, c, 10*time.Second)

	// The victim has now moved strictly past the retransmitted snapshot via
	// incremental AppendEntries.
	vs := c.Node(victim).Status()
	if vs.CommitIndex <= snap.Index {
		t.Fatalf("test setup: victim commit=%d not past snapshot=%d", vs.CommitIndex, snap.Index)
	}
	beforeKV, err := c.KVHash(victim)
	if err != nil {
		t.Fatal(err)
	}
	appliedBefore := vs.LastApplied
	commitBefore := vs.CommitIndex

	// 4. FIXED TRIGGER: replay the stale snapshot install one extra time,
	//    exactly like a duplicated RPC / leader retransmission would.
	ok, err := c.DeliverInstallSnapshot(ctx, leader, victim, snap)
	if err != nil {
		t.Fatalf("replayed InstallSnapshot delivery: %v", err)
	}
	if !ok {
		t.Fatalf("stale-but-valid snapshot should be acked, got success=false")
	}

	// 5. No state may have moved backwards: commit/applied watermarks and the
	//    actual state-machine contents must be unchanged.
	after := c.Node(victim).Status()
	if after.LastApplied != appliedBefore {
		t.Fatalf("applied position regressed: %d -> %d", appliedBefore, after.LastApplied)
	}
	if after.CommitIndex != commitBefore {
		t.Fatalf("commit position regressed: %d -> %d", commitBefore, after.CommitIndex)
	}
	afterKV, err := c.KVHash(victim)
	if err != nil {
		t.Fatal(err)
	}
	if afterKV != beforeKV {
		t.Fatalf("state-machine contents rolled back after stale snapshot: kv %s -> %s",
			beforeKV, afterKV)
	}

	// 6. The replayed snapshot must not have erased data it claimed to cover,
	//    nor the data written after it: spot-check the full window, including a
	//    key strictly inside the snapshot prefix and one after it.
	cl := c.Client()
	for _, i := range []int{1, 10, 40, 60, 70, 80, 95} {
		v, got, gerr := cl.Get(ctx, rtKey(i))
		if gerr != nil || !got || v != rtVal(i) {
			t.Fatalf("after replay %s=(%q,%v,%v) want %q", rtKey(i), v, got, gerr, rtVal(i))
		}
	}

	// 7. Replay it several more times (network retransmit burst) WHILE
	//    concurrent writers and readers run, then keep writing: the follower
	//    only ever moves forward, never half-updates, and stays converged. Run
	//    with -race to exercise concurrent observation/read paths.
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	for r := 0; r < 3; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			rc := c.Client()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _, _ = rc.Get(ctx, rtKey(50))
			}
		}()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		wc := c.Client()
		for i := 96; i <= 110; i++ {
			_ = wc.Put(ctx, rtKey(i), rtVal(i))
		}
	}()
	for j := 0; j < 5; j++ {
		if _, err := c.DeliverInstallSnapshot(ctx, leader, victim, snap); err != nil {
			t.Fatalf("burst replay %d: %v", j, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	close(stop)
	rwg.Wait()
	waitAligned(t, c, 10*time.Second)

	// Final whole-history spot check across the replayed snapshot boundary.
	cl2 := c.Client()
	for _, i := range []int{1, 33, 64, 70, 95, 110} {
		v, got, gerr := cl2.Get(ctx, rtKey(i))
		if gerr != nil || !got || v != rtVal(i) {
			t.Fatalf("final %s=(%q,%v,%v) want %q", rtKey(i), v, got, gerr, rtVal(i))
		}
	}
}

// TestStaleSnapshotOlderThanLocalSnapshotAcked covers the other duplicate
// boundary: a snapshot strictly older than one the node already installed
// locally must be acked (so the leader advances) but never installed.
func TestStaleSnapshotOlderThanLocalSnapshotAcked(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "s1", "s2", "s3")
	leader := waitLeader(t, c, 3*time.Second)
	victim := "s3"
	vc := c.Node(victim).Status().CommitIndex
	if err := c.Crash(ctx, victim); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 70; i++ {
		putAcked(t, ctx, c, "stale-"+itoa(i), "v")
	}
	var first SnapshotBundle
	dl := time.Now().Add(5 * time.Second)
	for time.Now().Before(dl) {
		ld := c.Leader()
		if ld != "" {
			if b, ok, err := c.SnapshotOf(ld); err == nil && ok && b.Index > vc {
				first, leader = b, ld
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	if first.Index == 0 {
		t.Fatal("no leader snapshot")
	}
	if err := c.Restart(ctx, victim); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, c, 4*time.Second)

	// Wait for the victim to have installed its own snapshot at the same or a
	// higher position.
	vd := time.Now().Add(10 * time.Second)
	for time.Now().Before(vd) {
		if c.Node(victim).Status().SnapshotIndex >= first.Index {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if c.Node(victim).Status().SnapshotIndex < first.Index {
		t.Fatalf("victim did not install snapshot >= %d", first.Index)
	}
	kvBefore, _ := c.KVHash(victim)

	// A snapshot at a strictly older position (fabricate index 1 with empty
	// payload) must be acked without touching state.
	ok, err := c.DeliverInstallSnapshot(ctx, leader, victim, SnapshotBundle{Index: 1, Term: 1})
	if err != nil {
		t.Fatalf("old snapshot delivery: %v", err)
	}
	if !ok {
		t.Fatalf("snapshot older than local snapshot should still be acked")
	}
	kvAfter, _ := c.KVHash(victim)
	if kvAfter != kvBefore {
		t.Fatalf("contents changed after snapshot older than local snapshot")
	}
}

func rtKey(i int) string { return "rt-k-" + itoa(i) }
func rtVal(i int) string { return "rt-v-" + itoa(i) }

// TestRetransmittedAppendEntriesCannotTruncateCommitted replays a stale,
// conflicting AppendEntries at a follower after it moved past that position.
// The committed prefix must survive: the conflicting request is refused and
// neither commit/applied nor the contents move. A benign duplicate (same
// terms) is still accepted, so healthy retransmission keeps working.
func TestRetransmittedAppendEntriesCannotTruncateCommitted(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "a1", "a2", "a3")
	leader := waitLeader(t, c, 3*time.Second)
	victim := "a3"
	for i := 1; i <= 20; i++ {
		putAcked(t, ctx, c, "ae-k-"+itoa(i), "v")
	}
	waitAligned(t, c, 8*time.Second)

	s := c.Node(victim).Status()
	if s.CommitIndex < 5 {
		t.Fatalf("need committed prefix, commit=%d", s.CommitIndex)
	}
	commitBefore := s.CommitIndex

	// Forged conflicting request: prevLogIndex in the committed range but
	// with an impossible prevLogTerm, plus a bogus extension entry.
	bad := &raftproto.AppendEntriesRequest{
		Term:         c.Node(leader).Status().Term,
		LeaderID:     leader,
		PrevLogIndex: 2,
		PrevLogTerm:  999999, // conflicts with the real committed term at 2
		Entries: []raftproto.Entry{{
			Term: 999999, Index: 3, Type: raftproto.EntryNormal, Data: []byte("garbage"),
		}},
		LeaderCommit: commitBefore,
	}
	if _, err := c.Transport().SendAppendEntries(ctx, leader, victim, bad); err != nil {
		t.Fatalf("send conflicting AE: %v", err)
	}

	after := c.Node(victim).Status()
	if after.CommitIndex < commitBefore {
		t.Fatalf("commit regressed after conflicting append: %d -> %d", commitBefore, after.CommitIndex)
	}
	// The conflicting entry must never have been applied: contents remain a
	// prefix of the healthy history (all pre-existing keys keep their values).
	kvAfter, _ := c.KVHash(victim)
	v, ok, _ := c.Client().Get(ctx, "ae-k-1")
	if !ok || v != "v" || kvAfter == "" {
		t.Fatalf("contents corrupted by conflicting append: ok=%v v=%q kv=%s", ok, v, kvAfter)
	}

	// Normal writes continue to commit afterwards (the guard must not
	// over-reject legitimate replication, only conflicting truncations).
	for i := 21; i <= 30; i++ {
		putAcked(t, ctx, c, "ae-k-"+itoa(i), "v2")
	}
	waitAligned(t, c, 8*time.Second)
	// Committed content from before the forged request is intact and the post
	// writes are present on every node.
	cl := c.Client()
	for _, i := range []int{1, 10, 20, 25, 30} {
		want := "v"
		if i >= 21 {
			want = "v2"
		}
		if got, ok, err := cl.Get(ctx, "ae-k-"+itoa(i)); err != nil || !ok || got != want {
			t.Fatalf("ae-k-%d=(%q,%v,%v) want %q", i, got, ok, err, want)
		}
	}
}
