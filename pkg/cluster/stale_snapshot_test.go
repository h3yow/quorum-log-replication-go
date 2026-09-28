package cluster

import (
	"context"
	"testing"
	"time"
)

// runCatchupWithDuplicateSnapshot executes the shared deterministic scenario
// and returns the victim's status right before and after a duplicate replay of
// a leader snapshot it already installed (after advancing beyond it).
func runCatchupWithDuplicateSnapshot(t *testing.T, ids []string, keyfn func(int) string, valfn func(int) string, bulk, post int) (before, after statusPlus) {
	t.Helper()
	ctx := context.Background()
	c := bootCluster(t, ctx, ids...)
	leader := waitLeader(t, c, 3*time.Second)
	victim := ""
	for _, id := range ids {
		if id != leader {
			victim = id
			break
		}
	}

	if err := c.Crash(ctx, victim); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= bulk; i++ {
		putAcked(t, ctx, c, keyfn(i), valfn(i))
	}
	waitLeader(t, c, 4*time.Second)
	snapIdx := uint64(0)
	waitFor(t, 5*time.Second, func() bool {
		if ld := c.Leader(); ld != "" {
			snapIdx = c.Node(ld).Status().SnapshotIndex
			return snapIdx > 0
		}
		return false
	})

	if err := c.Restart(ctx, victim); err != nil {
		t.Fatal(err)
	}
	// Restart re-registers the real handler; attach the interceptor after it.
	interceptor, err := c.InterceptSnapshots(victim)
	if err != nil {
		t.Fatal(err)
	}
	waitLeader(t, c, 4*time.Second)

	// The victim must really install at least one leader snapshot to cross the
	// compaction boundary.
	waitFor(t, 8*time.Second, func() bool {
		_, n := interceptor.Observed()
		return n > 0
	})
	firstSnap, _ := interceptor.OldestIndex()

	// Continue writing so the victim advances well beyond the snapshot it
	// installed, via ordinary incremental AppendEntries.
	for i := bulk + 1; i <= bulk+post; i++ {
		putAcked(t, ctx, c, keyfn(i), valfn(i))
	}
	waitAligned(t, c, 10*time.Second)

	st := c.Node(victim).Status()
	if st.LastApplied <= firstSnap {
		t.Fatalf("setup: victim applied %d not beyond installed snapshot %d",
			st.LastApplied, firstSnap)
	}
	kh, err := c.KVHash(victim)
	if err != nil {
		t.Fatal(err)
	}
	before = statusPlus{applied: st.LastApplied, commit: st.CommitIndex, snap: st.SnapshotIndex, kv: kh}
	t.Logf("victim=%s before replay applied=%d commit=%d snap=%d firstSnap=%d",
		victim, st.LastApplied, st.CommitIndex, st.SnapshotIndex, firstSnap)

	// Deterministic duplicate delivery of a snapshot the node already
	// installed, after it advanced beyond the snapshot contents. Replay BOTH
	// the oldest (stale boundary) and newest (just-installed) snapshots to
	// cover the two retransmission shapes.
	if err := interceptor.ReplayOldest(ctx); err != nil {
		t.Fatalf("replay oldest: %v", err)
	}
	if err := interceptor.ReplayNewest(ctx); err != nil {
		t.Fatalf("replay newest: %v", err)
	}

	st2 := c.Node(victim).Status()
	kh2, err := c.KVHash(victim)
	if err != nil {
		t.Fatal(err)
	}
	after = statusPlus{applied: st2.LastApplied, commit: st2.CommitIndex, snap: st2.SnapshotIndex, kv: kh2}

	// The reported applied position must match the real saved contents across
	// the whole offline/catch-up window.
	cl := c.Client()
	for _, i := range []int{1, bulk / 2, bulk, bulk + 1, bulk + post} {
		v, ok, gerr := cl.Get(ctx, keyfn(i))
		if gerr != nil || !ok || v != valfn(i) {
			t.Fatalf("after duplicate snapshot replay %s=(%q,%v,%v) want %q",
				keyfn(i), v, ok, gerr, valfn(i))
		}
	}
	waitAligned(t, c, 5*time.Second)
	return before, after
}

type statusPlus struct {
	applied uint64
	commit  uint64
	snap    uint64
	kv      string
}

// TestDuplicateSnapshotAfterAdvanceNoRollback deterministically reproduces the
// offline-compaction catch-up flake: a long-offline node installs a leader
// snapshot, advances well beyond it through incremental entries, and then
// receives duplicates of catch-up snapshots (retransmission/redelivery). Its
// applied position and state-machine contents must not roll back, and the
// reported applied index must keep matching the saved contents.
func TestDuplicateSnapshotAfterAdvanceNoRollback(t *testing.T) {
	ids := []string{"d1", "d2", "d3"}
	key := func(i int) string { return "dup-k-" + itoa(i) }
	val := func(i int) string { return "dup-v-" + itoa(i) }
	before, after := runCatchupWithDuplicateSnapshot(t, ids, key, val, 60, 40)
	if after.applied != before.applied || after.commit != before.commit {
		t.Fatalf("state rolled back after duplicate snapshot: before applied=%d commit=%d; after applied=%d commit=%d",
			before.applied, before.commit, after.applied, after.commit)
	}
	if after.kv != before.kv {
		t.Fatalf("state machine contents changed after duplicate snapshot replay:\nbefore=%s\nafter =%s",
			before.kv, after.kv)
	}
}

// TestDuplicateSnapshotRepeatableSameInput runs the exact same fixed scenario
// repeatedly and requires (a) no rollback within every run and (b) identical
// final applied position and KV contents across runs: the conclusion must not
// depend on scheduler/election luck.
func TestDuplicateSnapshotRepeatableSameInput(t *testing.T) {
	run := func() (statusPlus, statusPlus) {
		ids := []string{"z1", "z2", "z3"}
		key := func(i int) string { return "fix-k-" + itoa(i) }
		val := func(i int) string { return "fix-v-" + itoa(i) }
		return runCatchupWithDuplicateSnapshot(t, ids, key, val, 60, 40)
	}
	var firstAfter statusPlus
	for iter := 0; iter < 3; iter++ {
		b, a := run()
		if a.applied != b.applied || a.commit != b.commit || a.kv != b.kv {
			t.Fatalf("iteration %d rolled back: before applied=%d kv=%s; after applied=%d kv=%s",
				iter, b.applied, b.kv, a.applied, a.kv)
		}
		if iter == 0 {
			firstAfter = a
			continue
		}
		if a.applied != firstAfter.applied || a.commit != firstAfter.commit || a.kv != firstAfter.kv {
			t.Fatalf("iteration %d diverged from first: applied %d/%d commit %d/%d kv %s/%s",
				iter, a.applied, firstAfter.applied, a.commit, firstAfter.commit, a.kv, firstAfter.kv)
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
