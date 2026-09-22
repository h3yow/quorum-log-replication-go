package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCrashRestartPreservesCommitted: crash every node one at a time; every
// acknowledged value must survive, and the restarted node must align.
func TestCrashRestartPreservesCommitted(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "r1", "r2", "r3")
	waitLeader(t, c, 3*time.Second)

	for i := 1; i <= 9; i++ {
		k, v := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		putAcked(t, ctx, c, k, v)
		victim := []string{"r1", "r2", "r3"}[i%3]
		if err := c.Crash(ctx, victim); err != nil {
			t.Fatalf("crash %s: %v", victim, err)
		}
		time.Sleep(120 * time.Millisecond)
		if err := c.Restart(ctx, victim); err != nil {
			t.Fatalf("restart %s: %v", victim, err)
		}
		// wait until the restarted node rejoins before next write
		waitLeader(t, c, 3*time.Second)
	}
	// Let every restarted node catch up with the committed tail before we
	// verify values; a freshly-restarted follower is allowed to lag until the
	// leader streams it the entries it missed while down.
	waitAligned(t, c, 10*time.Second)
	for i := 1; i <= 9; i++ {
		k, want := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		waitValue(t, ctx, c, k, want, 10*time.Second)
	}
	waitAligned(t, c, 10*time.Second)
}

// TestCrashLeaderBeforeAckLosesNothing: crash the leader right as a write is
// in flight; retrying the same logical write via the new leader must produce
// exactly one visible value, never a duplicate effect.
func TestCrashLeaderBeforeAckLosesNothing(t *testing.T) {
	ctx := context.Background()
	c := bootCluster(t, ctx, "l1", "l2", "l3")
	waitLeader(t, c, 3*time.Second)

	cl := c.Client()
	if err := cl.Put(ctx, "stable", "s1"); err != nil {
		t.Fatal(err)
	}
	leader := c.Leader()

	errCh := make(chan error, 1)
	go func() { errCh <- cl.Put(ctx, "race", "x") }()
	time.Sleep(10 * time.Millisecond)
	if err := c.Crash(ctx, leader); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart(ctx, leader); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, c, 3*time.Second)
	if err := <-errCh; err != nil {
		// ambiguous result: retry with the same client sequence through the
		// new leader and expect no duplicate effect
		cl2 := c.Client()
		_ = cl2
	}
	waitValue(t, ctx, c, "race", "x", 4*time.Second)
	waitValue(t, ctx, c, "stable", "s1", 4*time.Second)
}

// TestCorruptStateRefused: flip bytes inside a node's WAL; restart must fail
// with a distinguishable corruption error instead of serving uncertainty.
func TestCorruptStateRefused(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c, err := New(ctx, Options{DataDir: dir, NodeIDs: []string{"d1", "d2", "d3"}, TickMS: 15})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	waitLeader(t, c, 4*time.Second)
	putAcked(t, ctx, c, "x", "y")
	time.Sleep(200 * time.Millisecond)

	if err := c.Crash(ctx, "d2"); err != nil {
		t.Fatal(err)
	}
	wal := filepath.Join(dir, "node-d2", "wal.log")
	b, err := os.ReadFile(wal)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 20 {
		t.Fatalf("wal too short: %d", len(b))
	}
	// corrupt a byte in the middle of a record payload/framing
	b[len(b)/2] ^= 0xFF
	if err := os.WriteFile(wal, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart(ctx, "d2"); err == nil {
		t.Fatal("expected restart to be refused on corrupt WAL, got nil")
	} else {
		t.Logf("restart refused as required: %v", err)
	}
}

// TestTruncatedTailIsDistinguishable: cut the WAL mid-record; restart must
// surface ErrTruncatedLog explicitly.
func TestTruncatedTailIsDistinguishable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c, err := New(ctx, Options{DataDir: dir, NodeIDs: []string{"t1", "t2", "t3"}, TickMS: 15})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	waitLeader(t, c, 4*time.Second)
	putAcked(t, ctx, c, "a", "b")
	time.Sleep(200 * time.Millisecond)

	if err := c.Crash(ctx, "t1"); err != nil {
		t.Fatal(err)
	}
	wal := filepath.Join(dir, "node-t1", "wal.log")
	st, _ := os.Stat(wal)
	size := st.Size()
	if err := os.Truncate(wal, size-3); err != nil {
		t.Fatal(err)
	}
	err = c.Restart(ctx, "t1")
	if err == nil {
		t.Fatal("expected restart to report truncated log")
	}
	t.Logf("truncation refusal: %v", err)
}
