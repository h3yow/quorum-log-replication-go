package drill

import (
	"context"
	"testing"

	"github.com/h3yow/quorum-log-replication-go/pkg/cluster"
)

// noopFaults performs NO fault at all: a correct drill must FAIL because its
// evidence checks prove the fault never took effect, rather than passing just
// because the scripted steps executed.
type noopFaults struct{}

func (noopFaults) Crash(context.Context, string) error   { return nil }
func (noopFaults) Restart(context.Context, string) error { return nil }
func (noopFaults) Partition(string, ...string)           {}
func (noopFaults) HealAll()                              {}

func runWithFaults(t *testing.T, s Scenario, f Faults) *Report {
	t.Helper()
	r, err := NewRunner(s, f)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := r.Run(context.Background(), Options{
		DataDir: t.TempDir(),
		Input:   DefaultInput(s),
		TickMS:  15,
	})
	if err != nil {
		t.Fatalf("Run returned infrastructure error: %v (drill should report FAIL in-report, not error)", err)
	}
	return rep
}

func failedCheck(rep *Report, id string) bool {
	for _, c := range rep.Checks {
		if c.ID == id {
			return !c.Passed
		}
	}
	return false
}

// TestNoFaultMajorityDrillFails: when the "majority crash" does nothing, the
// drill must fail on injection/effect evidence, never pass.
func TestNoFaultMajorityDrillFails(t *testing.T) {
	rep := runWithFaults(t, ScenarioMajorityDown, noopFaults{})
	if rep.Passed {
		t.Fatalf("drill PASSED although the majority was never made unreachable")
	}
	if !failedCheck(rep, "fault-majority-injected") {
		t.Fatalf("expected fault-majority-injected to fail when crash is a no-op")
	}
	if !failedCheck(rep, "leader-cannot-reach-quorum") {
		t.Fatalf("expected leader-cannot-reach-quorum to fail without a real partition")
	}
}

// TestNoFaultLeaderCrashDrillFails: when the leader is never crashed the
// drill must fail on crash/failover evidence.
func TestNoFaultLeaderCrashDrillFails(t *testing.T) {
	rep := runWithFaults(t, ScenarioLeaderCrashRetry, noopFaults{})
	if rep.Passed {
		t.Fatalf("drill PASSED although the leader was never crashed")
	}
	if !failedCheck(rep, "fault-leader-crashed") {
		t.Fatalf("expected fault-leader-crashed to fail when crash is a no-op")
	}
	if !failedCheck(rep, "new-leader-after-crash") {
		t.Fatalf("expected new-leader-after-crash to fail when no failover happened")
	}
}

// TestNoFaultSnapshotDrillFails: a node that never goes offline cannot cross a
// compaction boundary, so the drill must fail on offline/crossed evidence.
func TestNoFaultSnapshotDrillFails(t *testing.T) {
	rep := runWithFaults(t, ScenarioSnapshotCatchup, noopFaults{})
	if rep.Passed {
		t.Fatalf("drill PASSED although the lagging node never went offline")
	}
	if !failedCheck(rep, "fault-node-offline") {
		t.Fatalf("expected fault-node-offline to fail when crash is a no-op")
	}
}

// TestNoFaultCatchupDuplicateDrillFails: when the victim is never taken
// offline it never crosses a compaction boundary, so the duplicate-catch-up
// drill must fail on its offline/crossed evidence rather than pass on steps.
func TestNoFaultCatchupDuplicateDrillFails(t *testing.T) {
	rep := runWithFaults(t, ScenarioCatchupDuplicateSnapshot, noopFaults{})
	if rep.Passed {
		t.Fatalf("drill PASSED although the catch-up node never went offline")
	}
	if !failedCheck(rep, "fault-node-offline") {
		t.Fatalf("expected fault-node-offline to fail when crash is a no-op")
	}
}

// TestNoFaultMembershipDrillFails: with no leader crash the claimed switch
// cannot be evidenced and the drill must fail.
func TestNoFaultMembershipDrillFails(t *testing.T) {
	rep := runWithFaults(t, ScenarioMembershipLeaderSwitch, noopFaults{})
	if rep.Passed {
		t.Fatalf("drill PASSED although no leadership switch occurred")
	}
	if !failedCheck(rep, "fault-leader-crashed") {
		t.Fatalf("expected fault-leader-crashed to fail when crash is a no-op")
	}
	if !failedCheck(rep, "leadership-switched") {
		t.Fatalf("expected leadership-switched to fail when nothing switched")
	}
}

// singleNodeFaults maps every requested crash/restart onto one fixed node and
// performs no partitions. In the majority scenario that means the fault SCOPE
// is insufficient (1 of 5, not a majority); the drill must still fail on its
// quorum-loss evidence rather than report success.
type singleNodeFaults struct {
	c      *cluster.Cluster
	target string
}

func (f *singleNodeFaults) Crash(ctx context.Context, id string) error {
	return f.c.Crash(ctx, f.target)
}
func (f *singleNodeFaults) Restart(ctx context.Context, id string) error {
	return f.c.Restart(ctx, f.target)
}
func (f *singleNodeFaults) Partition(string, ...string) {}
func (f *singleNodeFaults) HealAll()                    {}

// crashButNoRestartFaults crashes for real but makes Restart a no-op: the node
// genuinely goes offline and the cluster compacts, but it never returns and so
// can never install a snapshot or be caught up. The duplicate-catch-up drill
// must FAIL on recovery/install evidence, proving it does not pass merely
// because the offline/compaction steps executed.
type crashButNoRestartFaults struct{ c *cluster.Cluster }

func (f *crashButNoRestartFaults) Crash(ctx context.Context, id string) error {
	return f.c.Crash(ctx, id)
}
func (f *crashButNoRestartFaults) Restart(context.Context, string) error { return nil }
func (f *crashButNoRestartFaults) Partition(string, ...string)           {}
func (f *crashButNoRestartFaults) HealAll()                              {}

// TestNoRecoveryCatchupDuplicateDrillFails: a victim that is crashed but never
// restarted cannot install a snapshot, so the drill must fail on the recovery
// and snapshot-install evidence.
func TestNoRecoveryCatchupDuplicateDrillFails(t *testing.T) {
	r, err := NewRunner(ScenarioCatchupDuplicateSnapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.SetFaultFactory(func(c *cluster.Cluster) Faults {
		return &crashButNoRestartFaults{c: c}
	})
	rep, err := r.Run(context.Background(), Options{
		DataDir: t.TempDir(),
		Input:   DefaultInput(ScenarioCatchupDuplicateSnapshot),
		TickMS:  15,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Passed {
		t.Fatalf("drill PASSED although the victim never restarted/caught up")
	}
	if !failedCheck(rep, "fault-node-recovered") {
		t.Fatalf("expected fault-node-recovered to fail when Restart is a no-op")
	}
}

// TestInsufficientFaultScopeFails uses a factory-bound fault set that only
// ever takes one node down, proving insufficient fault scope cannot pass.
func TestInsufficientFaultScopeFails(t *testing.T) {
	r, err := NewRunner(ScenarioMajorityDown, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.SetFaultFactory(func(c *cluster.Cluster) Faults {
		return &singleNodeFaults{c: c, target: "n1"}
	})
	rep, err := r.Run(context.Background(), Options{
		DataDir: t.TempDir(),
		Input:   DefaultInput(ScenarioMajorityDown),
		TickMS:  15,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Passed {
		t.Fatalf("drill PASSED although only one node (not a majority) went down")
	}
	if !failedCheck(rep, "fault-majority-injected") {
		t.Fatalf("expected fault-majority-injected to fail when only one node is down")
	}
}
