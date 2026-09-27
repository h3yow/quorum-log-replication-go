package drill

import (
	"context"
	"testing"
	"time"
)

// runOne is a small test helper around the public Runner.
func runOne(t *testing.T, id ScenarioID, sabotage bool) *Report {
	t.Helper()
	r := NewRunner()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	reps, err := r.Run(ctx, Options{
		Scenarios: []ScenarioID{id},
		Sabotage:  map[ScenarioID]bool{id: sabotage},
	})
	if err != nil {
		t.Fatalf("run %s: %v", id, err)
	}
	if len(reps) != 1 {
		t.Fatalf("expected 1 report, got %d", len(reps))
	}
	return reps[0]
}

func TestScenarioMajorityUnreachablePass(t *testing.T) {
	rep := runOne(t, ScenarioMajorityUnreachable, false)
	if rep.Facts.Verdict != VerdictPass {
		for _, c := range rep.Facts.Checks {
			t.Logf("check %s pass=%v %s", c.Name, c.Pass, c.Detail)
		}
		t.Fatalf("verdict=%s want PASS", rep.Facts.Verdict)
	}
}

func TestScenarioMajorityUnreachableSabotageFails(t *testing.T) {
	rep := runOne(t, ScenarioMajorityUnreachable, true)
	if rep.Facts.Verdict != VerdictFail {
		t.Fatalf("sabotaged run verdict=%s want FAIL", rep.Facts.Verdict)
	}
}

func TestScenarioLeaderCrashDedupPass(t *testing.T) {
	rep := runOne(t, ScenarioLeaderCrashDedup, false)
	if rep.Facts.Verdict != VerdictPass {
		for _, c := range rep.Facts.Checks {
			t.Logf("check %s pass=%v %s", c.Name, c.Pass, c.Detail)
		}
		t.Fatalf("verdict=%s want PASS", rep.Facts.Verdict)
	}
}

func TestScenarioLeaderCrashDedupSabotageFails(t *testing.T) {
	rep := runOne(t, ScenarioLeaderCrashDedup, true)
	if rep.Facts.Verdict != VerdictFail {
		t.Fatalf("sabotaged run verdict=%s want FAIL", rep.Facts.Verdict)
	}
}

func TestScenarioSnapshotCatchupPass(t *testing.T) {
	rep := runOne(t, ScenarioSnapshotCatchup, false)
	if rep.Facts.Verdict != VerdictPass {
		for _, c := range rep.Facts.Checks {
			t.Logf("check %s pass=%v %s", c.Name, c.Pass, c.Detail)
		}
		t.Fatalf("verdict=%s want PASS", rep.Facts.Verdict)
	}
}

func TestScenarioSnapshotCatchupSabotageFails(t *testing.T) {
	rep := runOne(t, ScenarioSnapshotCatchup, true)
	if rep.Facts.Verdict != VerdictFail {
		t.Fatalf("sabotaged run verdict=%s want FAIL", rep.Facts.Verdict)
	}
}

func TestScenarioMembershipLeaderSwitchPass(t *testing.T) {
	rep := runOne(t, ScenarioMembershipLeaderSwitch, false)
	if rep.Facts.Verdict != VerdictPass {
		for _, c := range rep.Facts.Checks {
			t.Logf("check %s pass=%v %s", c.Name, c.Pass, c.Detail)
		}
		t.Fatalf("verdict=%s want PASS", rep.Facts.Verdict)
	}
}

func TestScenarioMembershipLeaderSwitchSabotageFails(t *testing.T) {
	rep := runOne(t, ScenarioMembershipLeaderSwitch, true)
	if rep.Facts.Verdict != VerdictFail {
		t.Fatalf("sabotaged run verdict=%s want FAIL", rep.Facts.Verdict)
	}
}

// TestRepeatabilitySameDigest runs every scenario twice with the same fixed
// input and requires byte-identical canonical fact digests and verdicts:
// historical facts must not vary, only timestamps/durations may.
func TestRepeatabilitySameDigest(t *testing.T) {
	for _, id := range AllScenarios() {
		id := id
		t.Run(string(id), func(t *testing.T) {
			r1 := runOne(t, id, false)
			r2 := runOne(t, id, false)
			if r1.Facts.Verdict != VerdictPass || r2.Facts.Verdict != VerdictPass {
				t.Fatalf("expected both PASS, got %s/%s", r1.Facts.Verdict, r2.Facts.Verdict)
			}
			if r1.Digest != r2.Digest {
				t.Fatalf("digest differs across repeat runs:\nrun1=%s\nrun2=%s", r1.Digest, r2.Digest)
			}
			if r1.Facts.HistoryDigest != r2.Facts.HistoryDigest {
				t.Fatalf("final history digest differs: %s vs %s",
					r1.Facts.HistoryDigest, r2.Facts.HistoryDigest)
			}
			if len(r1.Facts.Checks) != len(r2.Facts.Checks) {
				t.Fatalf("check count differs: %d vs %d",
					len(r1.Facts.Checks), len(r2.Facts.Checks))
			}
			for i := range r1.Facts.Checks {
				if r1.Facts.Checks[i].Name != r2.Facts.Checks[i].Name ||
					r1.Facts.Checks[i].Pass != r2.Facts.Checks[i].Pass {
					t.Fatalf("check %d judgment differs: %+v vs %+v",
						i, r1.Facts.Checks[i], r2.Facts.Checks[i])
				}
			}
		})
	}
}

// TestSabotageVerdictsFail guarantees negative cases fail on the verdict,
// never merely by erroring: every sabotaged scenario must produce a report
// whose verdict is FAIL with at least one failing evidence check.
func TestSabotageVerdictsFail(t *testing.T) {
	for _, id := range AllScenarios() {
		id := id
		t.Run(string(id), func(t *testing.T) {
			rep := runOne(t, id, true)
			if rep.Facts.Verdict != VerdictFail {
				t.Fatalf("sabotaged %s verdict=%s want FAIL", id, rep.Facts.Verdict)
			}
			sawFail := false
			for _, c := range rep.Facts.Checks {
				if !c.Pass {
					sawFail = true
				}
			}
			if !sawFail {
				t.Fatalf("sabotaged %s FAIL verdict without a failing check", id)
			}
		})
	}
}
