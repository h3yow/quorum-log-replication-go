package drill

import (
	"context"
	"encoding/json"
	"testing"
)

func runOK(t *testing.T, s Scenario) *Report {
	t.Helper()
	r, err := NewRunner(s, nil)
	if err != nil {
		t.Fatalf("NewRunner(%s): %v", s, err)
	}
	rep, err := r.Run(context.Background(), Options{
		DataDir: t.TempDir(),
		Input:   DefaultInput(s),
		TickMS:  15,
	})
	if err != nil {
		t.Fatalf("Run(%s): %v", s, err)
	}
	return rep
}

func assertPass(t *testing.T, rep *Report) {
	t.Helper()
	if !rep.Passed {
		for _, c := range rep.Checks {
			t.Errorf("  [%s] passed=%v %s", c.ID, c.Passed, c.Evidence)
		}
		t.Fatalf("drill %s reported FAIL", rep.Scenario)
	}
}

// TestAllDrillsPassWithFixedInput: every built-in drill passes against its
// canonical fixed input.
func TestAllDrillsPassWithFixedInput(t *testing.T) {
	for _, s := range Scenarios() {
		t.Run(string(s), func(t *testing.T) {
			rep := runOK(t, s)
			assertPass(t, rep)
		})
	}
}

// TestRepeatableVerdict: two independent runs with the same input produce the
// same overall verdict and the same content hash (all historical facts), while
// timing fields may differ.
func TestRepeatableVerdict(t *testing.T) {
	for _, s := range Scenarios() {
		t.Run(string(s), func(t *testing.T) {
			r1 := runOK(t, s)
			r2 := runOK(t, s)
			if r1.Passed != r2.Passed {
				t.Fatalf("verdict drift: %v vs %v", r1.Passed, r2.Passed)
			}
			if r1.ContentHash != r2.ContentHash {
				t.Fatalf("content hash drift for same input:\n run1=%s\n run2=%s",
					r1.ContentHash, r2.ContentHash)
			}
			if len(r1.Checks) != len(r2.Checks) {
				t.Fatalf("check count drift: %d vs %d", len(r1.Checks), len(r2.Checks))
			}
			for i := range r1.Checks {
				if r1.Checks[i].ID != r2.Checks[i].ID ||
					r1.Checks[i].Passed != r2.Checks[i].Passed {
					t.Fatalf("check %d drift: %+v vs %+v", i, r1.Checks[i], r2.Checks[i])
				}
			}
			if r1.Convergence.HistoryHash != r2.Convergence.HistoryHash {
				t.Fatalf("final history hash drift: %s vs %s",
					r1.Convergence.HistoryHash, r2.Convergence.HistoryHash)
			}
		})
	}
}

// TestReportIsMachineReadableAndOrdered: the report marshals to JSON and every
// expected section is populated.
func TestReportIsMachineReadable(t *testing.T) {
	rep := runOK(t, ScenarioMajorityDown)
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	var back Report
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if back.Schema != "qlog-drill/v1" || back.ContentHash == "" {
		t.Fatalf("report missing schema/hash: %+v", back)
	}
	if len(back.Checks) == 0 || len(back.Faults) == 0 {
		t.Fatalf("report missing checks/faults: %+v", back)
	}
	if !back.Convergence.Reached || back.Convergence.HistoryHash == "" {
		t.Fatalf("report missing convergence: %+v", back.Convergence)
	}
	// Recomputing the hash over the same facts must match (integrity check).
	if h := rep.computeContentHash(); h != rep.ContentHash {
		t.Fatalf("content hash not reproducible: %s vs %s", h, rep.ContentHash)
	}
}
