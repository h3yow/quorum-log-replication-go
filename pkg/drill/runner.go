package drill

import (
	"context"
	"fmt"
	"os"
	"time"
)

// Runner executes drills against fresh clusters and collects reports.
type Runner struct {
	// RootDir, when non-empty, keeps per-scenario data there; otherwise a
	// fresh temp directory is created per run and removed on return.
	RootDir string
}

// NewRunner constructs a drill runner using a temp data root.
func NewRunner() *Runner { return &Runner{} }

// scenarioFn runs one fixed-input drill against an already-bootstrapped
// harness and returns its fully built report. Setup failures are errors;
// failed safety/evidence checks are FAIL reports, never errors.
type scenarioFn func(ctx context.Context, h *harness, started time.Time) (*Report, error)

// bootFn performs the per-run cluster bootstrap before invoking the script.
type bootFn func(ctx context.Context, dir string, sabotage bool) (*Report, error)

// Run executes the requested scenarios, each against an isolated fresh
// on-disk cluster, and returns one report per scenario in the requested
// (fixed) order.
func (r *Runner) Run(ctx context.Context, opt Options) ([]*Report, error) {
	ids := opt.Scenarios
	if len(ids) == 0 {
		ids = AllScenarios()
	}
	root := r.RootDir
	if root == "" {
		d, err := os.MkdirTemp("", "qlog-drill-")
		if err != nil {
			return nil, err
		}
		defer func() { _ = os.RemoveAll(d) }()
		root = d
	}

	fns := map[ScenarioID]bootFn{
		ScenarioMajorityUnreachable:    withBootstrap(ScenarioMajorityUnreachable, []string{"d1", "d2", "d3", "d4", "d5"}, runMajorityUnreachable),
		ScenarioLeaderCrashDedup:       withBootstrap(ScenarioLeaderCrashDedup, []string{"d1", "d2", "d3"}, runLeaderCrashDedup),
		ScenarioSnapshotCatchup:        withBootstrap(ScenarioSnapshotCatchup, []string{"d1", "d2", "d3"}, runSnapshotCatchup),
		ScenarioMembershipLeaderSwitch: withBootstrap(ScenarioMembershipLeaderSwitch, []string{"d1", "d2", "d3", "d4", "d5"}, runMembershipLeaderSwitch),
	}

	out := make([]*Report, 0, len(ids))
	for _, id := range ids {
		fn, ok := fns[id]
		if !ok {
			return nil, fmt.Errorf("unknown scenario %q", id)
		}
		rep, err := fn(ctx, root+"/"+string(id), sabotageFor(opt, id))
		if err != nil {
			return nil, fmt.Errorf("drill %s: %w", id, err)
		}
		out = append(out, rep)
	}
	return out, nil
}

func sabotageFor(opt Options, id ScenarioID) bool {
	return opt.Sabotage != nil && opt.Sabotage[id]
}

// withBootstrap creates the cluster + harness for one scenario, then runs it.
func withBootstrap(id ScenarioID, ids []string, fn scenarioFn) bootFn {
	return func(ctx context.Context, dir string, sabotage bool) (*Report, error) {
		started := time.Now()
		h, err := newHarness(id, ids, dir, sabotage)
		if err != nil {
			return nil, err
		}
		defer h.close()
		return fn(ctx, h, started)
	}
}
