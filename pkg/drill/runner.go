package drill

import (
	"context"
	"fmt"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/cluster"
)

// Faults abstracts the failure injection of a drill so tests can prove that a
// drill FAILS when the fault does not actually take effect. The default
// implementation performs the real faults against the cluster; tests use a
// no-op implementation to assert the evidence checks are not mere flow checks.
type Faults interface {
	// Crash hard-stops id and removes it from the network (disk retained).
	Crash(ctx context.Context, id string) error
	// Restart reloads id from its durable directory.
	Restart(ctx context.Context, id string) error
	// Partition isolates id bidirectionally from peers (all peers if empty).
	Partition(id string, peers ...string)
	// HealAll removes every installed partition.
	HealAll()
}

// Runner executes one drill against a fresh, self-contained cluster.
type Runner struct {
	scenario Scenario
	faults   Faults
	// factory, when set, builds Faults bound to the run's concrete cluster.
	// It takes precedence over the static faults field and lets tests inject
	// faults that need a live cluster handle.
	factory func(*cluster.Cluster) Faults
}

// NewRunner builds a runner for the named scenario. A nil faults uses the real
// crash/restart implementation.
func NewRunner(s Scenario, f Faults) (*Runner, error) {
	if !known(s) {
		return nil, fmt.Errorf("drill: unknown scenario %q", s)
	}
	return &Runner{scenario: s, faults: f}, nil
}

// SetFaultFactory installs a builder that receives the run's live cluster and
// returns the fault implementation to use. It is intended for tests that must
// inject faults (or verified no-ops) bound to that concrete cluster.
func (r *Runner) SetFaultFactory(fn func(*cluster.Cluster) Faults) {
	r.factory = fn
}

func known(s Scenario) bool {
	for _, x := range Scenarios() {
		if x == s {
			return true
		}
	}
	return false
}

// Options controls a drill run. DataDir must be an empty/per-run directory.
type Options struct {
	DataDir string
	Input   Input
	TickMS  int
}

// Run executes the drill and returns its audit report. A drill whose fault
// fails to take effect, whose safety property is violated, or whose cluster
// fails to converge returns Passed=false with the failing checks explaining
// why; infrastructure errors are returned as an error.
func (r *Runner) Run(ctx context.Context, opt Options) (*Report, error) {
	start := time.Now()
	in := withDefaults(opt.Input)
	c, err := cluster.New(ctx, cluster.Options{
		DataDir: in.DataDir(opt.DataDir),
		NodeIDs: in.NodeIDs,
		TickMS:  opt.TickMS,
	})
	if err != nil {
		return nil, fmt.Errorf("drill: bootstrap: %w", err)
	}
	defer c.Close()

	fr := &frame{
		ctx:    ctx,
		c:      c,
		faults: r.faults,
		input:  in,
	}
	if r.factory != nil {
		fr.faults = r.factory(c)
	}
	if fr.faults == nil {
		fr.faults = realFaults{c: c}
	}

	var rep *Report
	switch r.scenario {
	case ScenarioMajorityDown:
		rep = runMajorityDown(fr)
	case ScenarioLeaderCrashRetry:
		rep = runLeaderCrashRetry(fr)
	case ScenarioSnapshotCatchup:
		rep = runSnapshotCatchup(fr)
	case ScenarioMembershipLeaderSwitch:
		rep = runMembershipLeaderSwitch(fr)
	default:
		return nil, fmt.Errorf("drill: unimplemented scenario %q", r.scenario)
	}

	rep.StartedAtISO = start.UTC().Format(time.RFC3339Nano)
	rep.DurationMS = time.Since(start).Milliseconds()
	rep.finalize()
	return rep, nil
}

// RunAll runs every built-in drill in fixed order with independent data dirs.
func RunAll(ctx context.Context, rootDir string, tickMS int) ([]*Report, error) {
	out := make([]*Report, 0, len(Scenarios()))
	for _, s := range Scenarios() {
		r, err := NewRunner(s, nil)
		if err != nil {
			return nil, err
		}
		rep, err := r.Run(ctx, Options{
			DataDir: rootDir + "/" + string(s),
			Input:   DefaultInput(s),
			TickMS:  tickMS,
		})
		if err != nil {
			return nil, fmt.Errorf("drill %s: %w", s, err)
		}
		out = append(out, rep)
	}
	return out, nil
}

// DefaultInput returns the fixed canonical input for a scenario.
func DefaultInput(s Scenario) Input {
	switch s {
	case ScenarioMajorityDown:
		return Input{NodeCount: 5, WritesBefore: 5, WritesDuring: 3, WritesAfter: 5, KeyPrefix: "md", Seed: 0}
	case ScenarioLeaderCrashRetry:
		return Input{NodeCount: 5, WritesBefore: 5, WritesDuring: 1, WritesAfter: 5, KeyPrefix: "lc", CrashDelayMS: 20, Seed: 0}
	case ScenarioSnapshotCatchup:
		return Input{NodeCount: 5, WritesBefore: 3, WritesDuring: 70, WritesAfter: 20, KeyPrefix: "sc", Seed: 0}
	case ScenarioMembershipLeaderSwitch:
		return Input{NodeCount: 5, WritesBefore: 5, WritesDuring: 1, WritesAfter: 5, KeyPrefix: "ms", ChangeDelayMS: 200, Seed: 0}
	default:
		return Input{}
	}
}

func withDefaults(in Input) Input {
	if in.NodeCount == 0 {
		in.NodeCount = 5
	}
	if len(in.NodeIDs) == 0 {
		in.NodeIDs = make([]string, in.NodeCount)
		for i := range in.NodeIDs {
			in.NodeIDs[i] = fmt.Sprintf("n%d", i+1)
		}
	}
	if in.KeyPrefix == "" {
		in.KeyPrefix = "d"
	}
	return in
}

// DataDir joins the per-run data directory.
func (in Input) DataDir(root string) string {
	if root == "" {
		root = "data"
	}
	return root
}

// realFaults performs faults directly on the cluster.
type realFaults struct{ c *cluster.Cluster }

func (f realFaults) Crash(ctx context.Context, id string) error   { return f.c.Crash(ctx, id) }
func (f realFaults) Restart(ctx context.Context, id string) error { return f.c.Restart(ctx, id) }
func (f realFaults) Partition(id string, peers ...string)         { f.c.Partition(id, peers...) }
func (f realFaults) HealAll()                                     { f.c.HealAll() }
