// Command qlog is the unified, offline entry point for the quorum log
// replication system.
//
// It has two modes:
//
//	go run ./cmd/qlog                  # scripted smoke verification (default)
//	go run ./cmd/qlog -drill all       # run every failure drill
//	go run ./cmd/qlog -drill <name>    # run one failure drill
//	go run ./cmd/qlog -list-drills     # list drill names
//
// Failure drills run entirely offline against the in-process simulated
// network and local on-disk stores. Each drill emits a machine-readable JSON
// audit report (-report-out <file>, or stdout) plus a short human-readable
// line. Exit status is non-zero if any drill's verdict is FAIL.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/cluster"
	"github.com/h3yow/quorum-log-replication-go/pkg/drill"
)

func main() {
	keep := flag.Bool("keep", false, "keep the on-disk data directory after the run")
	nodes := flag.Int("nodes", 5, "number of cluster nodes (smoke mode)")
	tickMS := flag.Int("tick-ms", 20, "simulated raft tick in milliseconds")
	d := flag.String("drill", "", "failure drill to run: a drill name, 'all', or empty for smoke mode")
	listDrills := flag.Bool("list-drills", false, "list available failure drills and exit")
	reportOut := flag.String("report-out", "", "write JSON audit report(s) to this file (default stdout); with '-drill all' one file per drill is written using this as a prefix when it is a directory")
	flag.Parse()

	if *listDrills {
		for _, s := range drill.Scenarios() {
			fmt.Println(s)
		}
		return
	}

	dir, err := os.MkdirTemp("", "qlog-")
	must("create temp dir", err)
	if *keep {
		fmt.Printf("data directory: %s\n", dir)
	} else {
		defer os.RemoveAll(dir)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *d == "" {
		runSmoke(ctx, dir, *nodes)
		return
	}
	runDrills(ctx, dir, *d, *reportOut, *tickMS)
}

func waitSmokeAligned(c *cluster.Cluster, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		obs := c.Observe()
		var want uint64
		for _, s := range obs.Nodes {
			if s.CommitIndex > want {
				want = s.CommitIndex
			}
		}
		aligned := len(obs.Nodes) > 0
		for _, s := range obs.Nodes {
			if s.LastApplied != want {
				aligned = false
			}
		}
		if aligned {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func runSmoke(ctx context.Context, dir string, n int) {
	ids := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		ids = append(ids, fmt.Sprintf("n%d", i))
	}
	c, err := cluster.New(ctx, cluster.Options{DataDir: filepath.Join(dir, "data"), NodeIDs: ids, TickMS: 20})
	must("bootstrap cluster", err)
	defer c.Close()

	scenario := buildScenario(ids)
	fmt.Printf("== qlog verification: %d nodes ==\n", n)
	startAll := time.Now()
	for i, s := range scenario {
		t0 := time.Now()
		if err := s.fn(ctx, c); err != nil {
			fmt.Printf("[%2d/%2d] FAIL %-28s %v\n", i+1, len(scenario), s.name, err)
			os.Exit(1)
		}
		fmt.Printf("[%2d/%2d] OK   %-28s (%s)\n", i+1, len(scenario), s.name, time.Since(t0).Round(time.Millisecond))
	}

	// Wait for every node to apply up to a single common position before
	// printing the summary, so the reported applied positions and the actual
	// saved contents are shown in their quiescent, converged state.
	waitSmokeAligned(c, 5*time.Second)
	obs := c.Observe()
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("leader: %s\n", obs.Leader)
	for id, s := range obs.Nodes {
		fmt.Printf("  node %-3s state=%-9s term=%-3d commit=%-3d applied=%-3d snap=%-3d voters=%v\n",
			id, s.State, s.Term, s.CommitIndex, s.LastApplied, s.SnapshotIndex, s.CommittedConfig.Voters)
	}
	fmt.Printf("all %d steps passed in %s\n", len(scenario), time.Since(startAll).Round(time.Millisecond))
}

func runDrills(ctx context.Context, dir, which, reportOut string, tickMS int) {
	var scenarios []drill.Scenario
	if which == "all" {
		scenarios = drill.Scenarios()
	} else {
		scenarios = []drill.Scenario{drill.Scenario(which)}
	}

	reports := make([]*drill.Report, 0, len(scenarios))
	anyFailed := false
	for _, s := range scenarios {
		r, err := drill.NewRunner(s, nil)
		must("build drill", err)
		rep, err := r.Run(ctx, drill.Options{
			DataDir: filepath.Join(dir, string(s)),
			Input:   drill.DefaultInput(s),
			TickMS:  tickMS,
		})
		must("run drill "+string(s), err)
		reports = append(reports, rep)
		verdict := "PASS"
		if !rep.Passed {
			verdict = "FAIL"
			anyFailed = true
		}
		fmt.Printf("[drill] %-30s %s  checks=%d  duration=%dms  content_hash=%s\n",
			s, verdict, len(rep.Checks), rep.DurationMS, rep.ContentHash)
		for _, c := range rep.Checks {
			mark := "  ok"
			if !c.Passed {
				mark = "  !! FAIL"
			}
			fmt.Printf("           %s %-38s %s\n", mark, c.ID, c.Evidence)
		}
	}

	if err := writeReports(reportOut, which, reports); err != nil {
		must("write report", err)
	}

	if anyFailed {
		os.Exit(1)
	}
}

// writeReports emits the JSON report(s). With a single drill it writes one
// Report object; with 'all' it writes an array, or one file per drill when the
// target is an existing directory.
func writeReports(target, which string, reports []*drill.Report) error {
	b, err := json.MarshalIndent(reports[0], "", "  ")
	if err != nil {
		return err
	}
	if which == "all" {
		b, err = json.MarshalIndent(reports, "", "  ")
		if err != nil {
			return err
		}
	}
	if target == "" {
		fmt.Println("------------------------------------------------------------")
		fmt.Println(string(b))
		return nil
	}
	if fi, err := os.Stat(target); err == nil && fi.IsDir() {
		for _, r := range reports {
			p := filepath.Join(target, string(r.Scenario)+".json")
			if err := os.WriteFile(p, appendMustMarshal(r), 0o644); err != nil {
				return err
			}
			fmt.Printf("report written: %s\n", p)
		}
		return nil
	}
	return os.WriteFile(target, b, 0o644)
}

func appendMustMarshal(r *drill.Report) []byte {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		panic(err)
	}
	return b
}

func must(what string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
		os.Exit(1)
	}
}
