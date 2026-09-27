package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/h3yow/quorum-log-replication-go/pkg/drill"
)

// drillFlags holds the failure-drill command-line options.
type drillFlags struct {
	selected string
	report   string
	keepDir  string
}

func registerDrillFlags(fs *flag.FlagSet) *drillFlags {
	d := &drillFlags{}
	fs.StringVar(&d.selected, "drill", "",
		"run failure drills instead of the default verification: one of "+
			"majority-unreachable, leader-crash-dedup, snapshot-catchup, "+
			"membership-leader-switch, or \"all\"")
	fs.StringVar(&d.report, "report", "",
		"write the machine-readable JSON audit report(s) to this path (\"-\" for stdout)")
	fs.StringVar(&d.keepDir, "drill-dir", "",
		"retain per-drill on-disk cluster data under this directory")
	return d
}

// runDrills executes the requested drills, prints a human summary and emits
// the JSON report. It returns the process exit code.
func runDrills(d *drillFlags) int {
	sel := strings.TrimSpace(d.selected)
	var ids []drill.ScenarioID
	switch sel {
	case "all", "":
		ids = drill.AllScenarios()
	default:
		known := map[drill.ScenarioID]bool{}
		for _, s := range drill.AllScenarios() {
			known[s] = true
		}
		one := drill.ScenarioID(sel)
		if !known[one] {
			fmt.Fprintf(os.Stderr, "unknown drill %q; choose one of: all, %s\n",
				sel, joinScenarioIDs(drill.AllScenarios()))
			return 2
		}
		ids = []drill.ScenarioID{one}
	}

	r := drill.NewRunner()
	if d.keepDir != "" {
		r.RootDir = d.keepDir
	}
	reps, err := r.Run(context.Background(), drill.Options{Scenarios: ids})
	if err != nil {
		fmt.Fprintf(os.Stderr, "drill run failed: %v\n", err)
		return 1
	}

	exit := 0
	for _, rep := range reps {
		mark := "OK  "
		if rep.Facts.Verdict != drill.VerdictPass {
			mark = "FAIL"
			exit = 1
		}
		fmt.Printf("[%s] %-28s verdict=%s digest=%s (%dms)\n",
			mark, rep.Facts.Scenario, rep.Facts.Verdict,
			shortDigest(rep.Digest), rep.DurationMS)
		for _, c := range rep.Facts.Checks {
			cmark := "  +"
			if !c.Pass {
				cmark = "  -"
			}
			fmt.Printf("       %s %-32s %s\n", cmark, c.Name, c.Detail)
		}
	}

	if d.report != "" {
		if err := writeReports(d.report, reps); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			return 1
		}
	}
	return exit
}

func writeReports(path string, reps []*drill.Report) error {
	var w io.Writer
	if path == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if len(reps) == 1 {
		return enc.Encode(reps[0])
	}
	return enc.Encode(reps)
}

func joinScenarioIDs(ids []drill.ScenarioID) string {
	parts := make([]string, len(ids))
	for i, s := range ids {
		parts[i] = string(s)
	}
	return strings.Join(parts, ", ")
}

func shortDigest(d string) string {
	if len(d) > 12 {
		return d[:12]
	}
	return d
}
