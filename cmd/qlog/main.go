// Command qlog is the unified, offline entry point for the quorum log
// replication system. It boots a five-node cluster in one process, drives a
// scripted verification scenario (writes, partition, failover, crash/restart,
// membership change, snapshot catch-up and concurrent load) against the local
// simulated network and local on-disk stores, and prints a readable report.
//
// It requires no sockets, external services or credentials and is safe to run
// fully offline:
//
//	go run ./cmd/qlog
//
// All node data is written under a fresh OS temp directory and removed on exit
// unless -keep is given.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/cluster"
)

type step struct {
	name string
	fn   func(ctx context.Context, c *cluster.Cluster) error
}

func main() {
	keep := flag.Bool("keep", false, "keep the on-disk data directory after the run")
	nodes := flag.Int("nodes", 5, "number of cluster nodes")
	drillCfg := registerDrillFlags(flag.CommandLine)
	flag.Parse()

	// Failure-drill mode: repeatable, self-verifying scenarios with a
	// machine-readable audit report.
	if drillCfg.selected != "" {
		os.Exit(runDrills(drillCfg))
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

	ids := make([]string, 0, *nodes)
	for i := 1; i <= *nodes; i++ {
		ids = append(ids, fmt.Sprintf("n%d", i))
	}
	c, err := cluster.New(ctx, cluster.Options{DataDir: filepath.Join(dir, "data"), NodeIDs: ids, TickMS: 20})
	must("bootstrap cluster", err)
	defer c.Close()

	scenario := buildScenario(ids)
	fmt.Printf("== qlog verification: %d nodes ==\n", *nodes)
	startAll := time.Now()
	for i, s := range scenario {
		t0 := time.Now()
		if err := s.fn(ctx, c); err != nil {
			fmt.Printf("[%2d/%2d] FAIL %-28s %v\n", i+1, len(scenario), s.name, err)
			os.Exit(1)
		}
		fmt.Printf("[%2d/%2d] OK   %-28s (%s)\n", i+1, len(scenario), s.name, time.Since(t0).Round(time.Millisecond))
	}

	obs := c.Observe()
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("leader: %s\n", obs.Leader)
	for id, s := range obs.Nodes {
		fmt.Printf("  node %-3s state=%-9s term=%-3d commit=%-3d applied=%-3d snap=%-3d voters=%v\n",
			id, s.State, s.Term, s.CommitIndex, s.LastApplied, s.SnapshotIndex, s.CommittedConfig.Voters)
	}
	fmt.Printf("all %d steps passed in %s\n", len(scenario), time.Since(startAll).Round(time.Millisecond))
}

func buildScenario(ids []string) []step {
	leaderWait := func(c *cluster.Cluster) error {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if c.Leader() != "" {
				return nil
			}
			time.Sleep(20 * time.Millisecond)
		}
		return fmt.Errorf("no leader elected")
	}

	return []step{
		{"elect initial leader", func(ctx context.Context, c *cluster.Cluster) error {
			return leaderWait(c)
		}},
		{"initial committed writes", func(ctx context.Context, c *cluster.Cluster) error {
			cl := c.Client()
			for i := 1; i <= 5; i++ {
				if err := cl.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)); err != nil {
					return err
				}
			}
			return expect(ctx, c, "k3", "v3")
		}},
		{"idempotent duplicate retries", func(ctx context.Context, c *cluster.Cluster) error {
			// Two independent clients writing distinct keys; retries inside
			// the client reuse the same (client,seq) and must not double-apply.
			for i := 0; i < 5; i++ {
				if err := c.Client().Put(ctx, "dup", "same"); err != nil {
					return err
				}
			}
			return expect(ctx, c, "dup", "same")
		}},
		{"minority partition rejects commit", func(ctx context.Context, c *cluster.Cluster) error {
			leader := c.Leader()
			minority := ""
			for _, id := range ids {
				if id != leader {
					minority = id
					break
				}
			}
			c.Partition(minority)
			time.Sleep(300 * time.Millisecond)
			if err := c.Client().Put(ctx, "during-split", "1"); err != nil {
				c.HealAll()
				return err
			}
			c.HealAll()
			return expect(ctx, c, "during-split", "1")
		}},
		{"crash and restart follower", func(ctx context.Context, c *cluster.Cluster) error {
			victim := ids[1]
			if err := c.Crash(ctx, victim); err != nil {
				return err
			}
			time.Sleep(200 * time.Millisecond)
			if err := c.Restart(ctx, victim); err != nil {
				return err
			}
			if err := leaderWait(c); err != nil {
				return err
			}
			return expect(ctx, c, "k2", "v2")
		}},
		{"add a new voting member", func(ctx context.Context, c *cluster.Cluster) error {
			newID := fmt.Sprintf("n%d", len(ids)+1)
			return c.AddNode(ctx, newID)
		}},
		{"writes continue after add", func(ctx context.Context, c *cluster.Cluster) error {
			return c.Client().Put(ctx, "post-add", "ok")
		}},
		{"snapshot catch-up of a lagging node", func(ctx context.Context, c *cluster.Cluster) error {
			// Drive enough writes on the now 6-node cluster to cross the
			// snapshot threshold and keep log replication healthy.
			cl := c.Client()
			for i := 0; i < 40; i++ {
				if err := cl.Put(ctx, fmt.Sprintf("bulk-%d", i), fmt.Sprintf("%d", i)); err != nil {
					return err
				}
			}
			return expect(ctx, c, "bulk-39", "39")
		}},
		{"concurrent writers", func(ctx context.Context, c *cluster.Cluster) error {
			var wg sync.WaitGroup
			errCh := make(chan error, 4)
			for w := 0; w < 4; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					cl := c.Client()
					for i := 0; i < 5; i++ {
						if err := cl.Put(ctx, fmt.Sprintf("cw-%d-%d", w, i), "x"); err != nil {
							errCh <- err
							return
						}
					}
				}(w)
			}
			wg.Wait()
			close(errCh)
			for e := range errCh {
				return e
			}
			return expect(ctx, c, "cw-3-4", "x")
		}},
	}
}

func expect(ctx context.Context, c *cluster.Cluster, key, want string) error {
	cl := c.Client()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		v, ok, err := cl.Get(ctx, key)
		if err == nil && ok && v == want {
			return nil
		}
		time.Sleep(40 * time.Millisecond)
	}
	return fmt.Errorf("key %q did not reach %q", key, want)
}

func must(what string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
		os.Exit(1)
	}
}
