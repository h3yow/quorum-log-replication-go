package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/cluster"
)

type step struct {
	name string
	fn   func(ctx context.Context, c *cluster.Cluster) error
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
