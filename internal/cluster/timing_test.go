package cluster

import (
	"context"
	"testing"
	"time"
)

func TestFailoverSplitVote(t *testing.T) {
	c := testCluster(t, "n1", "n2", "n3")
	ctx := context.Background()
	if err := c.Put(ctx, "d", 1, "k", "v"); err != nil {
		t.Fatal(err)
	}
	leader := c.Leader()
	c.Crash(leader)
	// 给足够长时间（远超选举超时），必须能恢复。
	deadline := time.Now().Add(5 * time.Second)
	var nl string
	for time.Now().Before(deadline) {
		if x := c.Leader(); x != "" && x != leader {
			nl = string(x)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if nl == "" {
		t.Fatal("no new leader within 5s after crash")
	}
	t.Logf("new leader=%s", nl)
}
