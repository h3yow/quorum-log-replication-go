package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/internal/raft"
)

func testCluster(t *testing.T, ids ...raft.NodeID) *Cluster {
	t.Helper()
	c := New(Options{DataDir: t.TempDir()})
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Bootstrap(ctx, ids...); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	return c
}

func TestSmokeElectAndPutGet(t *testing.T) {
	c := testCluster(t, "n1", "n2", "n3")
	ctx := context.Background()
	if err := c.Put(ctx, "client-a", 1, "k1", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	v, ok, err := c.Get(ctx, "k1")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("get: v=%q ok=%v err=%v", v, ok, err)
	}
	t.Logf("leader=%s status=%+v", c.Leader(), func() string {
		st := c.Status()
		return string(st.Leader)
	}())
}
