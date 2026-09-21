package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/internal/raft"
)

// 长期离线节点必须经快照 + 增量复制追上，且无数据空洞。
func TestOfflineNodeSnapshotCatchUp(t *testing.T) {
	opts := raft.DefaultOptions()
	opts.SnapshotThreshold = 10 // 测试中加速触发压缩
	c := New(Options{DataDir: t.TempDir(), RaftOptions: opts})
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := c.Bootstrap(ctx, "n1", "n2", "n3"); err != nil {
		t.Fatal(err)
	}

	// 加入 n4 并让它先追平成为成员。
	if err := c.AddVoter(ctx, "n4"); err != nil {
		t.Fatalf("add n4: %v", err)
	}

	// 隔离 n4，在其余集群写入足够多条目触发多次压缩。
	st := c.Status()
	for _, n := range st.Nodes {
		if n.ID != "n4" {
			c.Partition("n4", n.ID)
		}
	}
	const n = 60
	for i := 1; i <= n; i++ {
		if err := c.Put(ctx, "bulk", uint64(i), fmt.Sprintf("bulk-%d", i), fmt.Sprintf("%d", i)); err != nil {
			t.Fatalf("bulk %d: %v", i, err)
		}
	}

	c.HealAll()
	// n4 的应用位置必须最终达到集群提交位置（快照+增量追平、无空洞）。
	lastKey := fmt.Sprintf("bulk-%d", n)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		stt := c.Status()
		var leaderCommit raft.Index
		var n4Applied raft.Index
		for _, nv := range stt.Nodes {
			if nv.ID == c.Leader() {
				leaderCommit = nv.CommitIndex
			}
			if nv.ID == "n4" {
				n4Applied = nv.AppliedIndex
			}
		}
		if n4Applied > 0 && n4Applied >= leaderCommit {
			if v, ok, err := c.Get(ctx, lastKey); err == nil && ok && v == fmt.Sprintf("%d", n) {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("离线节点未能在时限内经快照追上")
}
