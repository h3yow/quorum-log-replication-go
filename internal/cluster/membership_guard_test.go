package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/internal/raft"
)

// 联合配置尚未提交（稳定）前，第二次成员变更必须被拒绝，
// 而不是被当作既成事实继续推进。
func TestOverlappingMembershipChangeRejected(t *testing.T) {
	c := testCluster(t, "n1", "n2", "n3")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 启动 n4 但不预告、不追赶，并把它与集群隔开：包含 n4 的联合配置
	// 永远无法在 C_new 侧达成多数，变更会停留在"已追加未提交"阶段。
	if err := c.startMember("n4", raft.SingleConfig("n1", "n2", "n3")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []raft.NodeID{"n1", "n2", "n3"} {
		c.Partition("n4", id)
	}

	leader, _, err := c.leaderNode()
	if err != nil {
		t.Fatal(err)
	}
	ms := raft.NewMembership(leader)

	// 第一次变更：进入联合态后无法提交（C_new 中 n4 不可达）。
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- ms.ApplyConfig(ctx, raft.SingleConfig("n1", "n2", "n3", "n4").Voters)
	}()

	// 等待联合配置条目被追加到领导者日志（LatestConfig 进入联合态，
	// 但因 C_new 的 n4 不可达而无法提交）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if leader.Status().LatestConfig.Joint != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader.Status().LatestConfig.Joint == nil {
		t.Fatal("联合配置条目未被追加")
	}

	// 第二次变更必须立即被拒绝。
	err2 := ms.ApplyConfig(ctx, raft.SingleConfig("n1", "n2").Voters)
	if !errors.Is(err2, raft.ErrJointChangeInProgress) {
		t.Fatalf("期望 ErrJointChangeInProgress，得到 %v", err2)
	}
}
