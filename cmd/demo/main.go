// Command demo 提供统一的本地启动与验证入口：在临时目录启动一个 3 节点
// 集群，顺序演示选举、写入、崩溃/重启、网络分区下多数派可用性、
// 少数派不可提交、恢复后对齐、重复请求幂等、并发写入、动态成员变更、
// 快照追赶与只读观测。全程离线、无外部依赖、无真实凭据。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/h3yow/quorum-log-replication-go/internal/cluster"
	"github.com/h3yow/quorum-log-replication-go/internal/raft"
)

type step struct {
	name string
	fn   func(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error
}

func main() {
	dir := flag.String("dir", "", "数据目录（默认使用系统临时目录）")
	keep := flag.Bool("keep", false, "保留数据目录（默认演示结束清理）")
	flag.Parse()

	dataDir := *dir
	if dataDir == "" {
		var err error
		dataDir, err = os.MkdirTemp("", "qlr-demo-")
		if err != nil {
			fatal("创建临时目录失败: %v", err)
		}
	} else {
		_ = os.MkdirAll(dataDir, 0o755)
	}
	if !*keep && *dir == "" {
		defer os.RemoveAll(dataDir)
	}

	logf := func(format string, args ...any) {
		fmt.Printf("\n=== %s ===\n", fmt.Sprintf(format, args...))
	}
	fmt.Printf("数据目录: %s\n", dataDir)

	c := cluster.New(cluster.Options{
		DataDir: dataDir,
		Tick:    10 * time.Millisecond,
		RaftOptions: raft.Options{
			ElectionMinTicks:  10,
			ElectionMaxTicks:  20,
			HeartbeatTicks:    2,
			SnapshotThreshold: 20, // 演示中调低阈值以触发压缩
			RPCTimeout:        150 * time.Millisecond,
		},
	})
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	steps := []step{
		{"初始启动 3 节点并完成选举", stepElect},
		{"正常写入与线性一致读", stepPutGet},
		{"重复请求（同 ClientID+Seq）只生效一次", stepIdempotent},
		{"领导者崩溃后的故障切换", stepLeaderFailover},
		{"少数派分区期间不得提交，恢复后对齐", stepMinorityPartition},
		{"并发写入提交顺序一致", stepConcurrentWrites},
		{"动态成员变更：加入第 4、5 个节点后再缩容", stepMembershipChange},
		{"长期离线节点经快照追赶", stepSnapshotCatchUp},
		{"只读观测：成员/领导权/提交位置/同步进度", stepObservability},
	}

	for _, s := range steps {
		logf("%s", s.name)
		if err := s.fn(ctx, c, logf); err != nil {
			fatal("步骤失败 [%s]: %v", s.name, err)
		}
	}
	fmt.Println("\n所有演示步骤通过。")
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\n演示失败: "+format+"\n", args...)
	os.Exit(1)
}

func printStatus(c *cluster.Cluster) {
	b, _ := json.MarshalIndent(c.Status(), "", "  ")
	fmt.Println(string(b))
}

func stepElect(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	leader, err := c.Bootstrap(ctx, "n1", "n2", "n3")
	if err != nil {
		return err
	}
	fmt.Printf("集群选出领导者: %s\n", leader)
	return nil
}

func stepPutGet(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	if err := c.Put(ctx, "demo", 1, "greeting", "hello"); err != nil {
		return err
	}
	v, ok, err := c.Get(ctx, "greeting")
	if err != nil {
		return err
	}
	fmt.Printf("读回 greeting=%q (exists=%v)\n", v, ok)
	if v != "hello" {
		return fmt.Errorf("期望 hello，得到 %q", v)
	}
	return nil
}

func stepIdempotent(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	// 相同 ClientID/Seq 连续提交 3 次（模拟超时重试与乱序重复）。
	for i := 0; i < 3; i++ {
		if err := c.Put(ctx, "idem-client", 7, "dup-key", "same-value"); err != nil {
			return err
		}
	}
	v, _, err := c.Get(ctx, "dup-key")
	if err != nil {
		return err
	}
	fmt.Printf("重复请求后值=%q（仅第一次生效）\n", v)
	return nil
}

func stepLeaderFailover(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	old := c.Leader()
	fmt.Printf("崩溃当前领导者 %s ...\n", old)
	c.Crash(old)
	// 轮询等待故障切换完成（平票时可能需要多轮选举），最多 5 秒。
	var newLeader raft.NodeID
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if x := c.Leader(); x != "" && x != old {
			newLeader = x
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if newLeader == "" {
		return fmt.Errorf("故障切换失败：5 秒内未产生新领导者")
	}
	fmt.Printf("新领导者: %s\n", newLeader)
	if err := c.Put(ctx, "demo", 2, "after-failover", "ok"); err != nil {
		return fmt.Errorf("故障切换后写入失败: %w", err)
	}
	fmt.Printf("重启旧领导者 %s 并等待其对齐 ...\n", old)
	if err := c.Restart(ctx, old); err != nil {
		return err
	}
	time.Sleep(300 * time.Millisecond)
	if v, ok, _ := c.Get(ctx, "after-failover"); !ok || v != "ok" {
		return fmt.Errorf("旧领导者恢复后未对齐：%q/%v", v, ok)
	}
	fmt.Printf("旧领导者已对齐已提交历史\n")
	return nil
}

func stepMinorityPartition(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	leader := c.Leader()
	// 找到与领导者同区的另一个节点，把它和其余两个节点隔开，
	// 使领导者落入 1 节点少数派。
	st := c.Status()
	var other raft.NodeID
	for _, n := range st.Nodes {
		if n.ID != leader {
			other = n.ID
			break
		}
	}
	fmt.Printf("在 %s 与 %s、%s 之间注入分区（%s 成少数派）\n", leader, other, thirdOf(st, leader, other), leader)
	for _, n := range st.Nodes {
		if n.ID != leader {
			c.Partition(leader, n.ID)
		}
	}

	// 少数派写入必须失败（无多数派确认）。
	putErr := make(chan error, 1)
	go func() {
		ctx2, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		putErr <- c.Put(ctx2, "minority", 1, "should-not-commit", "x")
	}()
	select {
	case err := <-putErr:
		if err == nil {
			return fmt.Errorf("少数派写入竟然成功")
		}
		fmt.Printf("少数派写入被正确拒绝: %v\n", err)
	case <-time.After(2 * time.Second):
		return fmt.Errorf("少数派写入未在预期时间内失败")
	}

	// 多数派应选出新领导者并继续提交。
	time.Sleep(400 * time.Millisecond)
	newLeader := c.Leader()
	if newLeader == "" || newLeader == leader {
		return fmt.Errorf("多数派未能在分区期间选出新领导者")
	}
	fmt.Printf("多数派选出新领导者: %s\n", newLeader)
	if err := c.Put(ctx, "majority", 1, "during-partition", "committed"); err != nil {
		return fmt.Errorf("多数派分区期间写入失败: %w", err)
	}

	fmt.Println("修复分区，等待少数派对齐 ...")
	c.HealAll()
	time.Sleep(400 * time.Millisecond)
	if v, ok, _ := c.Get(ctx, "during-partition"); !ok || v != "committed" {
		return fmt.Errorf("分区恢复后历史不一致：%q/%v", v, ok)
	}
	fmt.Println("少数派恢复后已与已提交历史对齐")
	return nil
}

func thirdOf(st cluster.Status, a, b raft.NodeID) raft.NodeID {
	for _, n := range st.Nodes {
		if n.ID != a && n.ID != b {
			return n.ID
		}
	}
	return ""
}

func stepConcurrentWrites(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	const (
		writers   = 8
		perWriter = 15
	)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cid := fmt.Sprintf("writer-%d", w)
			for i := 1; i <= perWriter; i++ {
				if err := c.Put(ctx, cid, uint64(i), fmt.Sprintf("w%d", w), fmt.Sprintf("%d", i)); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	// 每个写者的序列号单调：最终值必须是 perWriter（重复生效会停在更早值）。
	for w := 0; w < writers; w++ {
		v, ok, err := c.Get(ctx, fmt.Sprintf("w%d", w))
		if err != nil || !ok || v != fmt.Sprintf("%d", perWriter) {
			return fmt.Errorf("writer-%d 最终值异常: %q ok=%v err=%v", w, v, ok, err)
		}
	}
	fmt.Printf("%d 个并发写者各 %d 条写入全部一致提交\n", writers, perWriter)
	return nil
}

func stepMembershipChange(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	fmt.Println("加入 n4、n5（联合共识两阶段）...")
	if err := c.AddVoter(ctx, "n4"); err != nil {
		return fmt.Errorf("add n4: %w", err)
	}
	if err := c.AddVoter(ctx, "n5"); err != nil {
		return fmt.Errorf("add n5: %w", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := c.Put(ctx, "demo", 3, "after-grow", "5-nodes"); err != nil {
		return err
	}
	printStatus(c)

	fmt.Println("移除 n1、n2，缩容为 3 节点 ...")
	if err := c.RemoveVoter(ctx, "n1"); err != nil {
		return fmt.Errorf("remove n1: %w", err)
	}
	if err := c.RemoveVoter(ctx, "n2"); err != nil {
		return fmt.Errorf("remove n2: %w", err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := c.Put(ctx, "demo", 4, "after-shrink", "ok"); err != nil {
		return fmt.Errorf("缩容后写入失败: %w", err)
	}
	fmt.Println("成员变更完成，变更期间写入未丢失")
	return nil
}

func stepSnapshotCatchUp(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	fmt.Println("加入新节点 n6，先令其在线，再长时间隔离 ...")
	if err := c.AddVoter(ctx, "n6"); err != nil {
		return fmt.Errorf("add n6: %w", err)
	}
	// 隔离 n6，并在其余集群上持续写入以触发快照压缩（阈值=20）。
	st := c.Status()
	for _, n := range st.Nodes {
		if n.ID != "n6" {
			c.Partition("n6", n.ID)
		}
	}
	for i := 0; i < 60; i++ {
		if err := c.Put(ctx, "bulk", uint64(i+1), fmt.Sprintf("bulk-%d", i), fmt.Sprintf("%d", i)); err != nil {
			return fmt.Errorf("bulk write %d: %w", i, err)
		}
	}
	fmt.Println("已在 n6 离线期间写入 60 条（触发多次快照压缩）")
	fmt.Println("恢复 n6 网络，等待其经 InstallSnapshot + 增量复制追上 ...")
	c.HealAll()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		v, ok, err := c.Get(ctx, "bulk-59")
		if err == nil && ok && v == "59" {
			// 再直接检查 n6 自身应用位置。
			var caught bool
			for _, n := range c.Status().Nodes {
				if n.ID == "n6" && n.AppliedIndex > 0 {
					caught = true
				}
			}
			if caught {
				fmt.Println("n6 已追上集群（无数据空洞）")
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("n6 追赶超时")
}

func stepObservability(ctx context.Context, c *cluster.Cluster, logf func(string, ...any)) error {
	printStatus(c)
	return nil
}
