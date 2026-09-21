// Package cluster 在本地编排多个 raft 节点：启动、崩溃/重启、网络分区、
// 成员变更与统一的只读观测入口。所有数据落在本地临时目录，离线可重复。
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/quorum-log-replication-go/internal/kv"
	"github.com/h3yow/quorum-log-replication-go/internal/raft"
	"github.com/h3yow/quorum-log-replication-go/internal/simnet"
)

// NodeView 单个节点的只读观测。
type NodeView struct {
	ID           raft.NodeID           `json:"id"`
	Role         string                `json:"role"`
	Term         raft.Term             `json:"term"`
	Leader       raft.NodeID           `json:"leader"`
	Online       bool                  `json:"online"`
	Voter        bool                  `json:"voter"`
	CommitIndex  raft.Index            `json:"commit_index"`
	LastIndex    raft.Index            `json:"last_index"`
	AppliedIndex raft.Index            `json:"applied_index"`
	KVApplied    raft.Index            `json:"kv_applied"`
	MatchIndex   map[string]raft.Index `json:"match_index,omitempty"`
}

// Status 集群整体只读观测快照（观测不改变集群行为）。
type Status struct {
	Leader  raft.NodeID `json:"leader"`
	Term    raft.Term   `json:"term"`
	Members []string    `json:"members"`
	Nodes   []NodeView  `json:"nodes"`
}

// memberSlot 一个逻辑成员的运行时槽：可反复崩溃/重启并复用同一数据目录。
type memberSlot struct {
	id  raft.NodeID
	dir string
	kv  *kv.Store

	node *raft.Node
	tr   *simnet.Transport
}

// Cluster 本地测试/演示集群。
type Cluster struct {
	mu      sync.Mutex
	net     *simnet.Network
	dataDir string
	tick    time.Duration
	opts    raft.Options

	slots     map[raft.NodeID]*memberSlot
	memberSeq int
	tickStop  chan struct{}
	tickerWG  sync.WaitGroup
	stopped   bool
}

// Options 集群编排参数。
type Options struct {
	DataDir     string
	Tick        time.Duration
	RPCDelay    time.Duration
	RPCJitter   time.Duration
	RaftOptions raft.Options
}

// New 创建空集群。
func New(opts Options) *Cluster {
	tick := opts.Tick
	if tick <= 0 {
		tick = 10 * time.Millisecond
	}
	delay, jitter := opts.RPCDelay, opts.RPCJitter
	if delay <= 0 {
		delay = tick / 2
	}
	if jitter <= 0 {
		jitter = tick / 2
	}
	return &Cluster{
		net:     simnet.New(delay, jitter),
		dataDir: opts.DataDir,
		tick:    tick,
		opts:    opts.RaftOptions,
		slots:   map[raft.NodeID]*memberSlot{},
	}
}

// Bootstrap 用初始成员集合启动集群并完成首次选举；返回选出的领导者。
func (c *Cluster) Bootstrap(ctx context.Context, ids ...raft.NodeID) (raft.NodeID, error) {
	if len(ids) == 0 {
		return "", raft.ErrInvalidConfig
	}
	cfg := raft.SingleConfig(ids...)
	c.startTicker()
	for _, id := range ids {
		if err := c.startMember(id, cfg); err != nil {
			return "", err
		}
	}
	return c.waitLeader(ctx, ids)
}

// startMember 从 dataDir/<id> 构造并注册节点（首次创建或重启共用此路径）。
func (c *Cluster) startMember(id raft.NodeID, bootstrap raft.Config) error {
	dir := filepath.Join(c.dataDir, string(id))
	st, err := raft.NewFileStorage(dir)
	if err != nil {
		// 损坏 / 截断错误原样向上返回，调用方可区分拒绝原因。
		return fmt.Errorf("start member %s: %w", id, err)
	}
	store := kv.New()
	tr := simnet.NewTransport(c.net, id)
	n, err := raft.NewNode(id, st, store, tr, bootstrap, c.optsWithDefaults())
	if err != nil {
		return fmt.Errorf("start member %s: %w", id, err)
	}
	n.Start()
	c.net.Register(id, n)
	c.mu.Lock()
	isNew := false
	if existing, ok := c.slots[id]; ok {
		existing.node = n
		existing.tr = tr
		existing.kv = store
		existing.dir = dir
	} else {
		isNew = true
		c.slots[id] = &memberSlot{id: id, dir: dir, kv: store, node: n, tr: tr}
	}
	phase := time.Duration(c.memberSeq) * c.tick / 3
	c.memberSeq++
	c.mu.Unlock()
	if c.tickStop != nil && isNew {
		c.startMemberTicker(id, phase)
	}
	return nil
}

func (c *Cluster) optsWithDefaults() raft.Options {
	o := c.opts
	if o.ElectionMinTicks <= 0 {
		o = raft.DefaultOptions()
	}
	return o
}

// startTicker 创建集群级停止信号；每个成员的独立 ticker 在其首次
// startMember 时由 startMemberTicker 启动（相位互相错开）。
func (c *Cluster) startTicker() {
	c.tickStop = make(chan struct{})
}

// startMemberTicker 为单个成员启动独立逻辑时钟。各成员 ticker 周期相同
// 但启动相位错开，避免共享节拍导致多个节点在同一拍触发选举超时平票。
// 节点崩溃期间 Tick 因 node==nil 被丢弃，重启后自动恢复。
func (c *Cluster) startMemberTicker(id raft.NodeID, phase time.Duration) {
	c.tickerWG.Add(1)
	go func() {
		defer c.tickerWG.Done()
		// 首个 tick 前等待相位偏移；之后严格按周期触发（不追赶、不连发）。
		select {
		case <-c.tickStop:
			return
		case <-time.After(phase):
		}
		t := time.NewTicker(c.tick)
		defer t.Stop()
		for {
			select {
			case <-c.tickStop:
				return
			case <-t.C:
				c.mu.Lock()
				var node *raft.Node
				if s := c.slots[id]; s != nil {
					node = s.node
				}
				c.mu.Unlock()
				if node != nil {
					node.Tick()
				}
			}
		}
	}()
}

func (c *Cluster) waitLeader(ctx context.Context, voters []raft.NodeID) (raft.NodeID, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range voters {
			s := c.nodeStatus(id)
			if s != nil && s.Role == raft.RoleLeader {
				return id, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(c.tick):
		}
	}
	return "", fmt.Errorf("cluster: no leader elected within 5s")
}

func (c *Cluster) nodeStatus(id raft.NodeID) *raft.NodeStatus {
	c.mu.Lock()
	s, ok := c.slots[id]
	var node *raft.Node
	if ok {
		node = s.node
	}
	c.mu.Unlock()
	if node == nil {
		return nil
	}
	st := node.Status()
	return &st
}

// Leader 返回当前集群领导者；无领导者时返回空串。
// 分区期间可能同时存在"旧领导者（低任期，不知自己已被罢免）"与
// 多数派新领导者（高任期），因此取领导者中任期最高者。
func (c *Cluster) Leader() raft.NodeID {
	c.mu.Lock()
	ids := make([]raft.NodeID, 0, len(c.slots))
	for id := range c.slots {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	var leader raft.NodeID
	var term raft.Term
	for _, id := range ids {
		st := c.nodeStatus(id)
		if st == nil || st.Role != raft.RoleLeader {
			continue
		}
		if leader == "" || st.Term > term {
			leader, term = id, st.Term
		}
	}
	return leader
}

// members 返回最近观测到的成员名单（任一在线节点视图即可，配置是复制一致的）。
func (c *Cluster) members() []raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.slots {
		if s.node == nil {
			continue
		}
		st := s.node.Status()
		out := make([]raft.NodeID, 0, len(st.Config.Voters))
		for id := range st.Config.Voters {
			out = append(out, id)
		}
		return out
	}
	return nil
}

// leaderNode 找到当前最高任期的领导者节点（避免选中分区中的旧领导者）。
func (c *Cluster) leaderNode() (*raft.Node, raft.NodeID, error) {
	c.mu.Lock()
	ids := make([]raft.NodeID, 0, len(c.slots))
	for id := range c.slots {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	var bestID raft.NodeID
	var bestTerm raft.Term
	var best *raft.Node
	for _, id := range ids {
		st := c.nodeStatus(id)
		if st == nil || st.Role != raft.RoleLeader {
			continue
		}
		if bestID == "" || st.Term > bestTerm {
			c.mu.Lock()
			best, bestID, bestTerm = c.slots[id].node, id, st.Term
			c.mu.Unlock()
		}
	}
	if best == nil {
		return nil, "", raft.ErrNotLeader
	}
	return best, bestID, nil
}

// Put 通过领导者写入键值。重复的 (clientID, seq) 幂等；
// 遇到 ErrNotLeader / 短暂无领导者时由本层重试到超时。
func (c *Cluster) Put(ctx context.Context, clientID string, seq uint64, key, value string) error {
	cmd := kv.Command{Op: kv.OpPut, Key: key, Value: value}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return c.propose(ctx, clientID, seq, data)
}

// Delete 通过领导者删除键值。
func (c *Cluster) Delete(ctx context.Context, clientID string, seq uint64, key string) error {
	cmd := kv.Command{Op: kv.OpDelete, Key: key}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return c.propose(ctx, clientID, seq, data)
}

func (c *Cluster) propose(ctx context.Context, clientID string, seq uint64, data []byte) error {
	for {
		n, _, err := c.leaderNode()
		if err == nil {
			err = n.Propose(ctx, clientID, seq, data)
			if err == nil {
				return nil
			}
		}
		if errors.Is(err, raft.ErrStopped) {
			return err
		}
		// ErrNotLeader / 选举中 / ctx 未到期：短暂退避后重定向。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.tick * 2):
		}
	}
}

// Get 线性一致读：ReadIndex 确认领导权与提交位置，等待该节点状态机
// 应用到该位置后再读本地 KV，所有节点读到的提交顺序一致。
func (c *Cluster) Get(ctx context.Context, key string) (string, bool, error) {
	for {
		n, _, err := c.leaderNode()
		if err != nil || n == nil {
			select {
			case <-ctx.Done():
				return "", false, ctx.Err()
			case <-time.After(c.tick * 2):
			}
			continue
		}
		idx, err := n.ReadIndex(ctx)
		if err != nil {
			if errors.Is(err, raft.ErrStopped) {
				return "", false, err
			}
			continue
		}
		store := c.waitApplied(ctx, idx)
		if store == nil {
			return "", false, ctx.Err()
		}
		v, ok := store.Read(key)
		return v, ok, nil
	}
}

// waitApplied 轮询任一在线节点（优先领导者）直到其 KV 应用位置 >= idx。
func (c *Cluster) waitApplied(ctx context.Context, idx raft.Index) *kv.Store {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, s := range c.slots {
			if s.node != nil && s.kv != nil && s.kv.AppliedIndex() >= idx {
				st := s.kv
				c.mu.Unlock()
				return st
			}
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(c.tick):
		}
	}
	return nil
}

// AddVoter 启动新节点（空目录）并通过"非投票预告 + 联合共识"加入：
//  1. 新节点先以进程身份启动，领导者通过 EntryAddNonVoter 开始向其
//     推送快照/日志（它无投票权，不影响多数派）；
//  2. 待其追上后，联合配置两阶段提交授予投票权。
//
// 配置条目提交前新节点不是既成投票成员，变更过程中已提交数据不丢不错位。
func (c *Cluster) AddVoter(ctx context.Context, id raft.NodeID) error {
	c.mu.Lock()
	_, exists := c.slots[id]
	c.mu.Unlock()
	if !exists {
		cfg := raft.SingleConfig(c.members()...)
		if err := c.startMember(id, cfg); err != nil {
			return err
		}
	} else if err := c.ensureOnline(id); err != nil {
		return err
	}
	// 阶段 0：让领导者立即开始向新节点复制（快照+增量）。
	for {
		n, _, err := c.leaderNode()
		if err == nil {
			err = n.AddNonVoter(ctx, id)
		}
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.tick * 2):
		}
	}
	// 等待新节点日志追上领导者提交位置。
	if err := c.waitNodeCaughtUp(ctx, id, 10*time.Second); err != nil {
		return err
	}
	// 阶段 1/2：联合共识授予投票权（领导权切换时自动重定向重试，
	// 目标配置相同，故整个变更对重试天然幂等）。
	return c.changeConfigRetry(ctx, func(n *raft.Node) error {
		return raft.NewMembership(n).AddVoter(ctx, id)
	})
}

// RemoveVoter 通过联合共识移除投票成员。成员变更可能引发领导权转移
// （例如领导者恰好在缩容后失去多数派），因此在领导权切换时自动
// 重定向到新领导者重试，直到成功或 ctx 超时。
func (c *Cluster) RemoveVoter(ctx context.Context, id raft.NodeID) error {
	return c.changeConfigRetry(ctx, func(n *raft.Node) error {
		return raft.NewMembership(n).RemoveVoter(ctx, id)
	})
}

// changeConfigRetry 在领导权切换/短暂无领导者时反复发现并重试成员变更。
func (c *Cluster) changeConfigRetry(ctx context.Context, do func(n *raft.Node) error) error {
	for {
		n, _, err := c.leaderNode()
		if err == nil {
			err = do(n)
		}
		if err == nil {
			return nil
		}
		if errors.Is(err, raft.ErrStopped) || errors.Is(err, raft.ErrInvalidConfig) {
			return err
		}
		// ErrNotLeader / ErrLeadershipLost / ErrNoQuorum /
		// ErrJointChangeInProgress：领导权刚切换或上一次联合变更尚在
		// 提交途中（例如上一任领导者应用最终配置后立刻卸任）。
		// 等待其稳定后重定向到新领导者重试——配置未稳定前不得开启新变更。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.tick * 2):
		}
	}
}

func (c *Cluster) ensureOnline(id raft.NodeID) error {
	c.mu.Lock()
	s, ok := c.slots[id]
	online := ok && s.node != nil
	c.mu.Unlock()
	if online {
		return nil
	}
	return c.startMember(id, raft.SingleConfig(c.members()...))
}

// waitNodeCaughtUp 等待新节点日志位置接近领导者提交位置（允许一轮复制延迟）。
func (c *Cluster) waitNodeCaughtUp(ctx context.Context, id raft.NodeID, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := c.nodeStatus(c.Leader())
		node := c.nodeStatus(id)
		if leader != nil && node != nil && node.LastIndex >= leader.CommitIndex {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.tick):
		}
	}
	return fmt.Errorf("cluster: node %s failed to catch up within %s", id, timeout)
}

// Crash 停止节点进程并从网络摘除（持久化文件保留）。
func (c *Cluster) Crash(id raft.NodeID) {
	c.mu.Lock()
	s := c.slots[id]
	c.mu.Unlock()
	if s == nil || s.node == nil {
		return
	}
	c.net.Unregister(id)
	_ = s.node.Stop()
	c.mu.Lock()
	s.node = nil
	c.mu.Unlock()
}

// Restart 从同一数据目录恢复节点；存储损坏/截断时返回可区分错误。
func (c *Cluster) Restart(ctx context.Context, id raft.NodeID) error {
	c.mu.Lock()
	_, ok := c.slots[id]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("cluster: unknown member %s", id)
	}
	// 重启时引导配置不会覆盖已有日志（NewNode 只对空目录种子化）。
	if err := c.startMember(id, raft.SingleConfig(id)); err != nil {
		return err
	}
	return nil
}

// Partition 在 a、b 之间注入双向网络分区。
func (c *Cluster) Partition(a, b raft.NodeID) { c.net.AddPartition(a, b) }

// Heal 修复 a、b 之间的分区。
func (c *Cluster) Heal(a, b raft.NodeID) { c.net.RemovePartition(a, b) }

// HealAll 清除全部分区。
func (c *Cluster) HealAll() { c.net.HealAll() }

// Status 返回只读观测快照。
func (c *Cluster) Status() Status {
	// 锁内只取节点指针与 KV 引用（指针赋值是同步点），锁外执行
	// 可能阻塞的 Status() 调用，避免与 startMember/Crash 的写入竞争。
	type slotRef struct {
		id raft.NodeID
		n  *raft.Node
		k  *kv.Store
	}
	c.mu.Lock()
	refs := make([]slotRef, 0, len(c.slots))
	for id, s := range c.slots {
		refs = append(refs, slotRef{id: id, n: s.node, k: s.kv})
	}
	c.mu.Unlock()

	st := Status{}
	// 先收集所有在线节点状态；集群成员名单以"最高任期领导者"的
	// 已提交配置为准，避免取到已被移除但尚未追平的旧节点的落后视图。
	views := make([]NodeView, 0, len(refs))
	type nsRef struct {
		id raft.NodeID
		ns raft.NodeStatus
		k  *kv.Store
	}
	online := make([]nsRef, 0, len(refs))
	for _, sr := range refs {
		view := NodeView{ID: sr.id, Online: sr.n != nil}
		if sr.n != nil {
			ns := sr.n.Status()
			online = append(online, nsRef{id: sr.id, ns: ns, k: sr.k})
			view.Role = ns.Role.String()
			view.Term = ns.Term
			view.Leader = ns.Leader
			if ns.Role == raft.RoleLeader {
				view.Leader = ns.ID
			}
			view.Voter = ns.Voter
			view.CommitIndex = ns.CommitIndex
			view.LastIndex = ns.LastIndex
			view.AppliedIndex = ns.AppliedIndex
			if sr.k != nil {
				view.KVApplied = sr.k.AppliedIndex()
			}
			if len(ns.Progress) > 0 {
				view.MatchIndex = map[string]raft.Index{}
				for pid, pr := range ns.Progress {
					view.MatchIndex[string(pid)] = pr.MatchIndex
				}
			}
			if ns.Role == raft.RoleLeader {
				st.Leader = ns.ID
				st.Term = ns.Term
			}
		}
		views = append(views, view)
	}
	// 成员名单：优先领导者；无领导者时取最高任期在线节点的配置。
	var authority *raft.NodeStatus
	for i := range online {
		ns := &online[i].ns
		if authority == nil || ns.Term > authority.Term ||
			(ns.Term == authority.Term && ns.Role == raft.RoleLeader) {
			authority = ns
		}
	}
	if authority != nil {
		mids := make([]string, 0, len(authority.Config.Voters))
		for mid := range authority.Config.Voters {
			mids = append(mids, string(mid))
		}
		sort.Strings(mids)
		st.Members = mids
	}
	// 以权威配置统一标注投票身份，避免被移除但尚未追平的旧节点自报。
	if authority != nil {
		voterSet := authority.Config.Voters
		for i := range views {
			_, views[i].Voter = voterSet[views[i].ID]
		}
	}
	sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
	st.Nodes = views
	return st
}

// DataDir 返回某节点的数据目录（测试中用于注入损坏）。
func (c *Cluster) DataDir(id raft.NodeID) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.slots[id]; ok {
		return s.dir
	}
	return ""
}

// Close 停止全部节点与 ticker。
func (c *Cluster) Close() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	nodes := make([]*raft.Node, 0, len(c.slots))
	for _, s := range c.slots {
		if s.node != nil {
			nodes = append(nodes, s.node)
		}
	}
	c.mu.Unlock()
	if c.tickStop != nil {
		close(c.tickStop)
		c.tickerWG.Wait()
	}
	for _, n := range nodes {
		_ = n.Stop()
	}
	return nil
}
