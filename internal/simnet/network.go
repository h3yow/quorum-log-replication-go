// Package simnet 提供进程内模拟网络：可注入延迟、分区与节点宕机，
// 全部在本地内存中完成，不依赖外部资源。
package simnet

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/h3yow/quorum-log-replication-go/internal/raft"
)

// ErrNodeDown 目标（或源）节点未注册：进程已崩溃/尚未启动。
var ErrNodeDown = errors.New("simnet: node is down")

// ErrPartitioned 两节点之间当前存在网络分区。
var ErrPartitioned = errors.New("simnet: network partitioned")

// Network 模拟网络。分区以"链路对"为粒度：AddPartition(a,b) 后双向不可达。
type Network struct {
	mu sync.Mutex

	nodes    map[raft.NodeID]raft.Handler
	dropping map[raft.NodeID]bool
	parts    map[[2]raft.NodeID]bool

	baseDelay time.Duration
	jitter    time.Duration
	rng       *rand.Rand
}

// New 创建模拟网络。rpcDelay 为基础延迟，jitter 为随机抖动上限。
func New(rpcDelay, jitter time.Duration) *Network {
	return &Network{
		nodes:     map[raft.NodeID]raft.Handler{},
		dropping:  map[raft.NodeID]bool{},
		parts:     map[[2]raft.NodeID]bool{},
		baseDelay: rpcDelay,
		jitter:    jitter,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Register 注册节点处理器；重复注册覆盖（用于重启）。
func (n *Network) Register(id raft.NodeID, h raft.Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = h
}

// Unregister 模拟节点崩溃：入站消息一律失败，出站连接被拒绝。
// 已建立的分区关系保留，重启后依旧生效。
func (n *Network) Unregister(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.nodes, id)
}

// IsRegistered 返回节点当前是否在线。
func (n *Network) IsRegistered(id raft.NodeID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.nodes[id]
	return ok
}

func pairKey(a, b raft.NodeID) [2]raft.NodeID {
	if a < b {
		return [2]raft.NodeID{a, b}
	}
	return [2]raft.NodeID{b, a}
}

// AddPartition 在 a、b 之间注入双向分区。
func (n *Network) AddPartition(a, b raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.parts[pairKey(a, b)] = true
}

// RemovePartition 修复 a、b 之间的分区。
func (n *Network) RemovePartition(a, b raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.parts, pairKey(a, b))
}

// HealAll 清除全部分区。
func (n *Network) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.parts = map[[2]raft.NodeID]bool{}
}

// SetDrop 设置节点出向消息全丢：请求挂起直到调用方超时，
// 用于构造超时重试、乱序与重复到达场景。
func (n *Network) SetDrop(id raft.NodeID, dropped bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropping[id] = dropped
}

// precheck 在加锁状态下抓取处理器并判定可达性。
func (n *Network) precheck(from, to raft.NodeID) (raft.Handler, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	fromH, ok1 := n.nodes[from]
	toH, ok2 := n.nodes[to]
	if !ok1 || !ok2 {
		return nil, ErrNodeDown
	}
	if n.parts[pairKey(from, to)] {
		return nil, ErrPartitioned
	}
	if n.dropping[from] {
		// 丢包对调用方表现为"无响应"，由 context 超时决定重试，
		// 与真实网络黑洞一致。
		return nil, errSilentDrop
	}
	_ = fromH
	return toH, nil
}

var errSilentDrop = errors.New("simnet: silent drop (waits for caller timeout)")

func (n *Network) delay() time.Duration {
	n.mu.Lock()
	d, j := n.baseDelay, n.jitter
	if j > 0 {
		d += time.Duration(n.rng.Int63n(int64(j)))
	}
	n.mu.Unlock()
	return d
}

// dispatch 模拟一次单向链路：先等待（期间可被 ctx 取消），再投递处理器。
func (n *Network) dispatch(ctx context.Context, from, to raft.NodeID, call func(h raft.Handler) error) error {
	h, err := n.precheck(from, to)
	if err != nil {
		if errors.Is(err, errSilentDrop) {
			<-ctx.Done()
			return ctx.Err()
		}
		return err
	}
	if d := n.delay(); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	// 投递前再次确认链路状态（等待期间可能发生分区/崩溃）。
	h, err = n.precheck(from, to)
	if err != nil {
		if errors.Is(err, errSilentDrop) {
			<-ctx.Done()
			return ctx.Err()
		}
		return err
	}
	return call(h)
}

func (n *Network) SendRequestVote(ctx context.Context, from, to raft.NodeID, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	var resp raft.RequestVoteResponse
	err := n.dispatch(ctx, from, to, func(h raft.Handler) error {
		resp = h.HandleRequestVote(req)
		return nil
	})
	return resp, err
}

func (n *Network) SendAppendEntries(ctx context.Context, from, to raft.NodeID, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	var resp raft.AppendEntriesResponse
	err := n.dispatch(ctx, from, to, func(h raft.Handler) error {
		resp = h.HandleAppendEntries(req)
		return nil
	})
	return resp, err
}

func (n *Network) SendInstallSnapshot(ctx context.Context, from, to raft.NodeID, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	var resp raft.InstallSnapshotResponse
	err := n.dispatch(ctx, from, to, func(h raft.Handler) error {
		resp = h.HandleInstallSnapshot(req)
		return nil
	})
	return resp, err
}

// Transport 把 Network 包装成单个节点视角的 raft.Transport（自动填 from）。
type Transport struct {
	net  *Network
	from raft.NodeID
}

// NewTransport 返回绑定 from 节点的传输端。
func NewTransport(n *Network, from raft.NodeID) *Transport {
	return &Transport{net: n, from: from}
}

func (t *Transport) SendRequestVote(ctx context.Context, to raft.NodeID, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return t.net.SendRequestVote(ctx, t.from, to, req)
}

func (t *Transport) SendAppendEntries(ctx context.Context, to raft.NodeID, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return t.net.SendAppendEntries(ctx, t.from, to, req)
}

func (t *Transport) SendInstallSnapshot(ctx context.Context, to raft.NodeID, req raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	return t.net.SendInstallSnapshot(ctx, t.from, to, req)
}

var _ raft.Transport = (*Transport)(nil)

// Peers 返回在线节点集合的快照（观测辅助）。
func (n *Network) Peers() []raft.NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]raft.NodeID, 0, len(n.nodes))
	for id := range n.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
