package raft

import (
	"context"
)

// SnapshotManager 日志压缩（快照）的对外薄封装；实际逻辑挂在 Node 上，
// 以保证状态变更仍在唯一事件循环内发生。
type SnapshotManager struct {
	node *Node
}

// NewSnapshotManager 创建快照管理器。
func NewSnapshotManager(n *Node) *SnapshotManager { return &SnapshotManager{node: n} }

// MaybeCompact 检查日志长度，超过阈值则对状态机做快照并截断旧日志。
func (m *SnapshotManager) MaybeCompact() error {
	ch := make(chan error, 1)
	m.node.stepCh <- event{kind: evSync, syncFn: func() { ch <- m.node.maybeCompact() }}
	return <-ch
}

// maybeCompact 在事件循环内执行：
// 选取 <= commitIndex 的安全快照点，让状态机导出数据，原子安装快照，
// 然后把该点之前的日志从磁盘段中移除。全程不触碰未提交内容。
func (n *Node) maybeCompact() error {
	if n.opts.SnapshotThreshold == 0 {
		return nil
	}
	entries := n.commitIndex - n.baseIndex
	if uint64(entries) < n.opts.SnapshotThreshold {
		return nil
	}
	// 快照点取当前已应用位置：保证状态机数据确实覆盖到该下标。
	snapIndex := n.sm.AppliedIndex()
	if snapIndex <= n.baseIndex {
		return nil
	}
	snapTerm := n.termAt(snapIndex)
	data, err := n.sm.Snapshot()
	if err != nil {
		return err
	}
	// 快照只冻结"已提交"配置：未提交的联合/新配置绝不能进入快照，
	// 否则崩溃恢复会把未稳定的成员关系当作既成事实。
	meta := SnapshotMeta{LastIndex: snapIndex, LastTerm: snapTerm, Config: n.committedConfig.Clone()}
	if err := n.storage.CreateSnapshot(meta, data); err != nil {
		return err
	}
	n.hasSnap, n.snapMeta, n.snapData = true, meta, data
	n.baseIndex, n.baseTerm = snapIndex, snapTerm
	n.log = nil
	if last := n.storage.LastIndex(); last > n.baseIndex {
		if ents, err := n.storage.Entries(n.baseIndex+1, last+1); err == nil {
			n.log = ents
		}
	}
	n.logger.Printf("compacted: snapshot at index=%d term=%d, retained entries=%d",
		snapIndex, snapTerm, n.lastIndex()-n.baseIndex)
	return nil
}

// sendInstallSnapshot 由领导者在 follower 的 NextIndex <= baseIndex 时调用。
// 发送本地最近快照（必然 >= 该 follower 所需点），随后增量复制补齐。
func (n *Node) sendInstallSnapshot(to NodeID) error {
	pr := n.progress[to]
	if pr == nil {
		return ErrNotLeader
	}
	if pr.snapshotting {
		return nil
	}
	if !n.hasSnap {
		// 尚未有快照但 follower 需要：先就地压一份（快照点=已应用位置）。
		if err := n.maybeCompact(); err != nil {
			return err
		}
		if !n.hasSnap {
			// 已应用位置 == base（通常 follower 落后超过可发范围但无快照）：
			// 以当前提交点强制生成快照。
			if n.sm.AppliedIndex() <= n.baseIndex {
				n.logger.Printf("cannot snapshot for %s: applied at base", to)
				return nil
			}
		}
	}
	data := append([]byte(nil), n.snapData...)
	req := InstallSnapshotRequest{
		Term:       n.currentTerm,
		LeaderID:   n.id,
		Meta:       n.snapMeta,
		Data:       data,
		FinalChunk: true, // 本地模拟：快照整体一次性发送
	}
	pr.snapshotting = true
	n.inflightAppend[to] = true
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.opts.RPCTimeout*4)
		defer cancel()
		resp, err := n.tr.SendInstallSnapshot(ctx, to, req)
		n.queue(event{kind: evInstallResult, install: newInstallResult(to, req, resp, err)})
	}()
	return nil
}
