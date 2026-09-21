package raft

import "errors"

// 可区分的错误原因：调用方（含集群编排层与测试）可据此判定故障类别。
var (
	// ErrNotLeader 当前节点不是领导者，或当前无已知领导者。
	ErrNotLeader = errors.New("raft: not leader")
	// ErrNoQuorum 多数派在超时时间内不可达，提交无法推进。
	ErrNoQuorum = errors.New("raft: no quorum")
	// ErrLeadershipLost 提案等待提交期间领导权易主或任期推进。
	ErrLeadershipLost = errors.New("raft: leadership lost while waiting for commit")
	// ErrJointChangeInProgress 上一次联合配置尚未稳定，拒绝新的成员变更。
	ErrJointChangeInProgress = errors.New("raft: joint configuration change in progress")
	// ErrInvalidConfig 成员变更目标配置非法（空配置、移除不存在节点等）。
	ErrInvalidConfig = errors.New("raft: invalid configuration")
	// ErrStopped 节点已停止，不再处理请求。
	ErrStopped = errors.New("raft: node stopped")
	// ErrCorruptStorage 持久化文件内容校验失败（CRC/魔数不符），节点拒绝以不确定状态启动。
	ErrCorruptStorage = errors.New("raft: corrupt persistent storage")
	// ErrTruncatedStorage 持久化文件尾部记录不完整，状态可能被截断，节点拒绝启动。
	ErrTruncatedStorage = errors.New("raft: truncated persistent storage")
	// ErrSnapshotBehind 快照落后于当前日志，不能用于恢复。
	ErrSnapshotBehind = errors.New("raft: snapshot behind log")
	// ErrGap 日志区间出现空洞（压缩/清理越界）。
	ErrGap = errors.New("raft: log gap")
)
