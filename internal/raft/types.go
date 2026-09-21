package raft

import "time"

// NodeID 节点标识。
type NodeID string

// Index 日志下标（1 起，0 表示无）。
type Index uint64

// Term 领导权任期。
type Term uint64

// Role 节点角色。
type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "follower"
	}
}

// EntryKind 日志条目类别。
type EntryKind uint8

const (
	EntryNormal EntryKind = iota
	EntryConf
	// EntryAddNonVoter 复制集预告条目（非投票）：提交后领导者立即把该
	// 节点纳入复制目标（发快照+增量），让其在正式获得投票权前完成追赶。
	// 它不改变投票配置，因此不影响多数派计算。
	EntryAddNonVoter
)

// NonVoterTarget 从条目中取出待加入的复制目标（存于 Data）。
func (e LogEntry) nonVoterTarget() NodeID { return NodeID(e.Data) }

// ConfigSet 一组投票成员。
type ConfigSet map[NodeID]struct{}

// Config 已生效（或联合变更中）的投票配置。
// Joint 为 nil 时为普通配置；Joint 非 nil 时处于联合共识：
// Voters 为旧配置 C_old，Joint 为新配置 C_new，提交需同时满足两边多数。
type Config struct {
	Voters ConfigSet
	Joint  ConfigSet
}

// LogEntry 单条日志。
type LogEntry struct {
	Index Index
	Term  Term
	Kind  EntryKind

	// 幂等去重：同一 ClientID 下 Seq 单调递增，重复提案只生效一次。
	ClientID string
	Seq      uint64

	// Normal 条目的不透明命令载荷。
	Data []byte
	// Conf 条目的配置载荷。Joint 非 nil 表示进入联合配置，nil 表示落到新配置。
	Conf *Config
}

// HardState 必须在响应前持久化的状态。
type HardState struct {
	Term Term
	Vote NodeID
	// CommitIndex 最近已提交位置：无快照崩溃重启时据此恢复提交下界，
	// 保证"已对外确认的结果不得丢失"。
	CommitIndex Index
}

// SnapshotMeta 快照元数据。
type SnapshotMeta struct {
	LastIndex Index
	LastTerm  Term
	Config    Config
}

// PeerProgress 领导者视角下单个跟随者的复制进度。
type PeerProgress struct {
	NextIndex    Index
	MatchIndex   Index
	readAck      bool // 当前 ReadIndex 轮次是否已确认本任期领导权
	snapshotting bool // 是否正在通过 InstallSnapshot 追赶
}

// NodeStatus 只读观测快照，不包含可写字段，观测不影响集群行为。
type NodeStatus struct {
	ID           NodeID
	Role         Role
	Term         Term
	Leader       NodeID
	Voter        bool
	CommitIndex  Index
	LastIndex    Index
	AppliedIndex Index
	// Config 最近一条"已提交"配置（投票/快照依据）。
	Config Config
	// LatestConfig 日志中最新配置（可能为已追加但尚未提交的联合配置），
	// 用于观测"变更是否已进入但尚未稳定"。
	LatestConfig Config
	Progress     map[NodeID]PeerProgress
}

// StateMachine 复制状态机接口。Apply 必须确定性、可重放，
// 并在内部对 (ClientID, Seq) 做幂等去重。
type StateMachine interface {
	Apply(e LogEntry) (duplicated bool, err error)
	Snapshot() ([]byte, error)
	Restore(data []byte) error
	AppliedIndex() Index
}

// Options 节点时序参数。
type Options struct {
	ElectionMinTicks  int
	ElectionMaxTicks  int
	HeartbeatTicks    int
	SnapshotThreshold uint64
	RPCTimeout        time.Duration
}
