// Package raftproto defines the wire-level and persistent primitive types
// shared by the log store, transport and raft state-machine packages.
package raftproto

// EntryType distinguishes normal data entries from configuration entries.
type EntryType int

const (
	// EntryNormal is an opaque client command.
	EntryNormal EntryType = iota
	// EntryConfig is a serialized membership configuration.
	EntryConfig
)

// Entry is a single log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// Member describes one cluster node.
type Member struct {
	ID      string
	Learner bool // replicated but never votes / counted in quorums
}

// Config is a membership configuration. When Joint is non-empty the node is
// inside a joint consensus transition: quorums of both Voters and JointVoters
// are required.
type Config struct {
	Voters      []string // committed voters
	Learners    []string
	JointVoters []string // old voters during a joint transition; empty otherwise
}

// HardState is the durable state that must survive a crash before responding.
// Apply is the highest index already applied to the local state machine; it is
// persisted so a restart never re-applies committed entries.
type HardState struct {
	Term    uint64
	VoteFor string
	Commit  uint64
	Apply   uint64
}

// SnapshotMeta identifies an existing snapshot.
type SnapshotMeta struct {
	Index  uint64
	Term   uint64
	Config Config
}

// NodeState is the role of a node.
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// AppendEntriesRequest is the log replication / heartbeat RPC.
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesResponse is the reply to AppendEntries.
type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64 // hint for the next backtrack position
	ConflictTerm  uint64
}

// RequestVoteRequest starts an election.
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse is the reply to RequestVote.
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// InstallSnapshotRequest streams a snapshot to a lagging peer.
type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
	Config            Config
	Done              bool
}

// InstallSnapshotResponse is the reply to InstallSnapshot.
type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}
