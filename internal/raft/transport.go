package raft

import "context"

// ===== Raft RPC 消息（RequestVote / AppendEntries / InstallSnapshot）=====

type RequestVoteRequest struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term
}

type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index
}

type AppendEntriesResponse struct {
	Term    Term
	Success bool
	// ConflictIndex/ConflictTerm 帮助领导者快速回退 nextIndex。
	ConflictIndex Index
	ConflictTerm  Term
}

type InstallSnapshotRequest struct {
	Term       Term
	LeaderID   NodeID
	Meta       SnapshotMeta
	Data       []byte
	FinalChunk bool
}

type InstallSnapshotResponse struct {
	Term Term
}

// Transport 本地进程内 RPC 传输抽象，由 simnet 实现：
// 可注入延迟、丢包、分区与节点宕机。
type Transport interface {
	SendRequestVote(ctx context.Context, to NodeID, req RequestVoteRequest) (RequestVoteResponse, error)
	SendAppendEntries(ctx context.Context, to NodeID, req AppendEntriesRequest) (AppendEntriesResponse, error)
	SendInstallSnapshot(ctx context.Context, to NodeID, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// Handler 由 Node 实现，Transport 将入站消息投递到这里。
type Handler interface {
	HandleRequestVote(req RequestVoteRequest) RequestVoteResponse
	HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse
	HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse
}
