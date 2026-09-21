package raft

import "context"

// 事件循环内部消息：所有外部输入（提案、读、RPC、定时器）都封装成
// event 投递到唯一的 stepCh，由同一个 goroutine 串行处理，
// 因此节点内部可变状态无需加锁，杜绝数据竞争。

type eventKind int

const (
	evTick eventKind = iota
	evPropose
	evReadIndex
	evVoteResult
	evAppendResult
	evInstallResult
	evSync
	evStop
)

type event struct {
	kind eventKind

	propose *Proposal
	read    *readReq

	vote    *rpcResult[RequestVoteRequest, RequestVoteResponse]
	append  *rpcResult[AppendEntriesRequest, AppendEntriesResponse]
	install *rpcResult[InstallSnapshotRequest, InstallSnapshotResponse]

	// syncFn 用于把网络回调同步投递进事件循环执行。
	syncFn func()
}

// readReq ReadIndex 请求句柄。
type readReq struct {
	ctx  context.Context
	ch   chan readReply
	term Term // 发起时的任期（由内部填充）
}

// Proposal 一次写入提案的句柄：提交并应用后向 resultCh 投递结果。
type Proposal struct {
	entry    LogEntry
	resultCh chan error
}

type readReply struct {
	index Index
	err   error
}

// rpcResult 异步 RPC 完成事件。
type rpcResult[Req any, Resp any] struct {
	to   NodeID
	req  Req
	resp Resp
	err  error
}

func newVoteResult(to NodeID, req RequestVoteRequest, resp RequestVoteResponse, err error) *rpcResult[RequestVoteRequest, RequestVoteResponse] {
	return &rpcResult[RequestVoteRequest, RequestVoteResponse]{to: to, req: req, resp: resp, err: err}
}

func newAppendResult(to NodeID, req AppendEntriesRequest, resp AppendEntriesResponse, err error) *rpcResult[AppendEntriesRequest, AppendEntriesResponse] {
	return &rpcResult[AppendEntriesRequest, AppendEntriesResponse]{to: to, req: req, resp: resp, err: err}
}

func newInstallResult(to NodeID, req InstallSnapshotRequest, resp InstallSnapshotResponse, err error) *rpcResult[InstallSnapshotRequest, InstallSnapshotResponse] {
	return &rpcResult[InstallSnapshotRequest, InstallSnapshotResponse]{to: to, req: req, resp: resp, err: err}
}
