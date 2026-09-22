package raft

import (
	"context"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// Every interaction with node state is an event handled on the single loop.
type event interface{ tag() }

type tickEvent struct{}

type proposeEvent struct {
	cmd   []byte
	reply chan applyResult
}

type readEvent struct {
	key   string
	reply chan readReply
}

type statusEvent struct {
	reply chan Status
}

type changeEvent struct {
	id    string
	add   bool
	reply chan error
}

// RPCs delivered by the transport are queued; the loop answers synchronously
// and the handler wrapper delivers the response back over the network call.

type appendReqEvent struct {
	ctx   context.Context
	from  string
	req   *raftproto.AppendEntriesRequest
	reply chan rpcReply[*raftproto.AppendEntriesResponse]
}

type voteReqEvent struct {
	ctx   context.Context
	from  string
	req   *raftproto.RequestVoteRequest
	reply chan rpcReply[*raftproto.RequestVoteResponse]
}

type snapReqEvent struct {
	ctx   context.Context
	from  string
	req   *raftproto.InstallSnapshotRequest
	reply chan rpcReply[*raftproto.InstallSnapshotResponse]
}

type rpcReply[T any] struct {
	v   T
	err error
}

// Responses coming back from detached sender goroutines.

type appendRespEvent struct {
	peer string
	req  *raftproto.AppendEntriesRequest
	resp *raftproto.AppendEntriesResponse
	err  error
	gen  uint64 // read-index generation if this broadcast was a read probe
}

type voteRespEvent struct {
	peer string
	gen  uint64
	req  *raftproto.RequestVoteRequest
	resp *raftproto.RequestVoteResponse
	err  error
}

type snapRespEvent struct {
	peer string
	req  *raftproto.InstallSnapshotRequest
	resp *raftproto.InstallSnapshotResponse
	err  error
}

// appliedEvent is produced by the apply worker after committing.
type appliedEvent struct {
	index   uint64
	results map[uint64]statemachineResult
}

// statemachineResult avoids an import alias hack in the event definition.
type statemachineResult struct {
	ok    bool
	value string
	dup   bool
	err   error
}

func (tickEvent) tag()       {}
func (proposeEvent) tag()    {}
func (readEvent) tag()       {}
func (statusEvent) tag()     {}
func (changeEvent) tag()     {}
func (appendReqEvent) tag()  {}
func (voteReqEvent) tag()    {}
func (snapReqEvent) tag()    {}
func (appendRespEvent) tag() {}
func (voteRespEvent) tag()   {}
func (snapRespEvent) tag()   {}
func (appliedEvent) tag()    {}
