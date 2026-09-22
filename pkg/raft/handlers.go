package raft

import (
	"context"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// The transport invokes these methods from its own goroutines. They merely
// queue onto the node's event loop and wait for the serialized answer, so
// no raft state is ever read/written concurrently.

func (n *Node) HandleAppendEntries(ctx context.Context, from string, req *raftproto.AppendEntriesRequest) (*raftproto.AppendEntriesResponse, error) {
	reply := make(chan rpcReply[*raftproto.AppendEntriesResponse], 1)
	if !n.enqueue(ctx, appendReqEvent{ctx: ctx, from: from, req: req, reply: reply}) {
		return nil, ErrStopped
	}
	select {
	case r := <-reply:
		return r.v, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) HandleRequestVote(ctx context.Context, from string, req *raftproto.RequestVoteRequest) (*raftproto.RequestVoteResponse, error) {
	reply := make(chan rpcReply[*raftproto.RequestVoteResponse], 1)
	if !n.enqueue(ctx, voteReqEvent{ctx: ctx, from: from, req: req, reply: reply}) {
		return nil, ErrStopped
	}
	select {
	case r := <-reply:
		return r.v, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) HandleInstallSnapshot(ctx context.Context, from string, req *raftproto.InstallSnapshotRequest) (*raftproto.InstallSnapshotResponse, error) {
	reply := make(chan rpcReply[*raftproto.InstallSnapshotResponse], 1)
	if !n.enqueue(ctx, snapReqEvent{ctx: ctx, from: from, req: req, reply: reply}) {
		return nil, ErrStopped
	}
	select {
	case r := <-reply:
		return r.v, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
