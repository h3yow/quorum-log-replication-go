package raft

import (
	"context"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// run is the node's single-threaded event loop. Timers produce synthetic
// tick events; network I/O happens in goroutines that feed responses back.
func (n *Node) run(ctx context.Context) {
	defer close(n.doneCh)
	n.logger.infof("event loop start: state=%s term=%d applied=%d commit=%d",
		n.state, n.term, n.applied, n.commit)

	tick := n.cfg.TickInterval
	if tick <= 0 {
		tick = 25 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			n.logger.infof("event loop stopping: context done")
			return
		case <-n.stopCh:
			n.logger.infof("event loop stopping: Stop()")
			return
		case <-ticker.C:
			n.tick()
			n.refreshObs()
		case e := <-n.eventCh:
			n.step(ctx, e)
			n.refreshObs()
		case <-n.wakeCh:
			// immediate kick after actions that want prompt replication
		}
	}
}

// tick advances timer-based state without blocking the loop.
func (n *Node) tick() {
	n.electionElapsed++
	n.heartbeatElapsed++
	switch n.state {
	case raftproto.StateLeader:
		if n.heartbeatElapsed >= max1(n.cfg.HeartbeatTick) {
			n.heartbeatElapsed = 0
			n.broadcastAppend(0)
		}
		n.pumpChange()
	default:
		// followers and candidates both time out into a new election.
		if n.electionElapsed >= n.electionTimeout {
			n.electionElapsed = 0
			n.campaign()
		}
	}
}

func max1(x int) int {
	if x <= 0 {
		return 1
	}
	return x
}

// step dispatches one event.
func (n *Node) step(ctx context.Context, e event) {
	switch ev := e.(type) {
	case proposeEvent:
		n.onPropose(ev)
	case readEvent:
		n.onRead(ctx, ev)
	case statusEvent:
		ev.reply <- n.getStatus()
	case changeEvent:
		n.startChangeMembership(ctx, ev.id, ev.add, ev.reply)
	case appendReqEvent:
		resp := n.handleAppendEntries(ev.from, ev.req)
		ev.reply <- rpcReply[*raftproto.AppendEntriesResponse]{v: resp}
	case voteReqEvent:
		resp := n.handleRequestVote(ev.from, ev.req)
		ev.reply <- rpcReply[*raftproto.RequestVoteResponse]{v: resp}
	case snapReqEvent:
		resp := n.handleInstallSnapshot(ev.from, ev.req)
		ev.reply <- rpcReply[*raftproto.InstallSnapshotResponse]{v: resp}
	case appendRespEvent:
		n.onAppendResponse(ev)
	case voteRespEvent:
		n.onVoteResponseEvent(ev)
	case snapRespEvent:
		n.onSnapResponse(ev)
	}
}
