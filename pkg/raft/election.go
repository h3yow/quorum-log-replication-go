package raft

import (
	"context"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// campaign starts a new term as candidate, persists term+vote before any
// network action (the vote must survive a crash), and solicits votes from
// every voter in the currently applicable configuration.
func (n *Node) campaign() {
	if isLearner(n.cfg.ID, n.config) {
		// Learners never campaign: they have no vote and cannot disrupt a
		// majority that excludes them.
		return
	}
	if !isVoter(n.cfg.ID, n.config) {
		return
	}
	// Pre-vote guard (without an extra RPC): a node that cannot currently
	// reach a quorum of the active voter sets must not increment its term.
	// This keeps an isolated minority from churning terms and disrupting the
	// majority's stable leadership across a partition.
	targets := n.campaignTargets()
	reachable := n.tr.ReachableVoters(n.cfg.ID, targets)
	if len(reachable) < quorum(len(targets)) {
		n.logger.infof("election timeout but only %d/%d voters reachable; staying follower (pre-vote guard)",
			len(reachable), quorum(len(targets)))
		n.electionElapsed = 0
		n.resetElectionTimeout()
		return
	}
	n.term++
	n.state = raftproto.StateCandidate
	n.vote = n.cfg.ID
	n.lead = ""
	n.voteGen++
	gen := n.voteGen
	n.votes = map[string]bool{n.cfg.ID: true}
	if err := n.persistHardState(); err != nil {
		n.logger.warnf("campaign: persist term/vote: %v", err)
		return
	}
	n.resetElectionTimeout()
	n.logger.statef("become candidate term=%d gen=%d reason=election timeout; requesting votes voters=%v joint=%v",
		n.term, gen, n.config.Voters, n.config.JointVoters)

	req := &raftproto.RequestVoteRequest{
		Term:         n.term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	// Single-node cluster: win immediately.
	if n.hasQuorum(n.votes) {
		n.becomeLeader()
		return
	}
	for _, id := range n.campaignTargets() {
		if id == n.cfg.ID {
			continue
		}
		go n.sendVote(id, gen, req)
	}
}

func (n *Node) campaignTargets() []string {
	seen := map[string]bool{}
	var out []string
	for _, v := range n.config.Voters {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	for _, v := range n.config.JointVoters {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func (n *Node) hasQuorum(grant map[string]bool) bool {
	return hasMajority(grant, n.config)
}

func (n *Node) persistHardState() error {
	return n.log.SaveHardState(raftproto.HardState{
		Term:    n.term,
		VoteFor: n.vote,
		Commit:  n.commit,
		Apply:   n.applied,
	})
}

// sendVote performs one RequestVote RPC with a bounded timeout.
func (n *Node) sendVote(peer string, gen uint64, req *raftproto.RequestVoteRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout())
	defer cancel()
	resp, err := n.tr.SendRequestVote(ctx, n.cfg.ID, peer, req)
	n.enqueue(context.Background(), voteRespEvent{peer: peer, gen: gen, req: req, resp: resp, err: err})
}

func (n *Node) rpcTimeout() time.Duration {
	if n.cfg.RPCTimeout > 0 {
		return n.cfg.RPCTimeout
	}
	return 300 * time.Millisecond
}

// onVoteResponseEvent handles a vote reply on the loop.
func (n *Node) onVoteResponseEvent(ev voteRespEvent) {
	if ev.err != nil {
		n.logger.infof("vote to/from %s failed: %v", ev.peer, ev.err)
		return
	}
	r := ev.resp
	if r.Term > n.term {
		n.logger.statef("vote reply from %s reveals term %d > %d; stepping down", ev.peer, r.Term, n.term)
		n.becomeFollower(r.Term, "")
		return
	}
	if n.state != raftproto.StateCandidate || r.Term != n.term {
		return
	}
	// Discard replies from an earlier election round (stale in-flight request
	// or a same-term retry); they must not seat this candidate.
	if ev.gen != n.voteGen {
		n.logger.infof("ignoring stale vote reply from %s gen=%d current=%d",
			ev.peer, ev.gen, n.voteGen)
		return
	}
	if r.VoteGranted {
		n.votes[ev.peer] = true
		n.logger.infof("granted vote from %s (%d votes)", ev.peer, len(n.votes))
		if n.hasQuorum(n.votes) {
			n.becomeLeader()
		}
	}
}

// become transitions the role, logging the explicit reason for each
// transition so state changes are explainable in test logs.
func (n *Node) become(s raftproto.NodeState, term uint64, reason string) {
	if term > n.term {
		n.term = term
		n.vote = ""
	}
	n.state = s
	n.electionElapsed = 0
	switch s {
	case raftproto.StateFollower:
		n.lead = ""
	case raftproto.StateLeader:
		n.becomeLeader()
		return
	case raftproto.StateCandidate:
		n.lead = ""
	}
	n.logger.statef("role -> %s term=%d reason=%s", s, n.term, reason)
}

func (n *Node) becomeFollower(term uint64, leader string) {
	n.state = raftproto.StateFollower
	if term > n.term {
		n.term = term
		n.vote = ""
		n.refusedInTerm = 0
		n.refusedCandidates = nil
	}
	n.lead = leader
	n.electionElapsed = 0
	n.resetElectionTimeout()
	if err := n.persistHardState(); err != nil {
		n.logger.warnf("becomeFollower: persist: %v", err)
	}
	n.logger.statef("role -> follower term=%d leader=%s", n.term, leader)
	if n.mchange != nil {
		// Leadership was lost (or a higher term arrived). Abort the parked
		// change; the client may safely retry against the new leader. Any
		// config entries already appended are still reconciled through the
		// normal log, never half-applied.
		n.failChange(ErrNotLeader)
	}
}

func (n *Node) becomeLeader() {
	n.state = raftproto.StateLeader
	n.lead = n.cfg.ID
	n.electionElapsed = 0
	n.heartbeatElapsed = 0
	n.reinitProgress()
	// A no-op at the start of the term defends read-index correctness and
	// commits pending entries once this term reaches a quorum.
	n.appendNoop()
	n.logger.statef("role -> LEADER term=%d reason=majority votes; voters=%v",
		n.term, n.config.Voters)
	n.broadcastAppend(0)
}

// grantVote implements the Raft voter rules: one vote per term, only for a
// candidate whose log is at least as up-to-date, only to an eligible node.
func (n *Node) grantVote(candidate string, term, lastIndex, lastTerm uint64) bool {
	if term < n.term {
		return false
	}
	// Up-to-date check happens BEFORE adopting the candidate's term. Adopting
	// the term first would persist a vote-less higher term and, on a repeated
	// request, let a candidate whose log was already rejected collect a vote
	// in a term where it was once refused — which can seat a leader with a
	// stale log and allow committed entries to be overwritten.
	upToDate := lastTerm > n.lastTerm() ||
		(lastTerm == n.lastTerm() && lastIndex >= n.lastIndex())
	if !upToDate {
		// Acknowledge a higher term (so we cannot disrupt a valid leader) but
		// remember this candidate was refused in this term. A repeated request
		// from the same stale candidate must keep failing even though we have
		// not cast our vote yet; a genuinely more up-to-date candidate may
		// still receive our vote later in this term.
		if term > n.term {
			n.becomeFollower(term, "")
		}
		n.refusedInTerm = term
		n.refusedCandidates = append(n.refusedCandidates, candidate)
		n.logger.infof("rejecting vote for %s: stale log (%d/%d vs %d/%d)",
			candidate, lastTerm, lastIndex, n.lastTerm(), n.lastIndex())
		return false
	}
	if term > n.term {
		n.becomeFollower(term, "")
	}
	// Deterministically repeat a refusal for a candidate already rejected in
	// this exact term.
	if term == n.refusedInTerm {
		for _, rc := range n.refusedCandidates {
			if rc == candidate {
				n.logger.infof("rejecting vote for %s: already refused in term %d (stale log)",
					candidate, term)
				return false
			}
		}
	}
	if n.vote != "" && n.vote != candidate {
		n.logger.infof("rejecting vote for %s: already voted for %s in term %d",
			candidate, n.vote, n.term)
		return false
	}
	if n.vote == candidate {
		// Idempotent repeat of a previously granted vote in this term.
		return true
	}
	n.vote = candidate
	if err := n.persistHardState(); err != nil {
		n.logger.warnf("grantVote: persist: %v", err)
		return false
	}
	n.electionElapsed = 0
	n.resetElectionTimeout()
	n.logger.infof("granted vote to %s for term %d", candidate, term)
	return true
}

// handleRequestVote is the synchronous RequestVote path.
func (n *Node) handleRequestVote(from string, req *raftproto.RequestVoteRequest) *raftproto.RequestVoteResponse {
	// Candidates outside the current config cannot win this node's vote.
	if !isVoter(req.CandidateID, n.config) && req.CandidateID != n.cfg.ID {
		n.logger.infof("rejecting vote for non-voter %s", req.CandidateID)
		return &raftproto.RequestVoteResponse{Term: n.term, VoteGranted: false}
	}
	granted := n.grantVote(req.CandidateID, req.Term, req.LastLogIndex, req.LastLogTerm)
	return &raftproto.RequestVoteResponse{Term: n.term, VoteGranted: granted}
}
