package raft

import (
	"context"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// changePhase is the stage of the non-blocking membership state machine.
type changePhase int

const (
	changeNone         changePhase = iota
	changeStageLearner             // learner config entry appended, awaiting commit
	changeWaitCatchup              // learner committed, awaiting its match to reach tail
	changeJoint                    // joint config appended, awaiting commit
	changeFinal                    // final config appended (auto on joint commit), awaiting commit
)

// pendingChange is the single in-flight membership transition. Everything it
// touches is mutated only on the event loop; the state machine never blocks
// the loop and therefore never steals the loop's inbound events.
type pendingChange struct {
	id        string
	add       bool
	phase     changePhase
	joint     raftproto.Config
	final     raftproto.Config
	needStage bool // brand-new server must first become a learner
	deadline  time.Time
	reply     chan error
}

// startChangeMembership validates and kicks off an asynchronous change.
func (n *Node) startChangeMembership(ctx context.Context, id string, add bool, reply chan error) {
	if n.state != raftproto.StateLeader {
		reply <- ErrNotLeader
		return
	}
	if n.mchange != nil {
		reply <- ErrChangePending
		return
	}
	cur := cloneCfg(n.commCfg)
	if add {
		if contains(cur.Voters, id) {
			reply <- nil // idempotent: already a voter
			return
		}
		needStage := !contains(cur.Learners, id) && !contains(n.config.Learners, id)
		pc := &pendingChange{
			id: id, add: true, needStage: needStage,
			deadline: time.Now().Add(30 * time.Second), reply: reply,
		}
		n.mchange = pc
		n.logger.statef("membership ADD start id=%s stageLearner=%v", id, needStage)
		n.pumpChange()
		return
	}
	if !contains(cur.Voters, id) {
		reply <- ErrUnknownMember
		return
	}
	newVoters := removeString(cur.Voters, id)
	pc := &pendingChange{
		id:       id,
		add:      false,
		phase:    changeJoint,
		joint:    raftproto.Config{Voters: newVoters, JointVoters: cur.Voters, Learners: cur.Learners},
		final:    raftproto.Config{Voters: newVoters, Learners: cur.Learners},
		deadline: time.Now().Add(30 * time.Second),
		reply:    reply,
	}
	n.mchange = pc
	n.changeInProgress = true
	n.logger.statef("membership REMOVE start id=%s joint=%v", id, pc.joint.Voters)
	n.pumpChange()
}

// pumpChange advances the state machine one stage when its precondition now
// holds. It is invoked after every commit/apply and append-response.
func (n *Node) pumpChange() {
	pc := n.mchange
	if pc == nil || n.state != raftproto.StateLeader {
		return
	}
	if time.Now().After(pc.deadline) {
		n.failChange(ErrNoQuorum)
		return
	}
	switch pc.phase {
	case changeNone, changeStageLearner:
		// Append a learner config entry (idempotent if tail already has it).
		cfg := cloneCfg(n.config)
		if contains(cfg.Voters, pc.id) {
			n.failChange(ErrAlreadyMember)
			return
		}
		if !contains(cfg.Learners, pc.id) {
			cfg.Learners = append(append([]string{}, cfg.Learners...), pc.id)
			if _, err := n.appendConfigEntry(cfg); err != nil {
				n.failChange(err)
				return
			}
		}
		n.changeInProgress = true
		pc.phase = changeWaitCatchup
		// If the learner entry is already committed we can proceed now.
		n.pumpChange()

	case changeWaitCatchup:
		// Wait until the learner config is committed and the learner's match
		// reached the tail at the time of promotion, so an empty voter is
		// never added.
		learnerCommitted := false
		for i := n.snapMeta.Index + 1; i <= n.commit; i++ {
			ents, err := n.log.Entries(i, i+1)
			if err != nil {
				break
			}
			if len(ents) == 1 && ents[0].Type == raftproto.EntryConfig {
				if c, err := decodeConfig(ents[0].Data); err == nil && contains(c.Learners, pc.id) {
					learnerCommitted = true
				}
			}
		}
		if pc.needStage && !learnerCommitted {
			return
		}
		p := n.peers[pc.id]
		target := n.promotionTail(pc)
		if p == nil || p.matchIndex < target {
			if p != nil {
				n.sendAppend(pc.id, 0)
			}
			return
		}
		// Promote: build joint config from the currently committed voters.
		cur := cloneCfg(n.commCfg)
		newVoters := append(append([]string{}, cur.Voters...), pc.id)
		pc.joint = raftproto.Config{
			Voters:      newVoters,
			JointVoters: cur.Voters,
			Learners:    removeString(cur.Learners, pc.id),
		}
		pc.final = raftproto.Config{
			Voters:   newVoters,
			Learners: removeString(cur.Learners, pc.id),
		}
		pc.phase = changeJoint
		n.logger.statef("learner %s caught up at %d; promoting via joint config", pc.id, p.matchIndex)
		n.pumpChange()

	case changeJoint:
		// If the joint entry is already the active config tail, wait for its
		// commit; otherwise append it.
		if !n.activeTailIs(pc.joint) && !n.configAtLeast(pc.joint) {
			if _, err := n.appendConfigEntry(pc.joint); err != nil {
				n.failChange(err)
				return
			}
		}
		if n.committedConfigMatches(pc.joint) {
			pc.phase = changeFinal
			n.pumpChange()
		}

	case changeFinal:
		// The final entry is appended automatically by onConfigCommitted once
		// the joint entry commits. Just wait for the final config to commit.
		if n.committedConfigMatches(pc.final) {
			n.logger.statef("membership change finalized: voters=%v learners=%v",
				pc.final.Voters, pc.final.Learners)
			n.changeInProgress = false
			n.mchange = nil
			select {
			case pc.reply <- nil:
			default:
			}
		}
	}
}

// promotionTail returns the index the learner must hold before promotion.
func (n *Node) promotionTail(pc *pendingChange) uint64 {
	// Require the tail at the moment the learner config committed, captured
	// simply as the current leader last index (heartbeats keep it tight).
	return n.lastIndex()
}

// activeTailIs reports whether the newest log entry is exactly cfg.
func (n *Node) activeTailIs(cfg raftproto.Config) bool {
	last := n.lastIndex()
	if last == 0 {
		return false
	}
	ents, err := n.log.Entries(last, last+1)
	if err != nil || len(ents) != 1 || ents[0].Type != raftproto.EntryConfig {
		return false
	}
	c, err := decodeConfig(ents[0].Data)
	return err == nil && sameConfig(c, cfg)
}

// configAtLeast reports whether cfg already appears as a config entry in the
// available log (avoid duplicate appends after a restart/redirect).
func (n *Node) configAtLeast(cfg raftproto.Config) bool {
	for i := n.firstIndex(); i <= n.lastIndex(); i++ {
		ents, err := n.log.Entries(i, i+1)
		if err != nil {
			return false
		}
		if len(ents) == 1 && ents[0].Type == raftproto.EntryConfig {
			if c, err := decodeConfig(ents[0].Data); err == nil && sameConfig(c, cfg) {
				return true
			}
		}
	}
	return false
}

// committedConfigMatches reports the committed config equals cfg.
func (n *Node) committedConfigMatches(cfg raftproto.Config) bool {
	return sameConfig(n.commCfg, cfg)
}

// failChange aborts the current change with an explicit reason.
func (n *Node) failChange(err error) {
	pc := n.mchange
	if pc == nil {
		return
	}
	n.logger.warnf("membership change for %s failed: %v", pc.id, err)
	n.changeInProgress = false
	n.mchange = nil
	select {
	case pc.reply <- err:
	default:
	}
}

// changeMembership is the synchronous API wrapper; it launches the async
// state machine on the loop and awaits its terminal result without blocking
// the loop itself.
func (n *Node) changeMembership(ctx context.Context, id string, add bool) error {
	reply := make(chan error, 1)
	if !n.enqueue(ctx, changeEvent{id: id, add: add, reply: reply}) {
		return ErrStopped
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrStopped
	}
}

func removeString(xs []string, x string) []string {
	out := make([]string, 0, len(xs))
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}

func sameVoters(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !contains(b, x) {
			return false
		}
	}
	return true
}
