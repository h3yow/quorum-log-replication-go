package drill

import (
	"context"
	"time"
)

// runMembershipLeaderSwitch:
//
//  1. elect a leader on a 5-voter cluster and commit a prefix;
//  2. add a brand-new node; inject a fixed delivery latency on the
//     leader->newnode link so its learner catch-up (a genuine phase of the
//     membership change) cannot finish instantly, deterministically widening
//     the "change in progress" window;
//  3. wait until the new node is observably staged as a learner on the
//     leader (proof the change really started) but not yet a voter;
//  4. crash the leader in that window;
//  5. wait for the surviving majority to elect a higher-term leader;
//  6. resume/complete AddNode through the new leader (it must never be
//     half-applied); the final committed voter set must include the new node
//     and writes must keep committing.
//
// Sabotage mode leaves the original leader alive, so the claimed switch
// never happens and the drill must FAIL.
func runMembershipLeaderSwitch(ctx context.Context, h *harness, started time.Time) (*Report, error) {
	old, err := h.waitLeader(5 * time.Second)
	if err != nil {
		return nil, err
	}
	pre, err := h.put("drill-mem", 1, "mem-pre", "1")
	if err != nil {
		return nil, err
	}
	if _, err := h.proposeOnce(ctx, old, pre); err != nil {
		return nil, err
	}

	newID := "m6"
	h.setNewVoter(newID)

	var checks []CheckResult

	// Register the on-disk node first (still outside the config), then slow
	// inbound RPC delivery to it so learner catch-up takes a long, observable
	// window once the change starts.
	if err := func() error {
		// openNode is unexported; go through AddNode but only after the latency
		// rule exists. AddNode opens the node itself, so set latency first for
		// the not-yet-registered peer: the rule persists in the rule map keyed
		// by id regardless of registration.
		h.c.Transport().SetLatency(old, newID, 1200)
		return nil
	}(); err != nil {
		return nil, err
	}

	// Start the change asynchronously; it will stage m6 as learner and block
	// on the slowed catch-up.
	done := make(chan error, 1)
	go func() { done <- h.c.AddNode(ctx, newID) }()

	// Evidence: the change genuinely started and is in progress (learner
	// staged, not yet voter).
	phase := "none"
	h.poll(3*time.Second, func() bool {
		s := h.status(old)
		for _, l := range s.ActiveConfig.Learners {
			if l == newID {
				phase = "learner-catchup"
				return true
			}
		}
		// It may already have advanced to the joint configuration.
		if len(s.ActiveConfig.JointVoters) > 0 {
			for _, v := range s.ActiveConfig.Voters {
				if v == newID {
					phase = "joint-consensus"
					return true
				}
			}
		}
		return false
	})
	changeStarted := phase == "learner-catchup"
	if changeStarted {
		checks = append(checks, checkPass(
			"change-in-progress",
			"new node staged as learner before the leader switch; membership change underway",
		))
	} else {
		checks = append(checks, checkFail(
			"change-in-progress",
			"no evidence the membership change started before the switch",
		))
	}

	// Crash the in-change leader for real (sabotage: do not crash).
	if !h.sabotage {
		if err := h.crashNode(ctx, old); err != nil {
			return nil, err
		}
	}
	offline := !h.c.Transport().Registered(old)
	if offline {
		checks = append(checks, checkPass(
			"old-leader-offline",
			"leader performing the change unregistered from the transport",
		))
	} else {
		checks = append(checks, checkFail(
			"old-leader-offline",
			"claimed leader switch never removed the original leader",
		))
	}

	// Surviving majority elects a distinct higher-term leader.
	var newLeader string
	oldTerm := h.status(old).Term
	h.poll(6*time.Second, func() bool {
		ld := h.c.Leader()
		if ld == "" || ld == old {
			return false
		}
		return h.status(ld).Term > oldTerm
	})
	if ld := h.c.Leader(); ld != "" && ld != old && h.status(ld).Term > oldTerm {
		newLeader = ld
	}
	switched := newLeader != ""
	if switched {
		checks = append(checks, checkPass(
			"leadership-switch",
			"surviving majority seated a distinct leader at a strictly higher term",
		))
	} else {
		checks = append(checks, checkFail(
			"leadership-switch",
			"no distinct higher-term leader after the claimed crash",
		))
	}

	// Remove the artificial latency so catch-up completes promptly under the
	// new leader, then resume the change. AddNode is retry-safe after a switch.
	h.c.Transport().SetLatency("", newID, 0)
	h.c.Transport().SetLatency(old, newID, 0)
	if switched {
		// Drain or abandon the first attempt, then converge via the new leader.
		select {
		case <-done:
		case <-time.After(200 * time.Millisecond):
		}
		if err := h.c.AddNode(ctx, newID); err != nil {
			checks = append(checks, checkFail("change-completes", err.Error()))
		} else {
			checks = append(checks, checkPass(
				"change-completes",
				"membership change resumed cleanly and committed; no half-update",
			))
		}
	}

	// Final committed voters must include the new node on every node.
	finalVoters := []string{}
	convergedVoters := h.poll(6*time.Second, func() bool {
		obs := h.c.Observe()
		want := append(append([]string{}, h.ids...), newID)
		for id, s := range obs.Nodes {
			if !h.c.Transport().Registered(id) {
				continue
			}
			if !sameSet(s.CommittedConfig.Voters, want) || len(s.CommittedConfig.JointVoters) != 0 {
				return false
			}
		}
		return true
	})
	if convergedVoters {
		finalVoters = append(append([]string{}, h.ids...), newID)
		checks = append(checks, checkPass(
			"final-config-committed",
			"every live node committed the non-joint voter set including the new member",
		))
	} else {
		checks = append(checks, checkFail(
			"final-config-committed",
			"final voter set did not commit cleanly everywhere (half-update risk)",
		))
	}

	// Writes continue on the post-change cluster; restart old leader too.
	if offline {
		if rerr := h.restartNode(ctx, old); rerr != nil {
			checks = append(checks, checkFail("old-leader-restart", rerr.Error()))
		}
	}
	if ld := h.c.Leader(); ld != "" {
		post, _ := h.put("drill-mem", 2, "mem-post", "2")
		if _, err := h.proposeOnce(ctx, ld, post); err != nil {
			checks = append(checks, checkFail("post-change-write", err.Error()))
		} else {
			checks = append(checks, checkPass(
				"post-change-write",
				"fixed write commits after the interrupted membership change",
			))
			h.recordWrite("mem-post", "post-change", "accepted", true, false)
		}
	}

	mf := &MembershipFact{
		NewVoterRole: roleNewVoter,
		PhaseAtCrash: phase,
		// FinalVoterSet is rendered in canonical-role form at reattach time,
		// after buildReport assigns roles; physical ids ride on the harness.
		OldLeaderRole: roleInitialLeader,
		NewLeaderRole: roleNewLeader,
		NewTermHigher: switched,
	}
	h.finalVoterIDs = append([]string(nil), finalVoters...)

	rep, err := h.finish(checks, 8*time.Second, started)
	if err != nil {
		return nil, err
	}
	rep.Facts.MembershipEvidence = mf
	if err := h.finalizeEvidence(rep); err != nil {
		return nil, err
	}
	return rep, nil
}

// sameSet reports string-set equality.
func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		found := false
		for _, y := range b {
			if x == y {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
