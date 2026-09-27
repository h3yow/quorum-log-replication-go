package drill

import (
	"fmt"
	"time"
)

// runMembershipLeaderSwitch verifies: a membership change (adding a new voter)
// is genuinely in flight — proven by observing a joint configuration and/or a
// staged learner — when the leader is hard-crashed; after failover the change
// is driven to exactly one consistent outcome (retry commits it, or it had
// already committed), never left half-applied, and writes plus convergence
// remain correct.
func runMembershipLeaderSwitch(fr *frame) *Report {
	rc := newRecorder(ScenarioMembershipLeaderSwitch, fr.input)
	in := fr.input
	ctx := fr.ctx

	leader := fr.waitLeader(5 * time.Second)
	if leader == "" {
		rc.check("cluster-elects-leader", false, "no leader elected before drill")
		return rc.rep
	}
	oldTerm := fr.c.Node(leader).Status().Term
	rc.recordLeader("baseline", leader, oldTerm)
	rc.check("cluster-elects-leader", true,
		fmt.Sprintf("leader=%s term=%d", leader, oldTerm))

	for i := 1; i <= in.WritesBefore; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-before-%d", in.KeyPrefix, i), "1"); err != nil {
			rc.check("baseline-writes-commit", false, err.Error())
			return rc.rep
		}
	}

	// The new member id must be outside the initial voter set.
	newNode := fmt.Sprintf("n%d", in.NodeCount+1)

	// Start the add asynchronously, then wait for EVIDENCE that the change is
	// genuinely underway on the leader (joint config active, or the learner
	// staged in its log/config) before crashing it.
	done := make(chan error, 1)
	go func() { done <- fr.c.AddNode(ctx, newNode) }()

	jointObserved := false
	var jointVoters []string
	learnerStaged := false
	delayMS := in.ChangeDelayMS
	if delayMS <= 0 {
		delayMS = 200
	}
	// Sample across the whole fixed window (do not exit early at the learner
	// stage): the change passes through learner -> joint -> final, and polling
	// for the entire window captures the joint phase deterministically while
	// guaranteeing the leader is crashed before the change can finalize.
	deadline0 := time.Now().Add(time.Duration(delayMS) * time.Millisecond)
	for time.Now().Before(deadline0) {
		s := fr.c.Node(leader).Status()
		if len(s.ActiveConfig.JointVoters) > 0 {
			jointObserved = true
			jointVoters = append([]string(nil), s.ActiveConfig.JointVoters...)
		}
		if len(s.ActiveConfig.Learners) > 0 && containsStr2(s.ActiveConfig.Learners, newNode) {
			learnerStaged = true
		}
		time.Sleep(2 * time.Millisecond)
	}
	changeInFlight := jointObserved || learnerStaged
	rc.check("membership-change-in-flight", changeInFlight,
		fmt.Sprintf("joint=%t joint_voters=%v learner_staged=%t", jointObserved, jointVoters, learnerStaged))

	// Crash the leader while the change is underway.
	crashErr := fr.faults.Crash(ctx, leader)
	time.Sleep(200 * time.Millisecond)
	tookEffect := crashErr == nil && fr.c.IsDown(leader)
	rc.fault("crash", "leader-during-membership-change", []string{leader}, tookEffect,
		fmt.Sprintf("crashed=%s down=%t", leader, !tookEffect))
	rc.check("fault-leader-crashed", tookEffect,
		fmt.Sprintf("leader %s unreachable mid-change", leader))

	// Failover evidence: a different leader in a strictly higher term.
	var newLeader string
	var newTerm uint64
	dl := time.Now().Add(6 * time.Second)
	for time.Now().Before(dl) {
		l := fr.c.Leader()
		if l != "" && l != leader {
			newLeader = l
			newTerm = fr.c.Node(l).Status().Term
			break
		}
		time.Sleep(fr.tick())
	}
	switched := newLeader != "" && newTerm > oldTerm
	if switched {
		rc.recordLeader("post-switch", newLeader, newTerm)
	}
	rc.check("leadership-switched", switched,
		fmt.Sprintf("old=%s@%d new=%q@%d", leader, oldTerm, newLeader, newTerm))

	// Settle the membership change: collect the first attempt's result; on
	// failure or an unfinished change, retry the same add (idempotent).
	select {
	case err := <-done:
		if err != nil {
			rc.check("first-change-attempt-settled", false, err.Error())
		} else {
			rc.check("first-change-attempt-settled", true, "change committed before crash")
		}
	case <-time.After(3 * time.Second):
		rc.check("first-change-attempt-settled", false, "first attempt never returned")
	}

	if err := fr.faults.Restart(ctx, leader); err != nil {
		rc.check("crashed-leader-restarted", false, err.Error())
		return rc.rep
	}
	if fr.waitLeader(5*time.Second) == "" {
		rc.check("crashed-leader-restarted", false, "no leader after restart")
		return rc.rep
	}
	rc.check("crashed-leader-restarted", true, "cluster serves after old leader returns")

	// Retry to converge; AddNode is a safe idempotent no-op for a present voter.
	retryErr := fr.c.AddNode(ctx, newNode)
	rc.check("membership-change-converges", retryErr == nil,
		fmt.Sprintf("retry add %s: %v", newNode, retryErr))

	// Final committed config: exactly one consistent voter set on every node,
	// never joint, and never missing/duplicating the new member.
	finalVoters := expectedVoters(in.NodeIDs, newNode)
	consistent := waitVoters(fr, finalVoters, 6*time.Second)
	rc.check("committed-config-consistent", consistent,
		fmt.Sprintf("want voters %v", finalVoters))

	// Writes continue and the cluster (including the restarted old leader and
	// the new member) converges to one history.
	writesOK := true
	for i := 1; i <= in.WritesAfter; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-after-%d", in.KeyPrefix, i), "2"); err != nil {
			writesOK = false
			rc.check("post-switch-writes-commit", false, err.Error())
			break
		}
	}
	if writesOK {
		rc.check("post-switch-writes-commit", true,
			fmt.Sprintf("%d post-switch writes acknowledged", in.WritesAfter))
	}

	conv := fr.snapshotConvergence(30 * time.Second)
	rc.rep.Convergence = conv
	rc.check("cluster-converges-one-history", conv.Reached,
		fmt.Sprintf("common_commit=%d history_hash=%s", conv.CommonCommit, conv.HistoryHash))

	rc.rep.Membership = &MembershipRecord{
		NewNode: newNode, ChangeInFlight: changeInFlight,
		JointObserved: jointObserved, JointVoters: jointVoters,
		OldLeader: leader, NewLeader: newLeader, Switched: switched,
		FinalVoters: finalVoters,
	}
	return rc.rep
}

// expectedVoters is the initial set plus the new member, sorted.
func expectedVoters(initial []string, newNode string) []string {
	out := append([]string(nil), initial...)
	if !containsStr2(out, newNode) {
		out = append(out, newNode)
	}
	sortStrings(out)
	return out
}

func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1] > xs[j]; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}

// waitVoters polls until every observed node's committed config lists exactly
// want and no joint config remains active.
func waitVoters(fr *frame, want []string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		obs := fr.c.Observe()
		if len(obs.Nodes) < len(want) {
			time.Sleep(fr.tick())
			continue
		}
		ok := true
		for _, s := range obs.Nodes {
			if len(s.CommittedConfig.JointVoters) != 0 {
				ok = false
				break
			}
			got := append([]string(nil), s.CommittedConfig.Voters...)
			sortStrings(got)
			if len(got) != len(want) {
				ok = false
				break
			}
			for i := range got {
				if got[i] != want[i] {
					ok = false
				}
			}
		}
		if ok {
			return true
		}
		time.Sleep(fr.tick())
	}
	return false
}
