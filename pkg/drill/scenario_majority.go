package drill

import (
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raft"
	"github.com/h3yow/quorum-log-replication-go/pkg/statemachine"
)

// runMajorityDown verifies: while a strict majority of voters is hard-down,
// the surviving minority cannot acknowledge new writes (no quorum), the commit
// position cannot advance, and after the majority returns every node
// converges to one history containing the post-recovery writes exactly once.
func runMajorityDown(fr *frame) *Report {
	rc := newRecorder(ScenarioMajorityDown, fr.input)
	in := fr.input
	ctx := fr.ctx

	// Phase 0: elect and establish committed baseline.
	leader := fr.waitLeader(5 * time.Second)
	if leader == "" {
		rc.check("cluster-elects-leader", false, "no leader elected before drill")
		return rc.rep
	}
	term := fr.c.Node(leader).Status().Term
	rc.recordLeader("baseline", leader, term)
	rc.check("cluster-elects-leader", true, fmt.Sprintf("leader=%s term=%d", leader, term))

	for i := 1; i <= in.WritesBefore; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-before-%d", in.KeyPrefix, i), "1"); err != nil {
			rc.check("baseline-writes-commit", false, err.Error())
			return rc.rep
		}
	}
	baseline := fr.c.Node(leader).Status().CommitIndex
	rc.check("baseline-writes-commit", true, fmt.Sprintf("commit_index=%d after %d writes", baseline, in.WritesBefore))

	// Phase 1: hard-crash a majority of the voters.
	quorum := majorityOf(in.NodeCount)
	victims := pickMajority(in.NodeIDs, leader)
	for _, id := range victims {
		if err := fr.faults.Crash(ctx, id); err != nil {
			rc.check("fault-majority-injected", false, "crash "+id+": "+err.Error())
			return rc.rep
		}
	}
	time.Sleep(300 * time.Millisecond)

	// Evidence 1: every crashed node is actually unreachable.
	allDown := true
	for _, id := range victims {
		if !fr.c.IsDown(id) {
			allDown = false
		}
	}
	downEvidence := fmt.Sprintf("crashed=%v reachable_from_quorum=%t need_quorum=%d", victims, !allDown, quorum)
	rc.fault("crash", "majority-unreachable", victims, allDown, downEvidence)
	rc.check("fault-majority-injected", allDown, downEvidence)

	// Evidence 2: even though the stranded old leader may still cache its
	// leader role, it cannot reach a quorum of voters, so it is incapable of
	// committing anything.
	time.Sleep(600 * time.Millisecond)
	quorumAvailable := false
	reachable := 0
	if l := fr.c.Leader(); l != "" {
		voters := fr.c.Node(l).Status().CommittedConfig.Voters
		reachable = len(fr.c.ReachableVoters(l, voters))
		quorumAvailable = reachable >= majorityOf(len(voters))
	}
	rc.check("leader-cannot-reach-quorum", !quorumAvailable,
		fmt.Sprintf("stranded_leader=%q reachable_voters=%d quorum=%d",
			fr.c.Leader(), reachable, quorum))

	// Evidence 3 + safety: every write attempted while the majority is down is
	// rejected, and the commit index never advances on any survivor.
	before := maxCommit(fr)
	rejectedAll := true
	var lastReason string
	for i := 1; i <= in.WritesDuring; i++ {
		out := fr.c.ExplicitPropose(ctx, "drill-md", uint64(i), statemachine.Command{
			Op:  statemachine.CmdPut,
			Key: fmt.Sprintf("%s-during-%d", in.KeyPrefix, i), Value: "blocked",
		})
		if out.Accepted {
			rejectedAll = false
			lastReason = "attempt " + fmt.Sprint(i) + " wrongly accepted"
			break
		}
		lastReason = reasonOf(out.Err)
		rc.rep.Writes = append(rc.rep.Writes, WriteRecord{
			Label: fmt.Sprintf("during-%d", i), ClientID: "drill-md", Seq: uint64(i),
			Accepted: false, RejectedReason: lastReason, Applied: false,
		})
	}
	after := maxCommit(fr)
	commitFrozen := after == before
	rc.check("writes-rejected-without-majority", rejectedAll,
		fmt.Sprintf("attempted=%d last_reason=%s", in.WritesDuring, lastReason))
	rc.check("commit-frozen-without-majority", commitFrozen,
		fmt.Sprintf("max survivor commit before=%d after=%d", before, after))

	// Phase 2: recover under a proven leadership change. The surviving
	// minority (with the stranded old leader and any uncommitted tentative
	// entries) is isolated first; the returned majority elects a leader in a
	// strictly higher term. Only then is the minority healed, forcing the old
	// leader to step down and truncate its uncommitted tail: a write rejected
	// during the outage can never be confirmed afterwards.
	survivors := removeSet(in.NodeIDs, victims)
	for _, id := range survivors {
		fr.faults.Partition(id, victims...)
	}
	for _, id := range victims {
		if err := fr.faults.Restart(ctx, id); err != nil {
			rc.check("fault-majority-recovered", false, "restart "+id+": "+err.Error())
			return rc.rep
		}
	}
	newLeader := waitLeaderAmong(fr, victims, 5*time.Second)
	recovered := newLeader != ""
	rc.check("fault-majority-recovered", recovered,
		fmt.Sprintf("majority-side leader=%s after restoring %v", newLeader, victims))

	higherTerm := false
	if recovered {
		nt := fr.c.Node(newLeader).Status().Term
		higherTerm = nt > term
		rc.recordLeader("majority-recovered", newLeader, nt)
		rc.check("new-term-after-recovery", higherTerm,
			fmt.Sprintf("old_term=%d new_leader=%s new_term=%d", term, newLeader, nt))
	}
	fr.faults.HealAll()
	restored := ""
	if recovered && higherTerm {
		restored = fr.waitLeader(5 * time.Second)
	}
	rc.check("cluster-reforms", restored != "",
		fmt.Sprintf("leader_after_heal=%q", restored))
	if restored != "" {
		rc.recordLeader("reformed", restored, fr.c.Node(restored).Status().Term)
	}

	for i := 1; i <= in.WritesAfter; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-after-%d", in.KeyPrefix, i), "2"); err != nil {
			rc.check("post-recovery-writes-commit", false, err.Error())
			break
		}
	}
	conv := fr.snapshotConvergence(30 * time.Second)
	rc.rep.Convergence = conv
	rc.check("post-recovery-writes-commit",
		conv.Reached && conv.CommonCommit > after,
		fmt.Sprintf("common_commit=%d (was %d)", conv.CommonCommit, after))
	rc.check("cluster-converges-one-history", conv.Reached,
		fmt.Sprintf("nodes=%d common_commit=%d history_hash=%s", len(conv.Nodes), conv.CommonCommit, conv.HistoryHash))

	// The blocked writes must be absent from the converged history: the old
	// leader's tentative tail was truncated by the higher-term recovery.
	noneLeaked := true
	leakDetail := "all blocked client/seq pairs absent from converged history"
	for i := 1; i <= in.WritesDuring; i++ {
		n, err := countCommittedEffect(fr, "drill-md", uint64(i))
		if err != nil {
			noneLeaked, leakDetail = false, err.Error()
			break
		}
		if n != 0 {
			noneLeaked = false
			leakDetail = fmt.Sprintf("seq %d present on %d retained logs", i, n)
			break
		}
	}
	rc.check("rejected-writes-never-applied", noneLeaked, leakDetail)
	return rc.rep
}

// waitLeaderAmong polls until one of the given ids reports leader state.
func waitLeaderAmong(fr *frame, ids []string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			n := fr.c.Node(id)
			if n != nil && n.Status().State.String() == "leader" && fr.c.Leader() == id {
				return id
			}
		}
		time.Sleep(fr.tick())
	}
	return ""
}

// pickMajority returns quorum nodes excluding the leader first where possible,
// so the surviving side is a genuine minority that cannot elect.
func pickMajority(ids []string, leader string) []string {
	quorum := majorityOf(len(ids))
	out := make([]string, 0, quorum)
	for _, id := range ids {
		if id != leader && len(out) < quorum {
			out = append(out, id)
		}
	}
	for _, id := range ids {
		if len(out) == quorum {
			break
		}
		if !containsStr2(out, id) {
			out = append(out, id)
		}
	}
	return out
}

func removeSet(all, remove []string) []string {
	out := []string{}
	for _, x := range all {
		if !containsStr2(remove, x) {
			out = append(out, x)
		}
	}
	return out
}

func containsStr2(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func maxCommit(fr *frame) uint64 {
	var m uint64
	for _, s := range fr.c.Observe().Nodes {
		if s.CommitIndex > m {
			m = s.CommitIndex
		}
	}
	return m
}

func reasonOf(err error) string {
	switch {
	case errors.Is(err, raft.ErrNoQuorum):
		return "no-quorum"
	case errors.Is(err, raft.ErrNotLeader):
		return "not-leader"
	case err == nil:
		return ""
	default:
		return err.Error()
	}
}
