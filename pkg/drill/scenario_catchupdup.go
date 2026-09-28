package drill

import (
	"fmt"
	"time"
)

// runCatchupDuplicateSnapshot verifies the offline-compaction catch-up path
// against duplicate / retransmitted catch-up snapshots:
//
//  1. a follower is hard-offline for a bulk-write window and genuinely misses
//     at least one compaction boundary;
//  2. on return it installs a leader InstallSnapshot (crossing the boundary)
//     and then advances WELL beyond it through ordinary AppendEntries;
//  3. the installed (and an older) snapshot are deterministically RE-DELIVERED,
//     modeling a network retransmission/duplicate delivery after the node moved
//     forward;
//
// A correct node ignores the stale/duplicate snapshots: its applied frontier
// must not move backwards, its saved contents must not change, its reported
// applied position must keep matching the contents, and the whole cluster must
// converge to one history. Every claim is evidence-gated: if the node never
// went offline, never crossed the compaction point, or the duplicate messages
// were not actually delivered, the drill FAILs rather than passing on steps.
func runCatchupDuplicateSnapshot(fr *frame) *Report {
	rc := newRecorder(ScenarioCatchupDuplicateSnapshot, fr.input)
	in := fr.input
	ctx := fr.ctx

	leader := fr.waitLeader(5 * time.Second)
	if leader == "" {
		rc.check("cluster-elects-leader", false, "no leader elected before drill")
		return rc.rep
	}
	rc.recordLeader("baseline", leader, fr.c.Node(leader).Status().Term)
	rc.check("cluster-elects-leader", true,
		fmt.Sprintf("leader=%s term=%d", leader, fr.c.Node(leader).Status().Term))

	// Deterministic victim: the first node that is not the current leader.
	victim := ""
	for _, id := range in.NodeIDs {
		if id != leader {
			victim = id
			break
		}
	}
	victimCommit := fr.c.Node(victim).Status().CommitIndex
	if err := fr.faults.Crash(ctx, victim); err != nil {
		rc.check("fault-node-offline", false, err.Error())
		return rc.rep
	}
	time.Sleep(200 * time.Millisecond)
	offline := fr.c.IsDown(victim)
	rc.fault("crash", "long-offline", []string{victim}, offline,
		fmt.Sprintf("node=%s down=%t last_commit=%d", victim, offline, victimCommit))
	rc.check("fault-node-offline", offline,
		fmt.Sprintf("%s unreachable after crash; last_commit=%d", victim, victimCommit))
	if !offline {
		return rc.rep
	}

	// Write enough acknowledged entries to cross the snapshot threshold by a
	// wide margin while the victim is down.
	for i := 1; i <= in.WritesBefore; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-before-%d", in.KeyPrefix, i), "1"); err != nil {
			rc.check("writes-during-offline-commit", false, err.Error())
			return rc.rep
		}
	}

	// Evidence: a current leader snapshot strictly exceeds the victim's last
	// committed position (it really missed a compaction boundary).
	l := fr.waitLeader(4 * time.Second)
	var leaderSnap uint64
	if l != "" {
		leaderSnap = fr.c.Node(l).Status().SnapshotIndex
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) && leaderSnap <= victimCommit {
		time.Sleep(50 * time.Millisecond)
		l = fr.waitLeader(time.Second)
		if l != "" {
			leaderSnap = fr.c.Node(l).Status().SnapshotIndex
		}
	}
	crossed := leaderSnap > victimCommit
	rc.check("compaction-passed-offline-node", crossed,
		fmt.Sprintf("leader=%s snapshot_index=%d > victim %s last_commit=%d",
			l, leaderSnap, victim, victimCommit))
	if !crossed {
		// Bring the victim back before bailing so the harness shuts down cleanly.
		_ = fr.faults.Restart(ctx, victim)
		return rc.rep
	}

	// Bring the victim back and attach the deterministic interceptor AFTER the
	// restart (restart re-registers the real transport handler).
	if err := fr.faults.Restart(ctx, victim); err != nil {
		rc.check("fault-node-recovered", false, err.Error())
		return rc.rep
	}
	interceptor, ierr := fr.c.InterceptSnapshots(victim)
	back := fr.waitLeader(5 * time.Second)
	rc.check("fault-node-recovered", back != "" && ierr == nil,
		fmt.Sprintf("leader=%s intercept=%v", back, ierr))
	if back == "" || ierr != nil {
		return rc.rep
	}

	// Evidence: the victim installs at least one leader snapshot to cross the
	// compaction boundary.
	installed := uint64(0)
	dl := time.Now().Add(30 * time.Second)
	for time.Now().Before(dl) {
		if _, newest := interceptor.Observed(); newest > 0 {
			installed = newest
			break
		}
		time.Sleep(fr.tick())
	}
	gotSnapshot := installed >= leaderSnap
	rc.check("lagging-node-installed-snapshot", gotSnapshot,
		fmt.Sprintf("victim=%s installed_snapshot=%d leader_snapshot=%d", victim, installed, leaderSnap))
	if !gotSnapshot {
		return rc.rep
	}
	oldest, _ := interceptor.OldestIndex()

	// Drive the victim well past the installed snapshot with incremental
	// entries so the later duplicate snapshot is genuinely stale relative to
	// its applied frontier.
	postOK := true
	for i := 1; i <= in.WritesAfter; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-after-%d", in.KeyPrefix, i), "2"); err != nil {
			postOK = false
			rc.check("incremental-replication-resumes", false, err.Error())
			break
		}
	}
	conv0 := fr.snapshotConvergence(30 * time.Second)
	advanced := conv0.CommonApplied
	if postOK {
		rc.check("incremental-replication-resumes", conv0.Reached,
			fmt.Sprintf("common_applied=%d after %d post writes", advanced, in.WritesAfter))
	}
	advancedBeyond := advanced > installed
	rc.check("node-advanced-beyond-snapshot", advancedBeyond,
		fmt.Sprintf("applied=%d installed_snapshot=%d", advanced, installed))
	if !advancedBeyond {
		rc.rep.Convergence = conv0
		return rc.rep
	}

	// Capture the victim's exact pre-replay frontier and contents.
	snap0, ok0 := fr.c.NodeState(victim)
	if !ok0 {
		rc.check("duplicate-snapshot-no-rollback", false, "victim state unavailable")
		return rc.rep
	}
	beforeApplied := snap0.Status.LastApplied
	beforeKV := snap0.KVHash

	// Deterministic duplicate delivery: re-deliver BOTH the oldest boundary
	// snapshot and the newest installed snapshot after the node advanced.
	rerrOld := interceptor.ReplayOldest(ctx)
	rerrNew := interceptor.ReplayNewest(ctx)
	replayDelivered := rerrOld == nil && rerrNew == nil
	_, newestIndex := interceptor.Observed()
	rc.fault("duplicate-delivery", "post-catchup-retransmission", []string{victim},
		replayDelivered,
		fmt.Sprintf("replayed oldest=%d newest=%d after applied=%d (old_err=%v new_err=%v)",
			oldest, newestIndex, beforeApplied, rerrOld, rerrNew))
	rc.check("duplicate-snapshot-delivered", replayDelivered,
		fmt.Sprintf("oldest=%d newest=%d delivered to %s", oldest, newestIndex, victim))

	// Give the event loop a deterministic moment to process both messages, then
	// observe. The assertions below are the safety property.
	time.Sleep(150 * time.Millisecond)
	snap1, ok1 := fr.c.NodeState(victim)
	if !ok1 {
		rc.check("duplicate-snapshot-no-rollback", false, "victim state unavailable after replay")
		return rc.rep
	}
	afterApplied := snap1.Status.LastApplied
	afterKV := snap1.KVHash
	frontierRolled := afterApplied < beforeApplied
	contentsChanged := afterKV != beforeKV
	noRollback := !frontierRolled && !contentsChanged && afterApplied == beforeApplied
	rc.check("duplicate-snapshot-no-rollback", noRollback,
		fmt.Sprintf("before applied=%d kv=%s; after applied=%d kv=%s (rolled=%t contents_changed=%t)",
			beforeApplied, beforeKV, afterApplied, afterKV, frontierRolled, contentsChanged))

	// The applied frontier must continue to make progress afterwards and match
	// the real contents; cluster converges to one history.
	if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-postdup-1", in.KeyPrefix), "3"); err != nil {
		rc.check("progress-after-duplicate", false, err.Error())
	}
	conv := fr.snapshotConvergence(45 * time.Second)
	rc.rep.Convergence = conv
	rc.check("cluster-converges-one-history", conv.Reached && replayDelivered,
		fmt.Sprintf("common_commit=%d history_hash=%s", conv.CommonCommit, conv.HistoryHash))
	rc.check("progress-after-duplicate",
		conv.Reached && conv.CommonApplied > advanced,
		fmt.Sprintf("common_applied=%d (was %d)", conv.CommonApplied, advanced))

	// Whole-window data integrity: before/during/after/post-dup keys all read.
	dataOK := true
	for _, probe := range []struct{ k, v string }{
		{fmt.Sprintf("%s-before-%d", in.KeyPrefix, in.WritesBefore), "1"},
		{fmt.Sprintf("%s-after-%d", in.KeyPrefix, in.WritesAfter), "2"},
		{fmt.Sprintf("%s-postdup-1", in.KeyPrefix), "3"},
	} {
		v, ok, err := fr.c.Client().Get(ctx, probe.k)
		if err != nil || !ok || v != probe.v {
			dataOK = false
			rc.check("snapshot-data-intact", false,
				fmt.Sprintf("get(%s)=(%q,%v,%v) want %q", probe.k, v, ok, err, probe.v))
			break
		}
	}
	if dataOK {
		rc.check("snapshot-data-intact", true,
			"before/during/after/post-duplicate keys all readable at correct values")
	}

	rc.rep.DuplicateCatchup = &DuplicateCatchupRecord{
		Node:                    "first-follower",
		OfflineCommitAt:         victimCommit,
		InstalledSnapshot:       installed,
		AdvancedTo:              beforeApplied,
		ReplayedOldest:          oldest,
		ReplayedNewest:          newestIndex,
		AppliedAfterReplay:      afterApplied,
		ContentsChangedAfter:    contentsChanged,
		FrontierRolledBackAfter: frontierRolled,
		ReplayTookEffect:        replayDelivered,
	}
	return rc.rep
}
