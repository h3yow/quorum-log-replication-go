package drill

import (
	"fmt"
	"time"
)

// runSnapshotCatchup verifies: a node that stays offline long enough to miss a
// compaction boundary catches up by installing a leader snapshot (not by
// receiving deleted WAL entries), then resumes incremental replication, and
// the whole cluster converges to one history. All claims require evidence:
// the node really went offline, the cluster really compacted past its last
// log position, and it really installed a snapshot on return.
func runSnapshotCatchup(fr *frame) *Report {
	rc := newRecorder(ScenarioSnapshotCatchup, fr.input)
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

	// Take the victim offline before the workload writes so it misses the
	// whole window and must cross a compaction boundary to catch up. Its exact
	// offline index can differ by one depending on election-heartbeat timing;
	// that concrete number is recorded for inspection but the stable safety
	// fact hashed across runs is that the later leader snapshot strictly
	// exceeded whatever position it stopped at.
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

	for i := 1; i <= in.WritesBefore; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-before-%d", in.KeyPrefix, i), "1"); err != nil {
			rc.check("baseline-writes-commit", false, err.Error())
			return rc.rep
		}
	}

	// Drive enough acknowledged writes to cross the snapshot threshold (32)
	// by a wide margin on every surviving node.
	for i := 1; i <= in.WritesDuring; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-bulk-%d", in.KeyPrefix, i), fmt.Sprintf("%d", i)); err != nil {
			rc.check("writes-during-offline-commit", false, err.Error())
			return rc.rep
		}
	}

	// Evidence: a current leader has compacted (snapshot index) strictly beyond
	// the victim's last committed position. Leadership may move while the
	// victim is down, so inspect dynamically.
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

	// Bring the victim back.
	if err := fr.faults.Restart(ctx, victim); err != nil {
		rc.check("fault-node-recovered", false, err.Error())
		return rc.rep
	}
	back := fr.waitLeader(5 * time.Second)
	rc.check("fault-node-recovered", back != "", fmt.Sprintf("leader=%s", back))

	// Continue writing to prove incremental replication resumes after the
	// snapshot install.
	postOK := true
	for i := 1; i <= in.WritesAfter; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-after-%d", in.KeyPrefix, i), "2"); err != nil {
			postOK = false
			rc.check("incremental-replication-resumes", false, err.Error())
			break
		}
	}

	// Evidence: the victim installed a snapshot at or beyond the boundary and
	// then converged; wait for its snapshot index to become positive and its
	// applied position to reach the cluster common position.
	installed := uint64(0)
	dl := time.Now().Add(30 * time.Second)
	for time.Now().Before(dl) {
		s := fr.c.Node(victim).Status()
		if s.SnapshotIndex > 0 {
			installed = s.SnapshotIndex
			break
		}
		time.Sleep(fr.tick())
	}
	gotSnapshot := installed > 0 && installed >= leaderSnap
	if postOK {
		rc.check("incremental-replication-resumes", true,
			fmt.Sprintf("%d post-return writes acknowledged", in.WritesAfter))
	}
	rc.check("lagging-node-installed-snapshot", gotSnapshot,
		fmt.Sprintf("victim=%s installed_snapshot=%d leader_snapshot=%d", victim, installed, leaderSnap))

	conv := fr.snapshotConvergence(45 * time.Second)
	rc.rep.Convergence = conv
	rc.check("cluster-converges-one-history", conv.Reached && gotSnapshot,
		fmt.Sprintf("common_commit=%d history_hash=%s", conv.CommonCommit, conv.HistoryHash))

	// Spot-check data from before, during and after the offline window.
	dataOK := true
	for _, probe := range []struct{ k, v string }{
		{fmt.Sprintf("%s-before-%d", in.KeyPrefix, in.WritesBefore), "1"},
		{fmt.Sprintf("%s-bulk-%d", in.KeyPrefix, in.WritesDuring), fmt.Sprintf("%d", in.WritesDuring)},
		{fmt.Sprintf("%s-after-%d", in.KeyPrefix, in.WritesAfter), "2"},
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
		rc.check("snapshot-data-intact", true, "before/during/after-window keys all readable at correct values")
	}

	rc.rep.Compaction = &CompactionRecord{
		Node: victim, OfflineCommitAt: victimCommit,
		LeaderSnapshotAt: leaderSnap, CrossedCompaction: crossed,
		InstalledSnapshot: installed,
	}
	return rc.rep
}
