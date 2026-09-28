package drill

import (
	"fmt"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/statemachine"
)

// runLeaderCrashRetry verifies exactly-once application across a leader crash
// at the moment a write's outcome is unknown:
//
//  1. one proposal with a fixed (client,seq) is issued directly to the leader;
//  2. the leader is hard-crashed before (or without) an ack;
//  3. the identical (client,seq) is retried through the post-failover leader;
//  4. the converged history must show the mutation applied exactly once
//     (the first attempt may have committed; the retry then returns the dedup
//     marker; or the first attempt was lost and the retry applies it once).
func runLeaderCrashRetry(fr *frame) *Report {
	rc := newRecorder(ScenarioLeaderCrashRetry, fr.input)
	in := fr.input
	ctx := fr.ctx
	const clientID = "drill-lc"
	const seq uint64 = 1001
	key := in.KeyPrefix + "-ambiguous"

	old := fr.waitLeader(5 * time.Second)
	if old == "" {
		rc.check("cluster-elects-leader", false, "no leader elected before drill")
		return rc.rep
	}
	oldTerm := fr.c.Node(old).Status().Term
	rc.recordLeader("baseline", old, oldTerm)
	rc.check("cluster-elects-leader", true, fmt.Sprintf("leader=%s term=%d", old, oldTerm))

	for i := 1; i <= in.WritesBefore; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-before-%d", in.KeyPrefix, i), "1"); err != nil {
			rc.check("baseline-writes-commit", false, err.Error())
			return rc.rep
		}
	}
	rc.check("baseline-writes-commit", true, "established committed baseline")

	// Fire ONE bounded, non-retried proposal at the leader, then crash the
	// leader while its outcome is still unknown to the caller.
	outcomeCh := make(chan struct {
		accepted bool
		dup      bool
		err      string
	}, 1)
	go func() {
		o := fr.c.ExplicitPropose(ctx, clientID, seq, statemachine.Command{
			Op: statemachine.CmdPut, Key: key, Value: "once",
		})
		err := ""
		if o.Err != nil {
			err = reasonOf(o.Err)
		}
		outcomeCh <- struct {
			accepted bool
			dup      bool
			err      string
		}{o.Accepted, o.Dup, err}
	}()
	delayMS := in.CrashDelayMS
	if delayMS <= 0 {
		delayMS = 20
	}
	time.Sleep(time.Duration(delayMS) * time.Millisecond)
	crashErr := fr.faults.Crash(ctx, old)
	time.Sleep(200 * time.Millisecond)
	first := <-outcomeCh

	// Fault evidence: the old leader is genuinely down/unreachable.
	tookEffect := crashErr == nil && fr.c.IsDown(old)
	ev := fmt.Sprintf("crashed_leader=%s reachable=%t", old, !tookEffect)
	rc.fault("crash", "leader-during-ambiguous-write", []string{old}, tookEffect, ev)
	rc.check("fault-leader-crashed", tookEffect, ev)
	if !tookEffect && crashErr != nil {
		rc.check("baseline-writes-commit", false, crashErr.Error())
		return rc.rep
	}

	// Failover evidence: a (possibly identical-node after restart, but
	// strictly higher term) leadership establishes without the crashed node.
	newLeader := ""
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		l := fr.c.Leader()
		if l != "" && l != old {
			newLeader = l
			break
		}
		time.Sleep(fr.tick())
	}
	failedOver := newLeader != ""
	rc.check("new-leader-after-crash", failedOver,
		fmt.Sprintf("old=%s new=%q", old, newLeader))

	// Retry the SAME (client,seq) through the new leader. It must either apply
	// exactly once (first attempt lost) or be served from the dedup table
	// (first attempt committed): never two effects.
	var retryAccepted, retryDup bool
	var retryReason string
	if failedOver {
		o := fr.c.ExplicitPropose(ctx, clientID, seq, statemachine.Command{
			Op: statemachine.CmdPut, Key: key, Value: "once",
		})
		retryAccepted, retryReason = o.Accepted, reasonOf(o.Err)
		retryDup = o.Dup
	}
	rc.check("ambiguous-write-retry-settled", failedOver && retryAccepted,
		fmt.Sprintf("first_attempt_accepted=%t first_err=%s retry_accepted=%t retry_dup=%t reason=%s",
			first.accepted, first.err, retryAccepted, retryDup, retryReason))

	// Restart the crashed leader so convergence is whole-cluster.
	if err := fr.faults.Restart(ctx, old); err != nil {
		rc.check("crashed-leader-restarted", false, err.Error())
		return rc.rep
	}
	time.Sleep(200 * time.Millisecond)
	backReachable := !fr.c.IsDown(old)
	rc.fault("restart", "crashed-leader-return", []string{old}, backReachable,
		fmt.Sprintf("node=%s reachable=%t after restart", old, backReachable))
	l := fr.waitLeader(5 * time.Second)
	rc.check("crashed-leader-restarted", backReachable && l != "",
		fmt.Sprintf("leader=%s old_leader_reachable=%t", l, backReachable))
	if l != "" {
		rc.recordLeader("after-restart", l, fr.c.Node(l).Status().Term)
	}

	// Post-drill traffic must remain healthy and observe no rollback.
	for i := 1; i <= in.WritesAfter; i++ {
		if err := fr.c.Client().Put(ctx, fmt.Sprintf("%s-after-%d", in.KeyPrefix, i), "2"); err != nil {
			rc.check("post-crash-writes-commit", false, err.Error())
			break
		}
	}

	conv := fr.snapshotConvergence(30 * time.Second)
	rc.rep.Convergence = conv
	rc.check("cluster-converges-one-history", conv.Reached,
		fmt.Sprintf("common_commit=%d history_hash=%s", conv.CommonCommit, conv.HistoryHash))

	// Exactly-once evidence from the authoritative source: the applied client
	// frontier in every node's state machine. The retry must not have advanced
	// it past the one logical request, and it must not have rolled it back.
	frontierOK := true
	frontierDetail := ""
	for _, id := range in.NodeIDs {
		e := fr.c.ClientSeq(id, clientID)
		if !e.Known || e.Seq != seq {
			frontierOK = false
			frontierDetail = fmt.Sprintf("node %s applied seq=%d known=%t, want %d", id, e.Seq, e.Known, seq)
			break
		}
	}
	if frontierOK {
		frontierDetail = fmt.Sprintf("every node applied (%s,%d) as its frontier", clientID, seq)
	}
	rc.check("ambiguous-write-effect-once", frontierOK, frontierDetail)

	dupObs := 0
	if retryDup {
		dupObs = 1
	}
	effect := 0
	if frontierOK {
		effect = 1
	}
	rc.rep.Writes = append(rc.rep.Writes, WriteRecord{
		Label: "ambiguous", ClientID: clientID, Seq: seq,
		Accepted: retryAccepted, Applied: effect == 1,
		DupObservations: dupObs, EffectCount: effect,
	})

	// The committed value must be exactly the one intended, visible linearly.
	v, ok, gerr := fr.c.Client().Get(ctx, key)
	rc.check("ambiguous-write-final-value", gerr == nil && ok && v == "once",
		fmt.Sprintf("get(%s)=(%q,%v,%v)", key, v, ok, gerr))
	return rc.rep
}
