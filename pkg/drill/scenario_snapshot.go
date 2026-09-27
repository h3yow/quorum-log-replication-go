package drill

import (
	"context"
	"fmt"
	"time"
)

// runSnapshotCatchup:
//
//  1. crash one follower while the cluster is near the start of its log;
//  2. drive enough fixed writes on the majority to cross the 32-entry
//     compaction threshold and snapshot;
//  3. PROVE the boundary: the leader's snapshot index is strictly past the
//     offline node's last applied position (a normal AppendEntries cannot
//     bridge it);
//  4. restart the node; it must install the snapshot, then incrementally
//     catch up new post-restart writes;
//  5. all nodes converge with identical history.
//
// Sabotage mode never takes the node offline, so no node is behind the
// compaction point and the drill must FAIL.
func runSnapshotCatchup(ctx context.Context, h *harness, started time.Time) (*Report, error) {
	leader, err := h.waitLeader(5 * time.Second)
	if err != nil {
		return nil, err
	}

	// Deterministic victim: the follower with the smallest id (never the
	// leader at selection time).
	victim := ""
	for _, id := range h.ids {
		if id != leader {
			victim = id
			break
		}
	}
	victimApplied := h.status(victim).LastApplied

	var checks []CheckResult
	threshold := uint64(32)

	if !h.sabotage {
		if err := h.crashNode(ctx, victim); err != nil {
			return nil, err
		}
	}
	offline := !h.c.Transport().Registered(victim)
	if offline {
		checks = append(checks, checkPass(
			"node-really-offline",
			"follower unregistered from the transport before the compaction window",
		))
	} else {
		checks = append(checks, checkFail(
			"node-really-offline",
			"claimed offline node stayed registered; it kept receiving the log",
		))
	}

	// Fixed bulk input: enough entries to cross the snapshot threshold with
	// margin on every run.
	for i := 1; i <= 70; i++ {
		payload, perr := h.put(fmt.Sprintf("drill-snap-%d", i), 1, fmt.Sprintf("snap-k-%d", i), fmt.Sprintf("%d", i))
		if perr != nil {
			return nil, perr
		}
		ld := h.c.Leader()
		if _, perr = h.proposeOnce(ctx, ld, payload); perr != nil {
			return nil, perr
		}
	}

	// Wait until a serving leader actually holds a snapshot beyond the
	// victim's pre-crash position.
	var leaderSnap uint64
	h.poll(5*time.Second, func() bool {
		h.sampleEra()
		ld := h.c.Leader()
		if ld == "" {
			return false
		}
		s := h.status(ld)
		if s.SnapshotIndex > threshold && s.SnapshotIndex > victimApplied {
			leaderSnap = s.SnapshotIndex
			return true
		}
		return false
	})

	crossed := offline && leaderSnap > victimApplied && leaderSnap > threshold
	var sf *SnapshotFact
	if crossed {
		checks = append(checks, checkPass(
			"offline-past-compaction",
			fmt.Sprintf("leader snapshot at %d while offline node stopped at %d; boundary crossed",
				leaderSnap, victimApplied),
		))
	} else {
		checks = append(checks, checkFail(
			"offline-past-compaction",
			fmt.Sprintf("no compaction boundary gap: leaderSnap=%d offlineApplied=%d",
				leaderSnap, victimApplied),
		))
	}
	h.setVictim(victim)
	sf = &SnapshotFact{
		OfflineRole:      roleVictim,
		ThresholdCrossed: crossed,
		LeaderSnapIndex:  leaderSnap,
		OfflineLastIndex: victimApplied,
	}

	// Bring the node back.
	if offline {
		if rerr := h.restartNode(ctx, victim); rerr != nil {
			checks = append(checks, checkFail("node-restart", rerr.Error()))
		} else {
			checks = append(checks, checkPass(
				"node-restart",
				"offline node reloaded its persisted state and rejoined",
			))
		}
	}

	// Post-restart fixed writes prove incremental replication resumes after
	// the snapshot install.
	for i := 71; i <= 90; i++ {
		payload, perr := h.put(fmt.Sprintf("drill-snap-%d", i), 1, fmt.Sprintf("snap-k-%d", i), fmt.Sprintf("%d", i))
		if perr != nil {
			return nil, perr
		}
		ld := h.c.Leader()
		if _, perr = h.proposeOnce(ctx, ld, payload); perr != nil {
			return nil, perr
		}
	}

	// The recovered node must now hold a snapshot covering its pre-crash
	// position: only possible via InstallSnapshot, never via incremental AE.
	recoveredSnap := h.status(victim).SnapshotIndex
	installed := crossed && recoveredSnap >= leaderSnap
	sf.RecoveredSnapIdx = recoveredSnap
	sf.SnapshotInstalled = installed
	if installed {
		checks = append(checks, checkPass(
			"snapshot-install-then-catchup",
			fmt.Sprintf("recovered node installed snapshot to %d then received incremental tail",
				recoveredSnap),
		))
	} else {
		checks = append(checks, checkFail(
			"snapshot-install-then-catchup",
			fmt.Sprintf("recovered node snapshot=%d did not cross leader boundary %d",
				recoveredSnap, leaderSnap),
		))
	}

	rep, err := h.finish(checks, 10*time.Second, started)
	if err != nil {
		return nil, err
	}
	rep.Facts.SnapshotEvidence = sf
	// Rebuild digest after attaching snapshot evidence.
	if err := h.finalizeEvidence(rep); err != nil {
		return nil, err
	}
	return rep, nil
}
