package drill

import (
	"context"
	"time"
)

// runLeaderCrashDedup:
//
//  1. commit a stable prefix;
//  2. partition the leader from its followers (so a proposal's outcome is
//     genuinely unknown: it cannot gain a quorum),
//  3. issue one fixed payload against the partitioned leader (it never acks),
//  4. crash that leader (the entry, if appended locally, is uncommitted),
//  5. let the majority elect a new leader;
//  6. retry the EXACT same (client,seq) payload through the new leader;
//  7. send the same payload a third time; it must be answered Dup=true;
//  8. restart the old leader; everyone converges with exactly one effect.
//
// Sabotage mode skips the real crash: the claimed failure never happens and
// the drill must FAIL.
func runLeaderCrashDedup(ctx context.Context, h *harness, started time.Time) (*Report, error) {
	old, err := h.waitLeader(5 * time.Second)
	if err != nil {
		return nil, err
	}
	stable, err := h.put("drill-dedup", 1, "dedup-stable", "s")
	if err != nil {
		return nil, err
	}
	if _, err := h.proposeOnce(ctx, old, stable); err != nil {
		return nil, err
	}

	var checks []CheckResult

	// Make the write outcome unknowable: leader can talk to nobody.
	h.partition(old)
	h.poll(500*time.Millisecond, func() bool {
		return len(h.reachableVoters(old)) < quorumSize(len(h.status(old).CommittedConfig.Voters))
	})
	partitioned := len(h.reachableVoters(old)) < quorumSize(len(h.status(old).CommittedConfig.Voters))
	if partitioned {
		checks = append(checks, checkPass(
			"ambiguity-window",
			"leader isolated from a quorum before the in-flight write, making its result unknowable",
		))
	} else {
		checks = append(checks, checkFail(
			"ambiguity-window",
			"leader still reached a quorum; the unknown-outcome condition was not created",
		))
	}

	payload, err := h.put("drill-dedup", 2, "dedup-race", "x")
	if err != nil {
		return nil, err
	}
	_, ambErr := h.proposeOnce(ctx, old, payload)
	ambiguous := ambErr != nil
	h.recordWrite("dedup-race", "leader-partitioned-first-attempt", outcomeLabel(ambiguous), ambiguous, false)
	if ambiguous {
		checks = append(checks, checkPass(
			"outcome-unknown",
			"first attempt returned an error before any quorum ack; outcome unknown",
		))
	} else {
		checks = append(checks, checkFail(
			"outcome-unknown",
			"first attempt was acknowledged; the ambiguous-outcome precondition was not created",
		))
	}

	// Crash the leader for real (sabotage: leave it running and just heal).
	if !h.sabotage {
		if err := h.crashNode(ctx, old); err != nil {
			return nil, err
		}
	}
	offline := !h.c.Transport().Registered(old)
	if offline {
		checks = append(checks, checkPass(
			"leader-really-offline",
			"old leader unregistered from the transport after crash",
		))
	} else {
		checks = append(checks, checkFail(
			"leader-really-offline",
			"claimed leader crash did not take the node offline",
		))
	}

	// New leader in the surviving majority.
	h.c.HealAll()
	var newLeader string
	if h.poll(5*time.Second, func() bool {
		ld := h.c.Leader()
		return ld != "" && ld != old
	}) {
		newLeader = h.c.Leader()
	}
	if newLeader != "" {
		checks = append(checks, checkPass(
			"new-leader-after-crash",
			"surviving majority elected a different leader at a higher term",
		))
	} else {
		checks = append(checks, checkFail(
			"new-leader-after-crash",
			"no distinct new leader emerged after the claimed crash",
		))
	}

	// Retry the SAME payload through the new leader.
	if newLeader != "" {
		if _, rerr := h.proposeOnce(ctx, newLeader, payload); rerr != nil {
			checks = append(checks, checkFail("retry-commit", rerr.Error()))
		} else {
			checks = append(checks, checkPass(
				"retry-commit",
				"retry of the identical payload committed through the new leader",
			))
		}
	}

	// Third delivery of the exact same payload: must be a duplicate, no second
	// state mutation.
	dup := false
	if newLeader != "" {
		res2, derr := h.proposeOnce(ctx, newLeader, payload)
		dup = derr == nil && res2.Dup
	}
	if dup {
		checks = append(checks, checkPass(
			"retry-no-double-effect",
			"third identical request answered Dup=true; exactly one effect",
		))
		h.recordWrite("dedup-race", "retried", "accepted-once", true, true)
	} else {
		checks = append(checks, checkFail(
			"retry-no-double-effect",
			"duplicate delivery not recognized as Dup; double effect not ruled out",
		))
		h.recordWrite("dedup-race", "retried", "double-or-unknown", false, false)
	}

	// Restart old leader; it must align without resurrecting an uncommitted
	// divergent suffix.
	if offline {
		if rerr := h.restartNode(ctx, old); rerr != nil {
			checks = append(checks, checkFail("old-leader-restart", rerr.Error()))
		} else {
			checks = append(checks, checkPass(
				"old-leader-restart",
				"crashed leader reloaded persisted state from disk and rejoined",
			))
		}
	}
	return h.finish(checks, 6*time.Second, started)
}

func outcomeLabel(failed bool) string {
	if failed {
		return "error-outcome-unknown"
	}
	return "accepted"
}
