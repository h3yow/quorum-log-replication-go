package drill

import (
	"context"
	"errors"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raft"
)

// runMajorityUnreachable:
//
//  1. elect a leader and commit a fixed prefix;
//  2. isolate the leader from ALL peers -> it can reach only itself, i.e.
//     strictly less than a quorum of voters;
//  3. a new write attempted through that leader must NOT be confirmed, and
//     its commit index must stay frozen;
//  4. heal; a normal fixed write must then commit and all nodes converge.
//
// In sabotage mode the "isolation" never installs: the claimed fault is
// absent, the write commits, and the drill must return FAIL.
func runMajorityUnreachable(ctx context.Context, h *harness, started time.Time) (*Report, error) {
	leader, err := h.waitLeader(5 * time.Second)
	if err != nil {
		return nil, err
	}

	// Fixed input: one stable prefix write.
	pre, err := h.put("drill-maj", 1, "maj-pre", "1")
	if err != nil {
		return nil, err
	}
	if _, err := h.proposeOnce(ctx, leader, pre); err != nil {
		return nil, err
	}
	// Wait until the prefix is committed cluster-wide (the leader's term
	// no-op may sit one index ahead of the data write).
	maxCommit := h.maxCommit()
	if !h.settleCommit(maxCommit, 5*time.Second) {
		checks := []CheckResult{checkFail("prefix-settled", "prefix write did not settle before isolation")}
		return h.finish(checks, 5*time.Second, started)
	}
	preStatus := h.status(leader)

	var checks []CheckResult
	totalVoters := len(preStatus.CommittedConfig.Voters)

	if !h.sabotage {
		h.partition(leader)
		// Give the network rule time to take effect and the majority side
		// time to notice its leader is gone.
		h.poll(800*time.Millisecond, func() bool {
			return len(h.reachableVoters(leader)) < quorumSize(totalVoters)
		})
	}

	reachableDuring := len(h.reachableVoters(leader))
	quorum := quorumSize(totalVoters)
	faultReal := reachableDuring < quorum
	if faultReal {
		checks = append(checks, checkPass(
			"majority-unreachable",
			"isolated leader reaches fewer than a quorum of committed voters",
		))
	} else {
		checks = append(checks, checkFail(
			"majority-unreachable",
			"claimed majority isolation never took effect: quorum still reachable",
		))
	}

	// Freeze the commit baseline only AFTER in-flight acks that predate the
	// partition have drained (commit stable for a quiet window). This is the
	// honest "what can still commit given the fault" baseline.
	frozen := preStatus.CommitIndex
	if faultReal {
		if b, ok := h.stableCommit(leader, 2*time.Second, 300*time.Millisecond); ok {
			frozen = b
		}
	}

	// Attempt a write through the isolated leader handle with no retry. It
	// must not be acknowledged while fewer than a quorum are reachable.
	during, err := h.put("drill-maj", 2, "maj-during", "2")
	if err != nil {
		return nil, err
	}
	_, perr := h.proposeOnce(ctx, leader, during)
	rejected := errors.Is(perr, context.DeadlineExceeded) ||
		errors.Is(perr, raft.ErrNoQuorum) || perr != nil
	stillFrozen := h.status(leader).CommitIndex == frozen

	if faultReal && rejected && stillFrozen {
		checks = append(checks, checkPass(
			"write-rejected-without-quorum",
			"new write was not confirmed and leader commit index stayed frozen",
		))
		h.recordWrite("maj-during", "partitioned", "rejected", true, false)
	} else {
		checks = append(checks, checkFail(
			"write-rejected-without-quorum",
			"a new write was confirmed despite lacking a quorum or commit advanced",
		))
		h.recordWrite("maj-during", "partitioned", "accepted", false, false)
	}

	// Heal and prove the cluster resumes confirming writes.
	h.c.HealAll()
	if _, err := h.requireLeader(5 * time.Second); err != nil {
		checks = append(checks, checkFail("post-heal-leader", "no leader after partition healed"))
		return h.finish(checks, 5*time.Second, started)
	}
	post, err := h.put("drill-maj", 3, "maj-post", "3")
	if err != nil {
		return nil, err
	}
	postLeader := h.c.Leader()
	if _, err := h.proposeOnce(ctx, postLeader, post); err != nil {
		checks = append(checks, checkFail("post-heal-write", err.Error()))
	} else {
		checks = append(checks, checkPass(
			"post-heal-write",
			"fixed write commits after the partition heals",
		))
		h.recordWrite("maj-post", "healed", "accepted", true, false)
	}
	return h.finish(checks, 6*time.Second, started)
}

// quorumSize is majority size n/2+1, mirrored from the raft rule.
func quorumSize(n int) int { return n/2 + 1 }
