// Package drill implements repeatable, self-verifying failure drills with
// machine-readable audit reports. Every drill drives only the in-process
// simulated cluster: no sockets, clocks-of-wall dependence for verdicts, or
// human log watching. A drill passes only when the failure condition it
// claims actually took effect (proved by observed evidence) and the safety
// properties held; otherwise the report verdict is FAIL with explicit
// reasons.
package drill

// Verdict is the machine-readable final conclusion of one drill.
type Verdict string

const (
	// VerdictPass means every claimed failure was evidenced and every safety
	// expectation held.
	VerdictPass Verdict = "PASS"
	// VerdictFail means the drill ran but at least one check failed: either a
	// claimed fault never took effect or a safety property was violated.
	VerdictFail Verdict = "FAIL"
)

// CheckResult is one named judgment contributing to the verdict.
type CheckResult struct {
	// Name is a stable, ordered identifier (e.g. "majority-unreachable").
	Name string `json:"name"`
	// Pass is the actual judgment.
	Pass bool `json:"pass"`
	// Detail is a fixed-vocabulary, input-derived explanation; it must not
	// contain timings or timestamps.
	Detail string `json:"detail"`
}

// NodeFact is one node's role-independent final state for the report.
type NodeFact struct {
	Role          string   `json:"role"`
	State         string   `json:"state"`
	TermOffset    uint64   `json:"term_offset"`
	CommitIndex   uint64   `json:"commit_index"`
	LastApplied   uint64   `json:"last_applied"`
	SnapshotIndex uint64   `json:"snapshot_index"`
	Voters        []string `json:"voters"`
	JointVoters   []string `json:"joint_voters,omitempty"`
	Learners      []string `json:"learners,omitempty"`
	Offline       bool     `json:"offline"`
}

// Fact is the deterministic, repeatable core of a drill report. Wall-clock
// fields deliberately live outside it so that re-running the same scenario
// with the same fixed input yields an identical fact set and digest.
type Fact struct {
	// Scenario is the stable scenario identifier.
	Scenario string `json:"scenario"`
	// Nodes is the number of nodes the fixed input created.
	Nodes int `json:"nodes"`
	// IsolatedRoles are the role labels isolated while the claim was tested.
	IsolatedRoles []string `json:"isolated_roles,omitempty"`
	// CrashedRoles are nodes that really left the transport (unregistered).
	CrashedRoles []string `json:"crashed_roles,omitempty"`
	// RestartedRoles are crashed nodes that rejoined afterwards.
	RestartedRoles []string `json:"restarted_roles,omitempty"`
	// LeaderOffsets is the ordered sequence of (role, term offset relative to
	// the first observed leader term) leadership observations.
	LeaderOffsets []LeaderFact `json:"leader_sequence,omitempty"`
	// LeadershipSwitched states whether a different node became leader at a
	// strictly higher term after the initial leader.
	LeadershipSwitched bool `json:"leadership_switched"`
	// WriteOutcomes records acceptance/rejection of each fixed-input write.
	WriteOutcomes []WriteOutcome `json:"write_outcomes,omitempty"`
	// SnapshotEvidence describes the compaction boundary a lagging node
	// crossed, when relevant.
	SnapshotEvidence *SnapshotFact `json:"snapshot_evidence,omitempty"`
	// MembershipEvidence describes an in-progress joint change hit by a
	// leader switch, when relevant.
	MembershipEvidence *MembershipFact `json:"membership_evidence,omitempty"`
	// FinalNodes are every node's final positions in fixed node order.
	FinalNodes []NodeFact `json:"final_nodes"`
	// Converged says all online nodes share commit/applied positions and the
	// same applied-content digest.
	Converged bool `json:"converged"`
	// HistoryDigest is the shared deterministic applied-history digest.
	HistoryDigest string `json:"history_digest,omitempty"`
	// Checks are the ordered judgments.
	Checks []CheckResult `json:"checks"`
	// Verdict is the final conclusion derived from Checks.
	Verdict Verdict `json:"verdict"`
}

// LeaderFact is one leadership transition record in role-relative terms.
type LeaderFact struct {
	Role       string `json:"role"`
	TermOffset uint64 `json:"term_offset"`
}

// WriteOutcome is the verdict of one fixed write attempt.
type WriteOutcome struct {
	Key    string `json:"key"`
	Phase  string `json:"phase"`
	Result string `json:"result"`
	// AppliedOnce states the key has exactly one effect across retries.
	AppliedOnce bool `json:"applied_once"`
	// RetryDedup states a same-payload retry was answered as a duplicate.
	RetryDedup bool `json:"retry_dedup,omitempty"`
}

// SnapshotFact is evidence that an offline node crossed a compaction
// boundary and recovered through it.
type SnapshotFact struct {
	OfflineRole       string `json:"offline_role"`
	ThresholdCrossed  bool   `json:"threshold_crossed"`
	LeaderSnapIndex   uint64 `json:"leader_snapshot_index_before_restart"`
	OfflineLastIndex  uint64 `json:"offline_last_index_before_restart"`
	RecoveredSnapIdx  uint64 `json:"recovered_snapshot_index"`
	SnapshotInstalled bool   `json:"snapshot_installed"`
}

// MembershipFact is evidence that a leader switch happened while a
// membership change was genuinely in progress.
type MembershipFact struct {
	NewVoterRole  string   `json:"new_voter_role"`
	PhaseAtCrash  string   `json:"phase_at_crash"`
	FinalVoterSet []string `json:"final_voter_set"`
	OldLeaderRole string   `json:"old_leader_role"`
	NewLeaderRole string   `json:"new_leader_role"`
	NewTermHigher bool     `json:"new_term_higher"`
}

// Report is the full machine-readable audit artifact. Facts are canonical
// and digest-stable across repeat runs; timing sections are informational.
type Report struct {
	Schema        string `json:"schema"`
	GeneratedAtMS int64  `json:"generated_at_ms"`
	DurationMS    int64  `json:"duration_ms"`
	// Facts is the deterministic body; its digest covers the whole verdict.
	Facts Fact `json:"facts"`
	// Digest is hex SHA-256 over the canonical-JSON encoding of Facts.
	Digest string `json:"digest"`
}

// finalizeEvidence fills canonical-role fields added after build and
// recomputes the digest. It runs after buildReport assigned roles.
func (h *harness) finalizeEvidence(r *Report) error {
	if r.Facts.MembershipEvidence != nil {
		r.Facts.MembershipEvidence.FinalVoterSet = h.rolesOf(h.finalVoterIDs)
	}
	dg, err := r.Facts.canonicalDigest()
	if err != nil {
		return err
	}
	r.Digest = dg
	return nil
}
