// Package drill implements repeatable, evidence-based failure drills that
// run entirely offline against the in-process cluster. Each drill produces a
// machine-readable audit Report with a fixed field order and content
// checksums; a drill only reports PASS when its injected faults are proven to
// have taken effect and the expected safety property held.
package drill

// Scenario identifies a built-in failure drill.
type Scenario string

// Built-in drill scenarios.
const (
	// ScenarioMajorityDown: a majority of voters is made unreachable; no new
	// write may be acknowledged while the majority is gone.
	ScenarioMajorityDown Scenario = "majority-unreachable"
	// ScenarioLeaderCrashRetry: the leader crashes while a write outcome is
	// unknown; retrying the same logical request must apply exactly once.
	ScenarioLeaderCrashRetry Scenario = "leader-crash-retry"
	// ScenarioSnapshotCatchup: a node stays offline across at least one
	// compaction boundary and catches up via snapshot on its return.
	ScenarioSnapshotCatchup Scenario = "offline-compaction-catchup"
	// ScenarioMembershipLeaderSwitch: a membership change is in flight when
	// leadership changes; the change ends consistent, never half-applied.
	ScenarioMembershipLeaderSwitch Scenario = "membership-leader-switch"
	// ScenarioCatchupDuplicateSnapshot: a node misses a compaction boundary
	// while offline, catches up via InstallSnapshot, advances beyond it, and
	// then deterministically receives a DUPLICATE/older catch-up snapshot
	// (network retransmission). The duplicate must be a no-op: no state
	// rollback, the reported applied position keeps matching real contents and
	// the cluster converges to one history.
	ScenarioCatchupDuplicateSnapshot Scenario = "offline-catchup-duplicate-delivery"
)

// Scenarios lists all built-in drills in their fixed run order.
func Scenarios() []Scenario {
	return []Scenario{
		ScenarioMajorityDown,
		ScenarioLeaderCrashRetry,
		ScenarioSnapshotCatchup,
		ScenarioCatchupDuplicateSnapshot,
		ScenarioMembershipLeaderSwitch,
	}
}

// CheckResult is one named safety/evidence assertion with evidence detail.
type CheckResult struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Evidence string `json:"evidence"`
}

// NodeFinal is the final per-node convergence snapshot recorded in a report.
type NodeFinal struct {
	ID            string   `json:"id"`
	State         string   `json:"state"`
	Term          uint64   `json:"term"`
	Leader        string   `json:"leader"`
	CommitIndex   uint64   `json:"commit_index"`
	LastApplied   uint64   `json:"last_applied"`
	SnapshotIndex uint64   `json:"snapshot_index"`
	Voters        []string `json:"voters"`
	StateHash     string   `json:"state_hash"`
	// KVHash is the canonical digest of committed key/value contents only;
	// it is the field used to compare history across repeat runs. StateHash
	// additionally covers the per-run dedup table and is informational.
	KVHash string `json:"kv_hash"`
}

// Report is the machine-readable audit report of one drill run.
//
// Field order is fixed and part of the on-disk contract. Timing fields
// (StartedAtISO, DurationMS) are allowed to differ across repeated runs; every
// other fact-bearing field feeds ContentHash so a repeat run with the same
// input produces the same historical facts and verdict.
type Report struct {
	Schema      string            `json:"schema"`
	Scenario    Scenario          `json:"scenario"`
	Input       Input             `json:"input"`
	Passed      bool              `json:"passed"`
	Checks      []CheckResult     `json:"checks"`
	LeaderTrace []LeaderEvent     `json:"leader_trace"`
	Faults      []FaultRecord     `json:"faults"`
	Writes      []WriteRecord     `json:"writes"`
	Compaction  *CompactionRecord `json:"compaction,omitempty"`
	// DuplicateCatchup is set by the offline-catchup-duplicate-delivery drill
	// and records the deterministic stale/duplicate snapshot retransmission
	// evidence.
	DuplicateCatchup *DuplicateCatchupRecord `json:"duplicate_catchup,omitempty"`
	Membership       *MembershipRecord       `json:"membership,omitempty"`
	Convergence      ConvergenceRecord       `json:"convergence"`
	StartedAtISO     string                  `json:"started_at_iso"`
	DurationMS       int64                   `json:"duration_ms"`
	ContentHash      string                  `json:"content_hash"`
}

// Input is the fixed, repeatable input of a drill. Two runs with an identical
// Input must produce an identical verdict and identical historical facts.
type Input struct {
	NodeCount    int      `json:"node_count"`
	NodeIDs      []string `json:"node_ids"`
	WritesBefore int      `json:"writes_before"`
	WritesDuring int      `json:"writes_during"`
	WritesAfter  int      `json:"writes_after"`
	KeyPrefix    string   `json:"key_prefix"`
	// CrashDelayMS is the fixed delay between issuing the ambiguous proposal
	// and crashing its leader in the leader-crash drill. It is part of the
	// input so the crash/proposal race is reproducible, not wall-clock random.
	CrashDelayMS int `json:"crash_delay_ms"`
	// ChangeDelayMS is the fixed delay between starting the membership change
	// and crashing its leader in the membership-switch drill, guaranteeing the
	// change is observably in flight (joint/learner stage) at crash time.
	ChangeDelayMS int `json:"change_delay_ms"`
	// Seed is reserved for any pseudo-random choice and is currently fixed at
	// zero: drills are deterministic without it.
	Seed int64 `json:"seed"`
}

// LeaderEvent records one observed leadership fact in order.
type LeaderEvent struct {
	Phase  string `json:"phase"`
	Leader string `json:"leader"`
	Term   uint64 `json:"term"`
}

// FaultRecord records one injected fault and the evidence that it took effect.
type FaultRecord struct {
	Kind       string   `json:"kind"`
	Nodes      []string `json:"nodes"`
	Phase      string   `json:"phase"`
	TookEffect bool     `json:"took_effect"`
	Evidence   string   `json:"evidence"`
}

// WriteRecord records the observed fate of one distinguished write.
type WriteRecord struct {
	Label          string `json:"label"`
	ClientID       string `json:"client_id"`
	Seq            uint64 `json:"seq"`
	Accepted       bool   `json:"accepted"`
	RejectedReason string `json:"rejected_reason,omitempty"`
	// Applied is true exactly when the mutation took effect once in the
	// converged state machine history.
	Applied bool `json:"applied"`
	// DupObservations counts how many successful proposals returned the
	// idempotent-dedup marker, proving retries did not re-apply.
	DupObservations int `json:"dup_observations"`
	// EffectCount is the number of non-duplicate applications found in the
	// converged committed history; safety requires it to be 0 or 1.
	EffectCount int `json:"effect_count"`
}

// CompactionRecord captures the snapshot/compaction evidence for a lagging
// node catch-up drill.
type CompactionRecord struct {
	Node              string `json:"node"`
	OfflineCommitAt   uint64 `json:"offline_commit_at"`
	LeaderSnapshotAt  uint64 `json:"leader_snapshot_index_at_return"`
	CrossedCompaction bool   `json:"crossed_compaction"`
	InstalledSnapshot uint64 `json:"installed_snapshot_index"`
}

// DuplicateCatchupRecord captures evidence for the deterministic duplicate
// catch-up delivery drill. The safety fact is that re-delivering an older or
// already-installed snapshot after the node advanced past it changes neither
// its applied frontier nor its state-machine contents.
type DuplicateCatchupRecord struct {
	// Node is the role-slot of the victim ("first-follower"); the concrete id
	// is run-dependent and excluded from the content hash.
	Node string `json:"node"`
	// OfflineCommitAt is the commit position the victim had when taken offline.
	OfflineCommitAt uint64 `json:"offline_commit_at"`
	// InstalledSnapshot is the boundary snapshot the victim installed to cross
	// the compaction point.
	InstalledSnapshot uint64 `json:"crossed_compaction"`
	// AdvancedTo is the applied position the victim reached via incremental
	// AppendEntries BEFORE the duplicate snapshot was re-delivered; the
	// duplicate must not move it.
	AdvancedTo uint64 `json:"advanced_to_before_replay"`
	// ReplayedOldest / ReplayedNewest are the snapshot indexes re-delivered
	// (the stale boundary snapshot and the just-installed one). Both are <= the
	// victim's installed/applied frontier.
	ReplayedOldest uint64 `json:"replayed_oldest_snapshot"`
	ReplayedNewest uint64 `json:"replayed_newest_snapshot"`
	// AppliedAfterReplay / ContentsChangedAfterReplay are the post-replay
	// observations. Safety requires the frontier to be unchanged and the
	// contents hash to be identical.
	AppliedAfterReplay      uint64 `json:"applied_after_replay"`
	ContentsChangedAfter    bool   `json:"contents_changed_after_replay"`
	FrontierRolledBackAfter bool   `json:"frontier_rolled_back_after_replay"`
	// ReplayTookEffect is true when the duplicate messages were really
	// delivered to the victim's handler (delivery actually happened, even
	// though a correct node ignores them).
	ReplayTookEffect bool `json:"replay_took_effect"`
}

// MembershipRecord captures a membership change interleaved with a leader
// switch.
type MembershipRecord struct {
	NewNode string `json:"new_node"`
	// ChangeInFlight is the stable fact that the change was underway but not
	// finalized when the leader was taken down (new node was a staged learner
	// or inside a joint config). It is what feeds the content hash.
	ChangeInFlight bool     `json:"change_in_flight"`
	JointObserved  bool     `json:"joint_observed"`
	JointVoters    []string `json:"joint_voters,omitempty"`
	OldLeader      string   `json:"old_leader"`
	NewLeader      string   `json:"new_leader"`
	Switched       bool     `json:"switched"`
	FinalVoters    []string `json:"final_voters"`
}

// ConvergenceRecord is the final whole-cluster consistency verdict.
type ConvergenceRecord struct {
	Reached       bool        `json:"reached"`
	CommonCommit  uint64      `json:"common_commit_index"`
	CommonApplied uint64      `json:"common_applied_index"`
	HistoryHash   string      `json:"history_hash"`
	Nodes         []NodeFinal `json:"nodes"`
}
