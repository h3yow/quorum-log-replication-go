package drill

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// finalize derives the overall verdict from the recorded checks and computes
// the content hash. It never fabricates a PASS: Passed is true only when every
// recorded check passed.
func (r *Report) finalize() {
	r.Passed = true
	for _, c := range r.Checks {
		if !c.Passed {
			r.Passed = false
			break
		}
	}
	r.ContentHash = r.computeContentHash()
}

// computeContentHash hashes all fact-bearing fields in fixed order. Fields
// that legitimately vary across repeat runs with identical input are
// normalized away:
//
//   - timing fields (started_at/duration) are excluded entirely;
//   - per-node terms and current-leader identities depend on randomized
//     election scheduling, so they are zeroed (leadership-CHANGE evidence is
//     retained as boolean facts and term-delta checks);
//   - StateHash covers the generated per-run client dedup table, so it is
//     dropped in favour of the canonical KVHash of committed contents.
func (r *Report) computeContentHash() string {
	type nodeFinalNorm struct {
		ID            string   `json:"id"`
		CommitIndex   uint64   `json:"commit_index"`
		LastApplied   uint64   `json:"last_applied"`
		SnapshotIndex uint64   `json:"snapshot_index"`
		Voters        []string `json:"voters"`
		KVHash        string   `json:"kv_hash"`
	}
	type leaderEventNorm struct {
		Phase string `json:"phase"`
		// Changed records whether a switch happened; the concrete successor
		// id/term can differ run to run without changing history.
		Changed bool `json:"changed"`
	}
	type membershipNorm struct {
		NewNode        string   `json:"new_node"`
		ChangeInFlight bool     `json:"change_in_flight"`
		Switched       bool     `json:"switched"`
		FinalVoters    []string `json:"final_voters"`
	}
	type convergenceNorm struct {
		Reached       bool            `json:"reached"`
		CommonCommit  uint64          `json:"common_commit_index"`
		CommonApplied uint64          `json:"common_applied_index"`
		HistoryHash   string          `json:"history_hash"`
		Nodes         []nodeFinalNorm `json:"nodes"`
	}
	type checkNorm struct {
		ID     string `json:"id"`
		Passed bool   `json:"passed"`
	}
	type faultNorm struct {
		Kind       string `json:"kind"`
		Phase      string `json:"phase"`
		NodeCount  int    `json:"node_count"`
		TookEffect bool   `json:"took_effect"`
	}
	type compactionNorm struct {
		// VictimSlot is the stable role of the offline node ("first
		// follower"), not its run-dependent concrete id.
		VictimSlot string `json:"victim_slot"`
		// OfflineCommitAt is intentionally excluded: the index the victim
		// stopped at can differ by one depending on when the first leader's
		// no-op committed the bootstrap entry; it is kept in the full report
		// for inspection. The repeatable facts are that a compaction boundary
		// was crossed and the snapshot installed at a fixed, deterministic
		// index (driven by the fixed write counts and snapshot threshold).
		LeaderSnapshotAt  uint64 `json:"leader_snapshot_index_at_return"`
		CrossedCompaction bool   `json:"crossed_compaction"`
		InstalledSnapshot uint64 `json:"installed_snapshot_index"`
	}
	type writeNorm struct {
		Label       string `json:"label"`
		Applied     bool   `json:"applied"`
		EffectCount int    `json:"effect_count"`
	}
	type digest struct {
		Schema      string            `json:"schema"`
		Scenario    Scenario          `json:"scenario"`
		Input       Input             `json:"input"`
		Passed      bool              `json:"passed"`
		Checks      []checkNorm       `json:"checks"`
		LeaderTrace []leaderEventNorm `json:"leader_trace"`
		Faults      []faultNorm       `json:"faults"`
		Writes      []writeNorm       `json:"writes"`
		Compaction  *compactionNorm   `json:"compaction,omitempty"`
		Membership  *membershipNorm   `json:"membership,omitempty"`
		Convergence convergenceNorm   `json:"convergence"`
	}

	checks := make([]checkNorm, 0, len(r.Checks))
	for _, c := range r.Checks {
		checks = append(checks, checkNorm{ID: c.ID, Passed: c.Passed})
	}
	faults := make([]faultNorm, 0, len(r.Faults))
	for _, f := range r.Faults {
		faults = append(faults, faultNorm{
			Kind: f.Kind, Phase: f.Phase, NodeCount: len(f.Nodes), TookEffect: f.TookEffect,
		})
	}
	var cnorm *compactionNorm
	if r.Compaction != nil {
		cnorm = &compactionNorm{
			VictimSlot:        "first-follower",
			LeaderSnapshotAt:  r.Compaction.LeaderSnapshotAt,
			CrossedCompaction: r.Compaction.CrossedCompaction,
			InstalledSnapshot: r.Compaction.InstalledSnapshot,
		}
	}
	trace := make([]leaderEventNorm, 0, len(r.LeaderTrace))
	for _, e := range r.LeaderTrace {
		trace = append(trace, leaderEventNorm{Phase: e.Phase, Changed: e.Leader != ""})
	}
	writes := make([]writeNorm, 0, len(r.Writes))
	for _, w := range r.Writes {
		writes = append(writes, writeNorm{Label: w.Label, Applied: w.Applied, EffectCount: w.EffectCount})
	}
	nodes := make([]nodeFinalNorm, 0, len(r.Convergence.Nodes))
	for _, n := range r.Convergence.Nodes {
		nodes = append(nodes, nodeFinalNorm{
			ID: n.ID, CommitIndex: n.CommitIndex, LastApplied: n.LastApplied,
			SnapshotIndex: n.SnapshotIndex, Voters: n.Voters, KVHash: n.KVHash,
		})
	}
	var mnorm *membershipNorm
	if r.Membership != nil {
		mnorm = &membershipNorm{
			NewNode:        r.Membership.NewNode,
			ChangeInFlight: r.Membership.ChangeInFlight,
			Switched:       r.Membership.Switched,
			FinalVoters:    r.Membership.FinalVoters,
		}
	}
	b, err := json.Marshal(digest{
		Schema:      r.Schema,
		Scenario:    r.Scenario,
		Input:       r.Input,
		Passed:      r.Passed,
		Checks:      checks,
		LeaderTrace: trace,
		Faults:      faults,
		Writes:      writes,
		Compaction:  cnorm,
		Membership:  mnorm,
		Convergence: convergenceNorm{
			Reached:       r.Convergence.Reached,
			CommonCommit:  r.Convergence.CommonCommit,
			CommonApplied: r.Convergence.CommonApplied,
			HistoryHash:   r.Convergence.HistoryHash,
			Nodes:         nodes,
		},
	})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
