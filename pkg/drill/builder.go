package drill

import (
	"sort"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raft"
)

// buildReport converts harness evidence plus its ordered checks into the
// canonical report. The fact body contains no wall-clock data; only the outer
// Report records timing for human reference.
func (h *harness) buildReport(checks []CheckResult, final map[string]raft.Status, digest string, converged bool, started time.Time) (*Report, error) {
	h.sampleEra()

	// Canonical labels are semantic (L0/L1/NEWVOTER/VICTIM/P1..), independent
	// of which physical node id won each election.
	allIDs := make([]string, 0, len(final))
	for id := range final {
		allIDs = append(allIDs, id)
	}
	if h.newVoterID != "" {
		allIDs = append(allIDs, h.newVoterID)
	}
	h.roleOf = h.buildCanonicalRoles(allIDs)

	fact := Fact{
		Scenario:      string(h.id),
		Nodes:         len(h.ids),
		Converged:     converged,
		HistoryDigest: digest,
	}

	// Ordered leader timeline in era-ordinal (repeatable) terms.
	for _, e := range h.eras {
		fact.LeaderOffsets = append(fact.LeaderOffsets, LeaderFact{
			Role:       h.roleOf[e.leader],
			TermOffset: h.eraRank(e.leader, e.term),
		})
	}
	if len(h.eras) >= 2 && h.eras[len(h.eras)-1].leader != h.eras[0].leader {
		fact.LeadershipSwitched = true
	}

	fact.IsolatedRoles = h.rolesOf(sortedUnique(h.isolated))
	fact.CrashedRoles = h.rolesOf(sortedUnique(h.crashed))
	fact.RestartedRoles = h.rolesOf(sortedUnique(h.restarted))
	fact.WriteOutcomes = append([]WriteOutcome(nil), h.writes...)

	// Final node facts ordered by role for stable array positioning.
	type rs struct {
		role string
		id   string
		s    raft.Status
	}
	var rows []rs
	for id, s := range final {
		rows = append(rows, rs{role: h.roleOf[id], id: id, s: s})
	}
	sort.Slice(rows, func(i, j int) bool { return roleOrder(rows[i].role) < roleOrder(rows[j].role) })
	for _, r := range rows {
		offline := !h.c.Transport().Registered(r.id)
		nf := NodeFact{
			Role:          r.role,
			State:         r.s.State.String(),
			TermOffset:    h.eraRank(r.id, r.s.Term),
			CommitIndex:   r.s.CommitIndex,
			LastApplied:   r.s.LastApplied,
			SnapshotIndex: r.s.SnapshotIndex,
			Voters:        h.rolesOf(r.s.CommittedConfig.Voters),
			JointVoters:   h.rolesOf(r.s.CommittedConfig.JointVoters),
			Learners:      h.rolesOf(r.s.CommittedConfig.Learners),
			Offline:       offline,
		}
		fact.FinalNodes = append(fact.FinalNodes, nf)
	}

	fact.Checks = append([]CheckResult(nil), checks...)
	allPass := true
	for _, ck := range fact.Checks {
		if !ck.Pass {
			allPass = false
		}
	}
	if allPass {
		fact.Verdict = VerdictPass
	} else {
		fact.Verdict = VerdictFail
	}

	dg, err := fact.canonicalDigest()
	if err != nil {
		return nil, err
	}
	return &Report{
		Schema:        reportSchema,
		GeneratedAtMS: time.Now().UnixMilli(),
		DurationMS:    time.Since(started).Milliseconds(),
		Facts:         fact,
		Digest:        dg,
	}, nil
}

func sortedUnique(xs []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	sort.Strings(out)
	return out
}

// roleOrder gives L0 < L1 < ... < NEWVOTER < VICTIM < P1 < P2 ... so final
// node facts and listings have a fixed position across runs.
func roleOrder(r string) int {
	if len(r) >= 1 && r[0] == 'L' {
		n := 0
		for i := 1; i < len(r); i++ {
			n = n*10 + int(r[i]-'0')
		}
		return n
	}
	if r == roleNewVoter {
		return 100
	}
	if r == roleVictim {
		return 150
	}
	if len(r) > 1 && r[0] == 'P' {
		n := 0
		for i := 1; i < len(r); i++ {
			n = n*10 + int(r[i]-'0')
		}
		return 200 + n
	}
	return 1000
}
