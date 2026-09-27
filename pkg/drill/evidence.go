package drill

import (
	"fmt"
	"sort"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
	"github.com/h3yow/quorum-log-replication-go/pkg/statemachine"
)

// recorder accumulates checks and facts for one report.
type recorder struct {
	rep    *Report
	frames int
}

func newRecorder(s Scenario, in Input) *recorder {
	return &recorder{rep: &Report{
		Schema:      "qlog-drill/v1",
		Scenario:    s,
		Input:       in,
		Checks:      []CheckResult{},
		LeaderTrace: []LeaderEvent{},
		Faults:      []FaultRecord{},
		Writes:      []WriteRecord{},
	}}
}

func (rc *recorder) check(id string, passed bool, evidence string) {
	rc.rep.Checks = append(rc.rep.Checks, CheckResult{
		ID: id, Passed: passed, Evidence: evidence,
	})
}

func (rc *recorder) fault(kind, phase string, nodes []string, tookEffect bool, evidence string) {
	rc.rep.Faults = append(rc.rep.Faults, FaultRecord{
		Kind: kind, Nodes: append([]string(nil), nodes...), Phase: phase,
		TookEffect: tookEffect, Evidence: evidence,
	})
}

// waitLeader polls until one node reports leader, up to timeout.
func (fr *frame) waitLeader(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := fr.c.Leader(); l != "" {
			return l
		}
		time.Sleep(fr.tick())
	}
	return ""
}

func (fr *frame) tick() time.Duration { return 20 * time.Millisecond }

// recordLeader appends a leadership observation to the trace.
func (rc *recorder) recordLeader(phase, leader string, term uint64) {
	rc.rep.LeaderTrace = append(rc.rep.LeaderTrace, LeaderEvent{Phase: phase, Leader: leader, Term: term})
}

// leaderTerm returns the current observed leader and its term.
func (fr *frame) leaderTerm() (string, uint64) {
	l := fr.c.Leader()
	if l == "" {
		return "", 0
	}
	return l, fr.c.Node(l).Status().Term
}

// majorityOf is the classic Raft majority size for n voters.
func majorityOf(n int) int { return n/2 + 1 }

// countCommittedEffect scans retained logs for normal entries whose decoded
// command matches (clientID,seq); it returns how many DISTINCT log positions
// (identified by term+index cluster-wide) carry that exact command. Raft
// replication stores one copy per node, so counting raw copies would report
// the cluster size rather than the number of effects.
func countCommittedEffect(fr *frame, clientID string, seq uint64) (int, error) {
	type pos struct {
		term  uint64
		index uint64
	}
	seen := map[pos]bool{}
	for _, id := range fr.input.NodeIDs {
		ents, err := fr.c.RetainedEntries(id)
		if err != nil {
			return 0, err
		}
		for _, e := range ents {
			if e.Config {
				continue
			}
			cmd, derr := statemachine.DecodeCommand(e.Data)
			if derr != nil {
				continue
			}
			if cmd.ClientID == clientID && cmd.Seq == seq {
				seen[pos{e.Term, e.Index}] = true
			}
		}
	}
	return len(seen), nil
}

// snapshotConvergence polls until all live nodes share one commit/applied
// position and one canonical KV hash. It does not need multi-sample
// debouncing: a node's applied watermark and its state-machine contents are
// published from the same raft event-loop step (contents are applied before
// the observation cache is refreshed), so a sample that shows every node at
// one common applied position with one common KV hash is already the
// quiescent final state — applied positions only move forward. The generous
// timeout exists solely to absorb scheduling slowness under -race.
func (fr *frame) snapshotConvergence(timeout time.Duration) ConvergenceRecord {
	var last ConvergenceRecord
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		last = fr.readConvergence()
		if last.Reached {
			return last
		}
		time.Sleep(fr.tick())
	}
	return last
}

func (fr *frame) readConvergence() ConvergenceRecord {
	ids := append([]string(nil), fr.input.NodeIDs...)
	sort.Strings(ids)
	rec := ConvergenceRecord{Nodes: make([]NodeFinal, 0, len(ids))}
	var commit, applied uint64
	var hash string
	first := true
	same := true
	for _, id := range ids {
		snap, ok := fr.c.NodeState(id)
		if !ok {
			// A node absent from observation (removed member) is ignored for
			// convergence of the surviving configuration.
			continue
		}
		s := snap.Status
		rec.Nodes = append(rec.Nodes, NodeFinal{
			ID: id, State: s.State.String(), Term: s.Term, Leader: s.Leader,
			CommitIndex: s.CommitIndex, LastApplied: s.LastApplied,
			SnapshotIndex: s.SnapshotIndex,
			Voters:        append([]string(nil), s.CommittedConfig.Voters...),
			StateHash:     snap.StateHash, KVHash: snap.KVHash,
		})
		if first {
			commit, applied, hash = s.CommitIndex, s.LastApplied, snap.KVHash
			first = false
		} else if s.CommitIndex != commit || s.LastApplied != applied || snap.KVHash != hash {
			same = false
		}
	}
	rec.Reached = !first && same
	rec.CommonCommit = commit
	rec.CommonApplied = applied
	rec.HistoryHash = hash
	return rec
}

var _ = raftproto.StateLeader
var _ = fmt.Sprintf
