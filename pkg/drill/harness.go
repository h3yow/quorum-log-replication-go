package drill

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/cluster"
	"github.com/h3yow/quorum-log-replication-go/pkg/raft"
	"github.com/h3yow/quorum-log-replication-go/pkg/statemachine"
)

// tickMS is the single fixed timing granularity for every drill. Election
// jitter and RPC latency vary in wall-clock terms, but all verdicts are
// derived from observed states rather than sleeps, so the conclusions are
// repeatable.
const tickMS = 20

// Role labels are assigned dynamically from event participation order, so
// reports never depend on which physical node id happened to win an election.
const (
	roleInitialLeader = "L0"
	roleNewLeader     = "L1"
	roleNewVoter      = "NEWVOTER"
	roleVictim        = "VICTIM"
)

// era is one observed leadership epoch: a distinct (leader node, term).
type era struct {
	leader string
	term   uint64
}

// harness runs one fixed-input drill against one fresh cluster and records
// its evidence. Nothing here is shared across scenarios.
type harness struct {
	id       ScenarioID
	ids      []string
	c        *cluster.Cluster
	sabotage bool

	// eras is the distinct leader/term timeline in first-observation order.
	eras []era
	// canonical physical-id -> semantic role labels, computed at report time.
	roleOf map[string]string
	// special roles claimed explicitly by scenarios.
	victimID   string
	newVoterID string

	isolated  []string
	crashed   []string
	restarted []string
	writes    []WriteOutcome

	// finalVoterIDs holds physical ids of the final committed voter set, used
	// to render membership evidence in canonical-role form after buildReport.
	finalVoterIDs []string
}

func newHarness(id ScenarioID, ids []string, dir string, sabotage bool) (*harness, error) {
	ctx := context.Background()
	c, err := cluster.New(ctx, cluster.Options{
		DataDir: dir, NodeIDs: ids, TickMS: tickMS,
	})
	if err != nil {
		return nil, err
	}
	return &harness{
		id: id, ids: append([]string(nil), ids...), c: c,
		sabotage: sabotage,
		roleOf:   map[string]string{},
	}, nil
}

func (h *harness) close() { h.c.Close() }

// waitLeader polls until a reachable leader exists and records the first
// leadership era. Canonical labels are assigned later from event semantics.
func (h *harness) waitLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ld := h.c.Leader(); ld != "" {
			h.sampleEra()
			return ld, nil
		}
		time.Sleep(tickMS * time.Millisecond)
	}
	return "", errors.New("no leader elected")
}

// setVictim marks the semantic VICTIM node (long-offline follower).
func (h *harness) setVictim(id string) { h.victimID = id }

// setNewVoter marks the semantic newly added voter node.
func (h *harness) setNewVoter(id string) { h.newVoterID = id }

// sampleEra records a distinct (leader, term) observation. Canonical labels
// are derived from the era timeline at report time, not physical identities.
func (h *harness) sampleEra() {
	ld := h.c.Leader()
	if ld == "" {
		return
	}
	term := h.status(ld).Term
	if len(h.eras) > 0 {
		last := h.eras[len(h.eras)-1]
		if last.leader == ld && last.term == term {
			return
		}
	}
	h.eras = append(h.eras, era{leader: ld, term: term})
}

// firstNewLeader returns the first era leader distinct from L0, or "".
func (h *harness) firstNewLeader() string {
	if len(h.eras) == 0 {
		return ""
	}
	for _, e := range h.eras[1:] {
		if e.leader != h.eras[0].leader {
			return e.leader
		}
	}
	return ""
}

func (h *harness) initialLeader() string {
	if len(h.eras) > 0 {
		return h.eras[0].leader
	}
	return ""
}

// buildCanonicalRoles assigns semantic labels by event participation so the
// fact body is independent of which physical id won each election:
//
//	L0         initial leader
//	L1         first distinct later leader
//	NEWVOTER   the node added by a membership change
//	VICTIM     the node taken offline across a compaction boundary
//	P1..Pk     all remaining, symmetry-equivalent peers in sorted id order
//
// allIDs are every physical id to label (initial members plus added nodes).
func (h *harness) buildCanonicalRoles(allIDs []string) map[string]string {
	m := map[string]string{}
	if l0 := h.initialLeader(); l0 != "" {
		m[l0] = roleInitialLeader
	}
	if l1 := h.firstNewLeader(); l1 != "" {
		m[l1] = roleNewLeader
	}
	if h.newVoterID != "" {
		m[h.newVoterID] = roleNewVoter
	}
	if h.victimID != "" {
		if _, taken := m[h.victimID]; !taken {
			m[h.victimID] = roleVictim
		}
	}
	rest := append([]string(nil), allIDs...)
	sort.Strings(rest)
	n := 0
	for _, id := range rest {
		if _, ok := m[id]; ok {
			continue
		}
		n++
		m[id] = fmt.Sprintf("P%d", n)
	}
	return m
}

// eraRank maps an observed absolute term to a repeatable ordinal: the index
// of the leadership era. Raw terms vary across runs (split-vote terms); the
// era sequence does not.
func (h *harness) eraRank(node string, term uint64) uint64 {
	_ = node
	if len(h.eras) == 0 {
		return 0
	}
	// Choose the last era whose term is <= the node's term (a follower may
	// report an era term before electing again).
	rank := uint64(0)
	for i, e := range h.eras {
		if term >= e.term {
			rank = uint64(i)
		}
	}
	return rank
}

// poll samples leadership eras until timeout or cond.
func (h *harness) poll(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		h.sampleEra()
		if cond() {
			return true
		}
		time.Sleep(tickMS / 2 * time.Millisecond)
	}
	return false
}

// pollHold requires cond to hold for hold consecutive samplings (separated
// by the sampling interval); any false resets the streak. This debounces
// transient equal readings during snapshot install or a moving commit.
func (h *harness) pollHold(timeout time.Duration, hold int, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	streak := 0
	for time.Now().Before(deadline) {
		h.sampleEra()
		if cond() {
			streak++
			if streak >= hold {
				return true
			}
		} else {
			streak = 0
		}
		time.Sleep(tickMS / 2 * time.Millisecond)
	}
	return false
}

// requireLeader waits for and returns a reachable leader.
func (h *harness) requireLeader(timeout time.Duration) (string, error) {
	if h.poll(timeout, func() bool { return h.c.Leader() != "" }) {
		return h.c.Leader(), nil
	}
	return "", errors.New("no leader")
}

// reachableVoters reports, from node fromID, which committed voters currently
// accept messages; the node itself is counted only when registered.
func (h *harness) reachableVoters(fromID string) []string {
	n := h.c.Node(fromID)
	if n == nil {
		return nil
	}
	voters := n.Status().CommittedConfig.Voters
	tr := h.c.Transport()
	out := []string{}
	for _, v := range voters {
		if v == fromID {
			if tr.Registered(v) {
				out = append(out, v)
			}
			continue
		}
		if tr.IsReachable(fromID, v) {
			out = append(out, v)
		}
	}
	return out
}

// partition isolates node id and records the evidence action.
func (h *harness) partition(id string, peers ...string) {
	h.c.Partition(id, peers...)
	h.isolated = appendIfMissing(h.isolated, id)
}

// crashNode stops+unregisters a node (real offline evidence).
func (h *harness) crashNode(ctx context.Context, id string) error {
	if err := h.c.Crash(ctx, id); err != nil {
		return err
	}
	h.crashed = appendIfMissing(h.crashed, id)
	h.isolated = removeString(h.isolated, id) // crashed subsumes isolated
	return nil
}

// restartNode reloads a crashed node from disk.
func (h *harness) restartNode(ctx context.Context, id string) error {
	if err := h.c.Restart(ctx, id); err != nil {
		return err
	}
	h.restarted = appendIfMissing(h.restarted, id)
	return nil
}

// put builds one fixed command with an explicit client/seq so retries can
// reuse the exact same payload.
func (h *harness) put(clientID string, seq uint64, key, val string) ([]byte, error) {
	cmd := statemachine.Command{
		Op: statemachine.CmdPut, Key: key, Value: val,
		ClientID: clientID, Seq: seq,
	}
	return cmd.Encode()
}

// proposeOnce sends one payload directly to the given leader handle with a
// bounded context and returns the proposal result without internal retry.
func (h *harness) proposeOnce(ctx context.Context, leader string, payload []byte) (raft.ProposeResult, error) {
	n := h.c.Node(leader)
	if n == nil {
		return raft.ProposeResult{}, raft.ErrStopped
	}
	pctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	return n.Propose(pctx, payload)
}

// recordWrite appends an ordered write judgment.
func (h *harness) recordWrite(key, phase, result string, once, dedup bool) {
	h.writes = append(h.writes, WriteOutcome{
		Key: key, Phase: phase, Result: result,
		AppliedOnce: once, RetryDedup: dedup,
	})
}

// settleCommit polls until every registered node has committed at least idx
// (use this when the tail may be a non-application no-op entry).
func (h *harness) settleCommit(idx uint64, timeout time.Duration) bool {
	return h.poll(timeout, func() bool {
		for id := range h.c.Observe().Nodes {
			if !h.c.Transport().Registered(id) {
				continue
			}
			if h.status(id).CommitIndex < idx {
				return false
			}
		}
		return true
	})
}

// maxCommit is the highest commit index among registered nodes right now.
func (h *harness) maxCommit() uint64 {
	var m uint64
	for id, s := range h.c.Observe().Nodes {
		if !h.c.Transport().Registered(id) {
			continue
		}
		if s.CommitIndex > m {
			m = s.CommitIndex
		}
	}
	return m
}

// stableCommit polls until node id's commit index has stayed unchanged for
// the quiet window. This drains in-flight acks that were already on the wire
// before a fault took effect, so a subsequent measurement reflects only
// what can commit AFTER the fault.
func (h *harness) stableCommit(id string, total, quiet time.Duration) (uint64, bool) {
	deadline := time.Now().Add(total)
	last := h.status(id).CommitIndex
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(tickMS / 2 * time.Millisecond)
		cur := h.status(id).CommitIndex
		if cur != last {
			last = cur
			stableSince = time.Now()
		}
		if time.Since(stableSince) >= quiet {
			return cur, true
		}
	}
	return h.status(id).CommitIndex, false
}

// status is a nil-safe status read.
func (h *harness) status(id string) raft.Status {
	if n := h.c.Node(id); n != nil {
		return n.Status()
	}
	return raft.Status{}
}

// converge waits for every registered node to share commit AND applied
// positions and the same applied-content digest, with the matching condition
// held across two consecutive samples so a mid-install/transient observation
// can never be mistaken for convergence. Offline (unregistered) nodes are
// excluded.
func (h *harness) converge(timeout time.Duration) (bool, uint64, string, map[string]raft.Status) {
	ok := h.pollHold(timeout, 2, func() bool {
		obs := h.c.Observe()
		var idx uint64
		live := 0
		for id, s := range obs.Nodes {
			if !h.c.Transport().Registered(id) {
				continue
			}
			live++
			if s.CommitIndex > idx {
				idx = s.CommitIndex
			}
		}
		if live == 0 || idx == 0 {
			return false
		}
		// Anchor on the applied-content digest of any fully-caught-up node.
		digest := ""
		for id, s := range obs.Nodes {
			if h.c.Transport().Registered(id) &&
				s.CommitIndex == idx && s.LastApplied == idx {
				digest = s.StateDigest
				break
			}
		}
		if digest == "" {
			return false
		}
		for id, s := range obs.Nodes {
			if !h.c.Transport().Registered(id) {
				continue
			}
			if s.CommitIndex != idx || s.LastApplied != idx || s.StateDigest != digest {
				return false
			}
		}
		return true
	})
	obs := h.c.Observe()
	snap := map[string]raft.Status{}
	var idx uint64
	digest := ""
	for id, s := range obs.Nodes {
		if !h.c.Transport().Registered(id) {
			continue
		}
		snap[id] = s
		if s.CommitIndex > idx {
			idx = s.CommitIndex
		}
		if s.CommitIndex == s.LastApplied {
			digest = s.StateDigest
		}
	}
	return ok, idx, digest, snap
}

// rolesOf renders a physical id list as sorted role labels.
func (h *harness) rolesOf(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if r, ok := h.roleOf[id]; ok {
			out = append(out, r)
		} else {
			out = append(out, "UNMAPPED:"+id)
		}
	}
	sort.Strings(out)
	return out
}

func appendIfMissing(xs []string, x string) []string {
	for _, v := range xs {
		if v == x {
			return xs
		}
	}
	return append(xs, x)
}

func removeString(xs []string, x string) []string {
	out := xs[:0]
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}
