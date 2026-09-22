package raft

import (
	"encoding/json"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

// Configuration entries are EntryConfig payloads (JSON). A Config with
// non-empty JointVoters is the "joint" (C_old,new) configuration; the
// follow-up empty-JointVoters entry commits the final configuration.

func encodeConfig(c raftproto.Config) []byte {
	b, _ := json.Marshal(c)
	return b
}

func decodeConfig(b []byte) (raftproto.Config, error) {
	var c raftproto.Config
	err := json.Unmarshal(b, &c)
	return c, err
}

// latestConfigIndex returns the index of the newest EntryConfig in the
// available log, or 0 if none remains (snapshot's config then applies).
func (n *Node) latestConfigIndex() uint64 {
	first := n.firstIndex()
	last := n.lastIndex()
	var found uint64
	for i := first; i <= last; i++ {
		ents, err := n.log.Entries(i, i+1)
		if err != nil {
			break
		}
		if len(ents) == 1 && ents[0].Type == raftproto.EntryConfig {
			found = i
		}
	}
	return found
}

// rebuildActiveConfig recomputes the config applicable to the newest log
// entry: snapshot config, overridden by every EntryConfig still present.
// This is what elections and commit rules must use.
func (n *Node) rebuildActiveConfig() {
	cfg := n.snapMeta.Config
	first := n.firstIndex()
	last := n.lastIndex()
	for i := first; i <= last; i++ {
		ents, err := n.log.Entries(i, i+1)
		if err != nil {
			n.logger.warnf("rebuildActiveConfig: read %d: %v", i, err)
			break
		}
		if len(ents) == 1 && ents[0].Type == raftproto.EntryConfig {
			c, err := decodeConfig(ents[0].Data)
			if err != nil {
				n.logger.warnf("rebuildActiveConfig: decode %d: %v", i, err)
				continue
			}
			cfg = c
		}
	}
	n.config = cfg
}

// rebuildCommittedConfig reports the config that governs at commit index.
func (n *Node) rebuildCommittedConfig() {
	cfg := n.snapMeta.Config
	first := n.firstIndex()
	for i := first; i <= n.commit; i++ {
		ents, err := n.log.Entries(i, i+1)
		if err != nil {
			break
		}
		if len(ents) == 1 && ents[0].Type == raftproto.EntryConfig {
			if c, err := decodeConfig(ents[0].Data); err == nil {
				cfg = c
			}
		}
	}
	n.commCfg = cfg
}

// appendConfigEntry writes a config entry through the leader pipeline. It
// deduplicates: if the log tail already carries an identical config nothing
// is appended (nested applyTo calls can otherwise emit it twice).
func (n *Node) appendConfigEntry(cfg raftproto.Config) (uint64, error) {
	if last := n.lastIndex(); last > 0 {
		if ents, err := n.log.Entries(last, last+1); err == nil && len(ents) == 1 &&
			ents[0].Type == raftproto.EntryConfig {
			if c, derr := decodeConfig(ents[0].Data); derr == nil && sameConfig(c, cfg) {
				return last, nil
			}
		}
	}
	idx := n.lastIndex() + 1
	e := raftproto.Entry{
		Term:  n.term,
		Index: idx,
		Type:  raftproto.EntryConfig,
		Data:  encodeConfig(cfg),
	}
	if err := n.log.Append([]raftproto.Entry{e}); err != nil {
		return 0, err
	}
	n.logger.statef("config appended at %d voters=%v learners=%v joint=%v",
		idx, cfg.Voters, cfg.Learners, cfg.JointVoters)
	n.rebuildActiveConfig()
	n.reinitProgress()
	n.broadcastAppend(0)
	return idx, nil
}

func sameConfig(a, b raftproto.Config) bool {
	return sameVoters(a.Voters, b.Voters) &&
		sameVoters(a.Learners, b.Learners) &&
		sameVoters(a.JointVoters, b.JointVoters)
}

// waitForCommit blocks the loop until idx is committed/applied under ctx.
// Must be invoked from the event loop and pumps events while waiting.
func (n *Node) waitForCommitIdx(idx uint64) <-chan struct{} {
	ch := make(chan struct{}, 1)
	n.waiters[idx] = append(n.waiters[idx], makeWaiter(ch))
	return ch
}

func makeWaiter(ch chan struct{}) chan applyResult {
	w := make(chan applyResult, 1)
	go func() {
		<-w
		ch <- struct{}{}
	}()
	return w
}

// isVoter reports membership in the currently applicable config (both
// voter sets count as voters while joint).
func isVoter(id string, cfg raftproto.Config) bool {
	for _, v := range cfg.Voters {
		if v == id {
			return true
		}
	}
	for _, v := range cfg.JointVoters {
		if v == id {
			return true
		}
	}
	return false
}

// isLearner reports whether id is staged but not yet voting.
func isLearner(id string, cfg raftproto.Config) bool {
	for _, v := range cfg.Learners {
		if v == id {
			return true
		}
	}
	return false
}

// hasMajority counts granted votes against both quorums of a joint config.
func hasMajority(grant map[string]bool, cfg raftproto.Config) bool {
	if !majorityIn(grant, cfg.Voters) {
		return false
	}
	if len(cfg.JointVoters) > 0 && !majorityIn(grant, cfg.JointVoters) {
		return false
	}
	return true
}

func majorityIn(grant map[string]bool, voters []string) bool {
	y := 0
	for _, v := range voters {
		if grant[v] {
			y++
		}
	}
	return y >= quorum(len(voters))
}

// quorum is majority size for a voter set.
func quorum(n int) int { return n/2 + 1 }
