package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/h3yow/quorum-log-replication-go/pkg/raft"
	"github.com/h3yow/quorum-log-replication-go/pkg/statemachine"
)

var clientCounter uint64

// Client provides linearizable KV operations with transparent leader
// redirect and idempotent retry semantics.
//
// Each client has a stable id and a strictly monotonic sequence number. The
// state machine records the last applied seq per client, so repeating the
// same command after a timeout (unknown commit outcome) never applies twice.
type Client struct {
	c        *Cluster
	clientID string
	seq      uint64
}

func newClient(c *Cluster) *Client {
	n := atomic.AddUint64(&clientCounter, 1)
	return &Client{c: c, clientID: fmt.Sprintf("client-%d", n)}
}

func (cl *Client) nextSeq() uint64 {
	cl.seq++
	return cl.seq
}

// Put writes a key through the leader, safely retrying ambiguous timeouts.
func (cl *Client) Put(ctx context.Context, key, value string) error {
	return cl.mutate(ctx, statemachine.Command{
		Op: statemachine.CmdPut, Key: key, Value: value,
	})
}

// Delete removes a key.
func (cl *Client) Delete(ctx context.Context, key string) error {
	return cl.mutate(ctx, statemachine.Command{Op: statemachine.CmdDelete, Key: key})
}

func (cl *Client) mutate(ctx context.Context, base statemachine.Command) error {
	base.ClientID = cl.clientID
	base.Seq = cl.nextSeq()
	data, err := base.Encode()
	if err != nil {
		return err
	}
	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		leaderID := cl.c.Leader()
		if leaderID == "" {
			lastErr = raft.ErrNoQuorum
			if !sleep(ctx, 30*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		leader := cl.c.Node(leaderID)
		if leader == nil {
			lastErr = raft.ErrNotLeader
			if !sleep(ctx, 20*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		// Bound each attempt; on timeout the outcome is unknown, so retry the
		// SAME payload (same client+seq): state-machine dedupe guarantees the
		// mutation cannot take effect twice.
		pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
		_, perr := leader.Propose(pctx, data)
		pcancel()
		if perr == nil {
			return nil
		}
		lastErr = perr
		if errors.Is(perr, raft.ErrStopped) {
			// The leader handle was replaced by a restart; rediscover quickly.
			if !sleep(ctx, 20*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		if errors.Is(perr, raft.ErrNotLeader) {
			if !sleep(ctx, 25*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		if !sleep(ctx, 40*time.Millisecond) {
			return ctx.Err()
		}
	}
	return fmt.Errorf("cluster client: giving up: %w", lastErr)
}

// Get performs a consistent read through the leader read-index protocol.
func (cl *Client) Get(ctx context.Context, key string) (string, bool, error) {
	var lastErr error
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		leaderID := cl.c.Leader()
		if leaderID == "" {
			lastErr = raft.ErrNoQuorum
			if !sleep(ctx, 30*time.Millisecond) {
				return "", false, ctx.Err()
			}
			continue
		}
		leader := cl.c.Node(leaderID)
		if leader == nil {
			continue
		}
		v, ok, err := leader.ReadIndex(ctx, key)
		if err == nil {
			return v, ok, nil
		}
		lastErr = err
		if !sleep(ctx, 25*time.Millisecond) {
			return "", false, ctx.Err()
		}
	}
	return "", false, lastErr
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
