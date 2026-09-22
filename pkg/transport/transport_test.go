package transport

import (
	"context"
	"errors"
	"testing"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

type stub struct{ id string }

func (s *stub) HandleAppendEntries(ctx context.Context, from string, r *raftproto.AppendEntriesRequest) (*raftproto.AppendEntriesResponse, error) {
	return &raftproto.AppendEntriesResponse{Term: 1, Success: true}, nil
}
func (s *stub) HandleRequestVote(ctx context.Context, from string, r *raftproto.RequestVoteRequest) (*raftproto.RequestVoteResponse, error) {
	return &raftproto.RequestVoteResponse{Term: 1, VoteGranted: true}, nil
}
func (s *stub) HandleInstallSnapshot(ctx context.Context, from string, r *raftproto.InstallSnapshotRequest) (*raftproto.InstallSnapshotResponse, error) {
	return &raftproto.InstallSnapshotResponse{Term: 1, Success: true}, nil
}

func TestPartitionBlocksDelivery(t *testing.T) {
	tr := New()
	a, b := &stub{id: "a"}, &stub{id: "b"}
	tr.Register("a", a)
	tr.Register("b", b)
	if _, err := tr.SendAppendEntries(context.Background(), "a", "b", &raftproto.AppendEntriesRequest{}); err != nil {
		t.Fatalf("before partition: %v", err)
	}
	tr.Partition("a", "b")
	_, err := tr.SendAppendEntries(context.Background(), "a", "b", &raftproto.AppendEntriesRequest{})
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("during partition want ErrUnreachable, got %v", err)
	}
	tr.HealAll()
	if _, err := tr.SendAppendEntries(context.Background(), "a", "b", &raftproto.AppendEntriesRequest{}); err != nil {
		t.Fatalf("after heal: %v", err)
	}
}

func TestIsolatedMinorityCannotSeeQuorum(t *testing.T) {
	tr := New()
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		tr.Register(id, &stub{id: id})
	}
	tr.Partition("a") // isolate a from everyone
	got := tr.ReachableVoters("a", []string{"a", "b", "c", "d", "e"})
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("isolated node sees %v, want only itself", got)
	}
}
