package logstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

func TestAppendEntriesAndHardStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ents := []raftproto.Entry{
		{Term: 1, Index: 1, Type: raftproto.EntryConfig, Data: []byte("cfg")},
		{Term: 1, Index: 2, Type: raftproto.EntryNormal, Data: []byte("a")},
		{Term: 2, Index: 3, Type: raftproto.EntryNormal, Data: []byte("b")},
	}
	if err := st.Append(ents); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveHardState(raftproto.HardState{Term: 2, VoteFor: "n1", Commit: 3, Apply: 3}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	hs, _ := st2.HardState()
	if hs.Term != 2 || hs.VoteFor != "n1" || hs.Commit != 3 || hs.Apply != 3 {
		t.Fatalf("hard state mismatch: %+v", hs)
	}
	got, err := st2.Entries(1, 4)
	if err != nil || len(got) != 3 {
		t.Fatalf("entries: %v %d", err, len(got))
	}
	if string(got[2].Data) != "b" {
		t.Fatalf("data mismatch: %q", got[2].Data)
	}
}

func TestCorruptPayloadRejected(t *testing.T) {
	dir := t.TempDir()
	st, _ := New(dir)
	if err := st.Append([]raftproto.Entry{{Term: 1, Index: 1, Data: []byte("hello world")}}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	p := filepath.Join(dir, walName)
	b, _ := os.ReadFile(p)
	// Flip a byte in the CRC-protected middle (avoid the length/magic header).
	b[len(b)/2] ^= 0xFF
	os.WriteFile(p, b, 0o644)

	_, err := New(dir)
	if !errors.Is(err, ErrCorruptLog) {
		t.Fatalf("want ErrCorruptLog, got %v", err)
	}
}

func TestTornTailReportedAsTruncated(t *testing.T) {
	dir := t.TempDir()
	st, _ := New(dir)
	if err := st.Append([]raftproto.Entry{
		{Term: 1, Index: 1, Data: []byte("xxxx")},
		{Term: 1, Index: 2, Data: []byte("yyyy")},
	}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	p := filepath.Join(dir, walName)
	info, _ := os.Stat(p)
	os.Truncate(p, info.Size()-5) // cut through the last record

	_, err := New(dir)
	if !errors.Is(err, ErrTruncatedLog) {
		t.Fatalf("want ErrTruncatedLog, got %v", err)
	}
}

func TestTruncateAndCompact(t *testing.T) {
	dir := t.TempDir()
	st, _ := New(dir)
	snaps, _ := NewSnapshotStore(dir)
	for i := uint64(1); i <= 6; i++ {
		if err := st.Append([]raftproto.Entry{{Term: 1, Index: i, Data: []byte{byte(i)}}}); err != nil {
			t.Fatal(err)
		}
	}
	// Conflict at 4: replace suffix.
	if err := st.Truncate(4, []raftproto.Entry{
		{Term: 2, Index: 4, Data: []byte("z")},
	}); err != nil {
		t.Fatal(err)
	}
	if got := st.LastIndex(); got != 4 {
		t.Fatalf("last=%d want 4", got)
	}
	if t0, _ := st.Term(4); t0 != 2 {
		t.Fatalf("term at 4 = %d want 2", t0)
	}
	// Compaction without snapshot cover must be refused.
	if err := st.Compact(4); !errors.Is(err, ErrBadRange) {
		t.Fatalf("compaction without snapshot should fail, got %v", err)
	}
	// With a snapshot covering 3 it must succeed.
	if err := snaps.Save(raftproto.SnapshotMeta{Index: 3, Term: 1}, []byte("snap")); err != nil {
		t.Fatal(err)
	}
	if err := st.Compact(3); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if st.FirstIndex() != 4 {
		t.Fatalf("first after compact = %d want 4", st.FirstIndex())
	}
}

func TestSnapshotCorruptionRejected(t *testing.T) {
	dir := t.TempDir()
	snaps, _ := NewSnapshotStore(dir)
	if err := snaps.Save(raftproto.SnapshotMeta{Index: 5, Term: 2, Config: raftproto.Config{Voters: []string{"a"}}}, []byte("data")); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, snapName)
	b, _ := os.ReadFile(p)
	b[len(b)/2] ^= 0xAA
	os.WriteFile(p, b, 0o644)
	_, _, err := snaps.Load()
	if !errors.Is(err, ErrCorruptSnapshot) {
		t.Fatalf("want ErrCorruptSnapshot, got %v", err)
	}
}

func TestSnapshotOlderRefused(t *testing.T) {
	dir := t.TempDir()
	snaps, _ := NewSnapshotStore(dir)
	if err := snaps.Save(raftproto.SnapshotMeta{Index: 10}, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := snaps.Save(raftproto.SnapshotMeta{Index: 9}, []byte("y")); !errors.Is(err, ErrSnapOutOfDate) {
		t.Fatalf("want ErrSnapOutOfDate, got %v", err)
	}
}
