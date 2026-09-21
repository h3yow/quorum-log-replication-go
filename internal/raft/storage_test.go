package raft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStorageAppendReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := SingleConfig("a", "b", "c")
	es := []LogEntry{
		{Index: 1, Term: 1, Kind: EntryConf, Conf: &cfg},
		{Index: 2, Term: 1, Kind: EntryNormal, ClientID: "x", Seq: 1, Data: []byte("d1")},
		{Index: 3, Term: 2, Kind: EntryNormal, ClientID: "x", Seq: 2, Data: []byte("d2")},
	}
	if err := s.Append(es); err != nil {
		t.Fatal(err)
	}
	if s.LastIndex() != 3 {
		t.Fatalf("last=%d", s.LastIndex())
	}
	got, err := s.Entries(1, 4)
	if err != nil || len(got) != 3 {
		t.Fatalf("entries: %v %d", err, len(got))
	}
	if got[2].ClientID != "x" || got[2].Seq != 2 {
		t.Fatalf("entry decode mismatch: %+v", got[2])
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开：内容完整恢复。
	s2, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if s2.LastIndex() != 3 {
		t.Fatalf("reopen last=%d", s2.LastIndex())
	}
	term, _ := s2.TermAt(3)
	if term != 2 {
		t.Fatalf("term@3=%d", term)
	}
}

func TestStorageTruncatedVsCorrupt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(b []byte) []byte
		wantErr error
	}{
		{"truncated-tail", func(b []byte) []byte { return b[:len(b)-4] }, ErrTruncatedStorage},
		{"corrupt-crc", func(b []byte) []byte { b[len(b)-8] ^= 0xFF; return b }, ErrCorruptStorage},
		{"corrupt-magic", func(b []byte) []byte { b[0] ^= 0xFF; return b }, ErrCorruptStorage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := NewFileStorage(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Append([]LogEntry{{Index: 1, Term: 1, Kind: EntryNormal, Data: []byte("hello")}}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(dir, "log.bin")
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, tc.mutate(b), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err = NewFileStorage(dir)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestStorageSnapshotCompaction(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := SingleConfig("a", "b")
	if err := s.Append([]LogEntry{
		{Index: 1, Term: 1, Kind: EntryConf, Conf: &cfg},
		{Index: 2, Term: 1, Kind: EntryNormal, Data: []byte("a")},
		{Index: 3, Term: 1, Kind: EntryNormal, Data: []byte("b")},
		{Index: 4, Term: 2, Kind: EntryNormal, Data: []byte("c")},
	}); err != nil {
		t.Fatal(err)
	}
	meta := SnapshotMeta{LastIndex: 3, LastTerm: 1, Config: cfg}
	if err := s.CreateSnapshot(meta, []byte("SNAP")); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if s.BaseIndex() != 3 {
		t.Fatalf("base=%d", s.BaseIndex())
	}
	got, err := s.Entries(4, 5)
	if err != nil || len(got) != 1 || got[0].Data[0] != 'c' {
		t.Fatalf("post-snapshot entries: %v %v", got, err)
	}
	// 快照点之前的数据已不在日志（空洞由快照覆盖）。
	if _, err := s.Entry(2); !errors.Is(err, ErrGap) {
		t.Fatalf("expect ErrGap, got %v", err)
	}
	// 落后快照必须被拒绝。
	if err := s.CreateSnapshot(SnapshotMeta{LastIndex: 2, LastTerm: 1, Config: cfg}, nil); !errors.Is(err, ErrSnapshotBehind) {
		t.Fatalf("expect behind, got %v", err)
	}
	// 快照可读。
	m, r, ok, err := s.Snapshot()
	if err != nil || !ok || m.LastIndex != 3 {
		t.Fatalf("snapshot read: %v ok=%v meta=%+v", err, ok, m)
	}
	r.Close()
}

func TestQuorumAndConfig(t *testing.T) {
	cfg := SingleConfig("a", "b", "c")
	if !cfg.HasQuorum(ConfigSet{"a": {}, "b": {}}) {
		t.Fatal("2/3 应为多数")
	}
	if cfg.HasQuorum(ConfigSet{"a": {}}) {
		t.Fatal("1/3 不应为多数")
	}
	joint := Config{Voters: SingleConfig("a", "b", "c").Voters, Joint: SingleConfig("b", "c", "d").Voters}
	// C_old={a,b} 够多数，但 C_new={b} 仅 1/3：联合配置不得通过。
	if joint.HasQuorum(ConfigSet{"a": {}, "b": {}}) {
		t.Fatal("C_new 仅 1/3 不应通过")
	}
	if !joint.HasQuorum(ConfigSet{"a": {}, "b": {}, "c": {}, "d": {}}) {
		t.Fatal("双多数应通过")
	}
	if err := ValidateChange(SingleConfig("a").Voters, SingleConfig("x", "y").Voters); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("零交集变更应拒绝, got %v", err)
	}
}
