package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

const (
	walName  = "wal.log"
	hardName = "hardstate.bin"

	recHardState = 1
	recEntry     = 2

	magic0 byte = 0x51
	magic1 byte = 0x4C // "QL"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Bounds errors.
var (
	ErrCompacted = errors.New("logstore: index before snapshot (compacted)")
	ErrBadRange  = errors.New("logstore: requested range invalid")
)

// Store is the durable write-ahead log plus hard-state for one node.
//
// On-disk WAL framing (little endian):
//
//	0x51 0x4C | type:1 | len:4 | payload:len | crc32:4
//
// crc covers type+len+payload. A bad magic/crc means ErrCorruptLog, a record
// header or payload cut at EOF means ErrTruncatedLog: callers must refuse to
// serve rather than guessing.
type Store struct {
	dir string

	mu       sync.Mutex
	f        *os.File
	hard     raftproto.HardState
	entries  []raftproto.Entry // log window after the snapshot
	tornTail int64             // length of valid prefix when tail was torn
	syncOn   bool

	snap *SnapshotStore
}

// SetSync toggles fsync on durable writes. It defaults to true (crash-safe).
func (s *Store) SetSync(on bool) {
	s.mu.Lock()
	s.syncOn = on
	s.mu.Unlock()
}

// fsync must be called with s.mu already held (the durable write paths hold
// the lock); re-locking here would self-deadlock.
func (s *Store) fsync(f *os.File) error {
	if !s.syncOn {
		return nil
	}
	return f.Sync()
}

// New creates/opens a store rooted at dir, validating existing files eagerly.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("logstore: mkdir: %w", err)
	}
	s := &Store{dir: dir, syncOn: true}
	var err error
	if s.snap, err = NewSnapshotStore(dir); err != nil {
		return nil, err
	}
	if err := s.loadHardState(); err != nil {
		return nil, err
	}
	if err := s.loadWAL(); err != nil {
		return nil, err
	}
	if err := s.openForAppend(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) openForAppend() error {
	path := filepath.Join(s.dir, walName)
	if s.tornTail > 0 {
		// Drop only the torn tail; the validated prefix and the committed
		// state it represents remain intact.
		if err := os.Truncate(path, s.tornTail); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("logstore: open wal: %w", err)
	}
	s.f = f
	return nil
}

// Close flushes and releases file handles.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		if err := s.fsync(s.f); err != nil {
			return err
		}
		err := s.f.Close()
		s.f = nil
		return err
	}
	return nil
}

// ---------------- hard state ----------------
//
// current layout: term(8) voteLen(8) vote(voteLen) commit(8) apply(8) crc32(4)
// legacy layout:  term(8) voteLen(8) vote(voteLen) commit(8) crc32(4)
// crc covers everything before it.

func hardPath(dir string) string { return filepath.Join(dir, hardName) }

func (s *Store) loadHardState() error {
	b, err := os.ReadFile(hardPath(s.dir))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) < 8+8+8+4 {
		return ErrCorruptLog
	}
	off := len(b) - 4
	if crc32.Checksum(b[:off], crcTable) != binary.LittleEndian.Uint32(b[off:]) {
		return ErrCorruptLog
	}
	term := binary.LittleEndian.Uint64(b[0:8])
	voteLen := int(binary.LittleEndian.Uint64(b[8:16]))
	base := 16 + voteLen
	switch len(b) {
	case base + 8 + 4: // legacy: commit only
		commit := binary.LittleEndian.Uint64(b[base : base+8])
		s.hard = raftproto.HardState{Term: term, VoteFor: string(b[16:base]), Commit: commit}
	case base + 16 + 4: // current: commit + apply
		commit := binary.LittleEndian.Uint64(b[base : base+8])
		apply := binary.LittleEndian.Uint64(b[base+8 : base+16])
		s.hard = raftproto.HardState{Term: term, VoteFor: string(b[16:base]), Commit: commit, Apply: apply}
	default:
		return ErrCorruptLog
	}
	return nil
}

// SaveHardState atomically persists the hard state (fsync'd before a node
// acknowledges anything to its clients).
func (s *Store) SaveHardState(h raftproto.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h == s.hard {
		return nil // nothing durable changed; avoid a redundant fsync on the loop
	}
	vote := []byte(h.VoteFor)
	b := make([]byte, 0, 32+len(vote)+4)
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, h.Term)
	b = append(b, tmp...)
	binary.LittleEndian.PutUint64(tmp, uint64(len(vote)))
	b = append(b, tmp...)
	b = append(b, vote...)
	binary.LittleEndian.PutUint64(tmp, h.Commit)
	b = append(b, tmp...)
	binary.LittleEndian.PutUint64(tmp, h.Apply)
	b = append(b, tmp...)
	cb := make([]byte, 4)
	binary.LittleEndian.PutUint32(cb, crc32.Checksum(b, crcTable))
	b = append(b, cb...)
	if err := writeFileAtomicSync(hardPath(s.dir), b, s.syncOn); err != nil {
		return err
	}
	s.hard = h
	return nil
}

// HardState returns the persisted hard state.
func (s *Store) HardState() (raftproto.HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hard, nil
}

// ---------------- entries ----------------

// entry encoding: type:1 term:8 index:8 dataLen:4 data
func encodeEntry(e raftproto.Entry) []byte {
	b := make([]byte, 21+len(e.Data))
	b[0] = byte(e.Type)
	binary.LittleEndian.PutUint64(b[1:9], e.Term)
	binary.LittleEndian.PutUint64(b[9:17], e.Index)
	binary.LittleEndian.PutUint32(b[17:21], uint32(len(e.Data)))
	copy(b[21:], e.Data)
	return b
}

func decodeEntry(b []byte) (raftproto.Entry, error) {
	if len(b) < 21 {
		return raftproto.Entry{}, errors.New("short entry")
	}
	t := raftproto.EntryType(b[0])
	if t != raftproto.EntryNormal && t != raftproto.EntryConfig {
		return raftproto.Entry{}, fmt.Errorf("bad entry type %d", t)
	}
	dl := int(binary.LittleEndian.Uint32(b[17:21]))
	if 21+dl != len(b) {
		return raftproto.Entry{}, errors.New("entry length mismatch")
	}
	data := make([]byte, dl)
	copy(data, b[21:])
	return raftproto.Entry{
		Type:  t,
		Term:  binary.LittleEndian.Uint64(b[1:9]),
		Index: binary.LittleEndian.Uint64(b[9:17]),
		Data:  data,
	}, nil
}

func writeFileAtomic(path string, b []byte) error {
	return writeFileAtomicSync(path, b, true)
}

func writeFileAtomicSync(path string, b []byte, doSync bool) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if doSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func encodeRecord(recType byte, payload []byte) []byte {
	b := make([]byte, 0, 7+len(payload)+4)
	b = append(b, magic0, magic1, recType)
	lb := make([]byte, 4)
	binary.LittleEndian.PutUint32(lb, uint32(len(payload)))
	b = append(b, lb...)
	b = append(b, payload...)
	crcInput := make([]byte, 0, 5+len(payload))
	crcInput = append(crcInput, recType)
	crcInput = append(crcInput, lb...)
	crcInput = append(crcInput, payload...)
	cb := make([]byte, 4)
	binary.LittleEndian.PutUint32(cb, crc32.Checksum(crcInput, crcTable))
	b = append(b, cb...)
	return b
}

// loadWAL replays the file once, distinguishing corruption from truncation.
func (s *Store) loadWAL() error {
	f, err := os.Open(filepath.Join(s.dir, walName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	var ents []raftproto.Entry
	var validLen int64
	hdr := make([]byte, 7)
	for {
		n, err := io.ReadFull(f, hdr)
		if errors.Is(err, io.EOF) {
			break // clean end
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.ErrShortBuffer) {
			// Partial header at tail: torn write, recoverable prefix only.
			s.entries = ents
			s.tornTail = validLen
			return fmt.Errorf("%w: %d header bytes after offset %d", ErrTruncatedLog, n, validLen)
		}
		if err != nil {
			return err
		}
		if hdr[0] != magic0 || hdr[1] != magic1 {
			return fmt.Errorf("%w: bad magic at offset %d", ErrCorruptLog, validLen)
		}
		recType := hdr[2]
		plen := int(binary.LittleEndian.Uint32(hdr[3:7]))
		if plen > 64*1024*1024 {
			return fmt.Errorf("%w: implausible record length %d", ErrCorruptLog, plen)
		}
		payload := make([]byte, plen+4)
		pn, perr := io.ReadFull(f, payload)
		if errors.Is(perr, io.ErrUnexpectedEOF) || errors.Is(perr, io.ErrShortBuffer) {
			s.entries = ents
			s.tornTail = validLen
			return fmt.Errorf("%w: %d payload bytes after offset %d", ErrTruncatedLog, pn, validLen+7)
		}
		if perr != nil {
			return perr
		}
		body := payload[:plen]
		want := binary.LittleEndian.Uint32(payload[plen:])
		crcInput := make([]byte, 0, 5+plen)
		crcInput = append(crcInput, recType)
		crcInput = append(crcInput, hdr[3:7]...)
		crcInput = append(crcInput, body...)
		if crc32.Checksum(crcInput, crcTable) != want {
			return fmt.Errorf("%w: crc mismatch at offset %d", ErrCorruptLog, validLen)
		}
		switch recType {
		case recEntry:
			e, derr := decodeEntry(body)
			if derr != nil {
				return fmt.Errorf("%w: %v", ErrCorruptLog, derr)
			}
			ents = append(ents, e)
		case recHardState:
			// tolerated/ignored: hard state lives in its own atomic file
		default:
			return fmt.Errorf("%w: unknown record type %d", ErrCorruptLog, recType)
		}
		validLen += int64(7 + plen + 4)
	}
	if len(ents) > 0 {
		var snapIndex uint64
		if m, _, err := s.snap.Load(); err == nil && m != nil {
			snapIndex = m.Index
		}
		for i := range ents {
			if want := snapIndex + uint64(i) + 1; ents[i].Index != want {
				return fmt.Errorf("%w: non-contiguous index %d want %d", ErrCorruptLog, ents[i].Index, want)
			}
		}
	}
	s.entries = ents
	return nil
}

// snapIndex returns 0 or the latest snapshot's last included index.
func (s *Store) snapIndex() uint64 {
	if m, _, err := s.snap.Load(); err == nil && m != nil {
		return m.Index
	}
	return 0
}

// FirstIndex returns the first index still held (after the snapshot).
func (s *Store) FirstIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return s.snapIndex() + 1
	}
	return s.entries[0].Index
}

// LastIndex returns the highest on-disk index.
func (s *Store) LastIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return s.snapIndex()
	}
	return s.entries[len(s.entries)-1].Index
}

// Term returns the term of the entry at index, honoring the snapshot.
func (s *Store) Term(index uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snapMeta *raftproto.SnapshotMeta
	if m, _, err := s.snap.Load(); err == nil && m != nil {
		snapMeta = m
	}
	if snapMeta != nil {
		if index == snapMeta.Index {
			return snapMeta.Term, nil
		}
		if index < snapMeta.Index {
			return 0, ErrCompacted
		}
	}
	if len(s.entries) == 0 || index < s.entries[0].Index {
		return 0, ErrCompacted
	}
	if index > s.entries[len(s.entries)-1].Index {
		return 0, ErrBadRange
	}
	return s.entries[index-s.entries[0].Index].Term, nil
}

// Entries returns entries in [lo,hi).
func (s *Store) Entries(lo, hi uint64) ([]raftproto.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lo > hi {
		return nil, ErrBadRange
	}
	var snapMeta *raftproto.SnapshotMeta
	if m, _, err := s.snap.Load(); err == nil && m != nil {
		snapMeta = m
	}
	var first uint64 = 1
	if snapMeta != nil {
		first = snapMeta.Index + 1
	}
	if len(s.entries) > 0 {
		first = s.entries[0].Index
	}
	if lo < first {
		return nil, ErrCompacted
	}
	last := first // exclusive-ish guard
	if len(s.entries) > 0 {
		last = s.entries[len(s.entries)-1].Index + 1
	}
	if hi > last {
		return nil, ErrBadRange
	}
	out := make([]raftproto.Entry, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, s.entries[i-first])
	}
	return out, nil
}

// Append appends entries; they must be contiguous with the current tail.
func (s *Store) Append(entries []raftproto.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	want := uint64(0)
	if len(s.entries) > 0 {
		want = s.entries[len(s.entries)-1].Index
	} else {
		want = s.snapIndex()
	}
	for i := range entries {
		if entries[i].Index != want+1 {
			return fmt.Errorf("%w: append index %d want %d", ErrBadRange, entries[i].Index, want+1)
		}
		want = entries[i].Index
	}
	buf := make([]byte, 0)
	for i := range entries {
		buf = append(buf, encodeRecord(recEntry, encodeEntry(entries[i]))...)
	}
	if _, err := s.f.Write(buf); err != nil {
		return err
	}
	if err := s.fsync(s.f); err != nil {
		return err
	}
	s.entries = append(s.entries, entries...)
	return nil
}

// Truncate removes all entries with index >= from, then appends ents.
func (s *Store) Truncate(from uint64, ents []raftproto.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := uint64(1)
	var snapIndex uint64
	if m, _, err := s.snap.Load(); err == nil && m != nil {
		first = m.Index + 1
		snapIndex = m.Index
	}
	if len(s.entries) > 0 {
		first = s.entries[0].Index
	}
	if from <= snapIndex {
		return fmt.Errorf("%w: cannot truncate committed snapshot %d", ErrCompacted, from)
	}
	if from < first {
		return ErrCompacted
	}
	// Rewrite the WAL: prefix before `from` then new entries.
	var keep []raftproto.Entry
	if len(s.entries) > 0 {
		keep = s.entries[:from-first]
	}
	for i := range ents {
		if ents[i].Index != from+uint64(i) {
			return fmt.Errorf("%w: replacement index %d want %d", ErrBadRange, ents[i].Index, from+uint64(i))
		}
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	path := filepath.Join(s.dir, walName)
	tmp, err := os.OpenFile(path+".rewrite", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	buf := make([]byte, 0)
	for i := range keep {
		buf = append(buf, encodeRecord(recEntry, encodeEntry(keep[i]))...)
	}
	for i := range ents {
		buf = append(buf, encodeRecord(recEntry, encodeEntry(ents[i]))...)
	}
	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		return err
	}
	if err := maybeSync(tmp); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(path+".rewrite", path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.f = f
	merged := make([]raftproto.Entry, 0, len(keep)+len(ents))
	merged = append(merged, keep...)
	merged = append(merged, ents...)
	s.entries = merged
	return nil
}

// Compact drops entries with index <= snapIndex; a snapshot at snapIndex must
// already exist. Entries are never removed without durable snapshot cover, so
// a follower catching up never sees a hole: it receives InstallSnapshot.
func (s *Store) Compact(snapIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, _, err := s.snap.Load()
	if err != nil {
		return err
	}
	if m == nil || m.Index < snapIndex {
		return fmt.Errorf("%w: compaction index %d without snapshot cover", ErrBadRange, snapIndex)
	}
	if len(s.entries) == 0 {
		return nil
	}
	cut := 0
	for cut < len(s.entries) && s.entries[cut].Index <= snapIndex {
		cut++
	}
	if cut == 0 {
		return nil
	}
	keep := s.entries[cut:]
	if err := s.f.Close(); err != nil {
		return err
	}
	path := filepath.Join(s.dir, walName)
	if err := rewriteWAL(path, keep); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.f = f
	s.entries = keep
	return nil
}

func rewriteWAL(path string, ents []raftproto.Entry) error {
	tmp, err := os.OpenFile(path+".rewrite", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	buf := make([]byte, 0)
	for i := range ents {
		buf = append(buf, encodeRecord(recEntry, encodeEntry(ents[i]))...)
	}
	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		return err
	}
	if err := maybeSync(tmp); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(path+".rewrite", path)
}

// maybeSync preserves crash-safe fsync semantics for WAL rewrites. The local
// simulator disables fsync per-Store via SetSync; rewrite helpers are static so
// they keep the safe default (the rewrite path is rare and not hot-loop).
func maybeSync(f *os.File) error { return f.Sync() }
