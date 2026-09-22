package logstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/h3yow/quorum-log-replication-go/pkg/raftproto"
)

const snapName = "snapshot.bin"

// snapshot file layout:
//
//	index:8 term:8 votersLen:4 voters... learnersLen:4 learners...
//	jointLen:4 joint... dataLen:4 data... crc32:4
//
// Only one snapshot exists at a time; Save is atomic (tmp + rename + fsync).

// SnapshotStore persists application snapshots next to the WAL.
type SnapshotStore struct {
	dir string

	mu     sync.Mutex
	syncOn bool
}

// NewSnapshotStore creates an empty snapshot store rooted at dir.
func NewSnapshotStore(dir string) (*SnapshotStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &SnapshotStore{dir: dir, syncOn: true}, nil
}

func putStrList(b []byte, xs []string) []byte {
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(xs)))
	b = append(b, lb[:]...)
	for _, x := range xs {
		binary.LittleEndian.PutUint32(lb[:], uint32(len(x)))
		b = append(b, lb[:]...)
		b = append(b, x...)
	}
	return b
}

func getStrList(b []byte, off int) ([]string, int, error) {
	if len(b) < off+4 {
		return nil, 0, ErrCorruptSnapshot
	}
	n := int(binary.LittleEndian.Uint32(b[off : off+4]))
	off += 4
	xs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if len(b) < off+4 {
			return nil, 0, ErrCorruptSnapshot
		}
		l := int(binary.LittleEndian.Uint32(b[off : off+4]))
		off += 4
		if len(b) < off+l {
			return nil, 0, ErrCorruptSnapshot
		}
		xs = append(xs, string(b[off:off+l]))
		off += l
	}
	return xs, off, nil
}

func encodeSnapshot(meta raftproto.SnapshotMeta, data []byte) []byte {
	b := make([]byte, 0, 32+len(data))
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], meta.Index)
	b = append(b, tmp[:]...)
	binary.LittleEndian.PutUint64(tmp[:], meta.Term)
	b = append(b, tmp[:]...)
	b = putStrList(b, meta.Config.Voters)
	b = putStrList(b, meta.Config.Learners)
	b = putStrList(b, meta.Config.JointVoters)
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(data)))
	b = append(b, lb[:]...)
	b = append(b, data...)
	c := crc32.Checksum(b, crcTable)
	var cb [4]byte
	binary.LittleEndian.PutUint32(cb[:], c)
	b = append(b, cb[:]...)
	return b
}

func decodeSnapshot(b []byte) (raftproto.SnapshotMeta, []byte, error) {
	var meta raftproto.SnapshotMeta
	if len(b) < 16+4 {
		return meta, nil, ErrCorruptSnapshot
	}
	if crc32.Checksum(b[:len(b)-4], crcTable) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return meta, nil, ErrCorruptSnapshot
	}
	body := b[:len(b)-4]
	off := 0
	meta.Index = binary.LittleEndian.Uint64(body[0:8])
	meta.Term = binary.LittleEndian.Uint64(body[8:16])
	off = 16
	var err error
	if meta.Config.Voters, off, err = getStrList(body, off); err != nil {
		return meta, nil, err
	}
	if meta.Config.Learners, off, err = getStrList(body, off); err != nil {
		return meta, nil, err
	}
	if meta.Config.JointVoters, off, err = getStrList(body, off); err != nil {
		return meta, nil, err
	}
	if len(body) < off+4 {
		return meta, nil, ErrCorruptSnapshot
	}
	dl := int(binary.LittleEndian.Uint32(body[off : off+4]))
	off += 4
	if len(body) < off+dl {
		return meta, nil, ErrCorruptSnapshot
	}
	data := make([]byte, dl)
	copy(data, body[off:off+dl])
	return meta, data, nil
}

// SetSync toggles fsync for snapshot writes (defaults to true).
func (s *SnapshotStore) SetSync(on bool) {
	s.mu.Lock()
	s.syncOn = on
	s.mu.Unlock()
}

// Save atomically persists a snapshot (meta + opaque application data).
func (s *SnapshotStore) Save(meta raftproto.SnapshotMeta, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Refuse to overwrite with an older snapshot: committed state must never
	// roll back.
	if cur, _, err := s.loadLocked(); err == nil && cur != nil && cur.Index > meta.Index {
		return ErrSnapOutOfDate
	}
	return writeFileAtomicSync(filepath.Join(s.dir, snapName), encodeSnapshot(meta, data), s.syncOn)
}

// Load reads the newest snapshot; it returns (nil, nil, nil) when none exists.
func (s *SnapshotStore) Load() (*raftproto.SnapshotMeta, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

func (s *SnapshotStore) loadLocked() (*raftproto.SnapshotMeta, []byte, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, snapName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	meta, data, err := decodeSnapshot(b)
	if err != nil {
		return nil, nil, err
	}
	return &meta, data, nil
}
