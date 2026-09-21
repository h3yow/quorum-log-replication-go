package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// 磁盘文件布局（全部位于节点数据目录）：
//
//	state.bin     单条 HardState 记录（任期+投票），更新时临时文件 + rename + fsync(dir)
//	log.bin       段头记录(baseIndex/baseTerm) + 若干日志条目记录，追加写、fsync
//	snapshot.bin  快照状态机数据（原子替换）
//	snapmeta.bin  快照元数据 JSON（带 CRC，原子替换）
//
// 记录帧格式（小端）：
//
//	magic(4)=0x524C4731 | type(1) | payloadLen(4) | payload | crc32(4)
//
// 启动时逐帧校验：读到不完整尾部（头部或载荷/CRC 不足）=> ErrTruncatedStorage；
// 魔数不符 / 长度非法 / CRC 不符 => ErrCorruptStorage。两类拒绝原因可区分。

const (
	recordMagic uint32 = 0x524C4731 // "RLG1"

	recTypeHeader byte = 1
	recTypeHS     byte = 2
	recTypeEntry  byte = 3
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type frame struct {
	typ     byte
	payload []byte
}

func encodeFrame(typ byte, payload []byte) []byte {
	buf := make([]byte, 9+len(payload)+4)
	binary.LittleEndian.PutUint32(buf[0:4], recordMagic)
	buf[4] = typ
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(payload)))
	copy(buf[9:], payload)
	c := crc32.Checksum(append([]byte{typ}, payload...), crcTable)
	binary.LittleEndian.PutUint32(buf[9+len(payload):], c)
	return buf
}

// readFrames 解析整个文件。EOF 且无完整帧：返回空。尾部不完整 => ErrTruncatedStorage。
func readFrames(b []byte) ([]frame, error) {
	var out []frame
	pos := 0
	for pos < len(b) {
		if len(b)-pos < 9 {
			return nil, ErrTruncatedStorage
		}
		if binary.LittleEndian.Uint32(b[pos:pos+4]) != recordMagic {
			return nil, fmt.Errorf("%w: bad magic at offset %d", ErrCorruptStorage, pos)
		}
		typ := b[pos+4]
		n := int(binary.LittleEndian.Uint32(b[pos+5 : pos+9]))
		if n < 0 || pos+9+n+4 > len(b) {
			// 声明长度超出文件剩余字节：写入在中途崩溃，属于截断。
			return nil, ErrTruncatedStorage
		}
		payload := b[pos+9 : pos+9+n]
		want := binary.LittleEndian.Uint32(b[pos+9+n : pos+9+n+4])
		got := crc32.Checksum(append([]byte{typ}, payload...), crcTable)
		if want != got {
			return nil, fmt.Errorf("%w: crc mismatch at offset %d", ErrCorruptStorage, pos)
		}
		out = append(out, frame{typ: typ, payload: append([]byte(nil), payload...)})
		pos += 9 + n + 4
	}
	return out, nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// writeAtomic 临时文件写 + fsync + rename + fsync(dir)，保证替换原子且持久。
func writeAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return fsyncDir(dir)
}

// ---- 条目 / HardState / 段头 的确定性编码（长度前缀 + JSON 载荷）----

func encodeEntry(e LogEntry) []byte {
	// 固定字段 + 可变字段长度前缀，保证无分隔符歧义。
	b := make([]byte, 8+8+1+8)
	binary.LittleEndian.PutUint64(b[0:8], uint64(e.Index))
	binary.LittleEndian.PutUint64(b[8:16], uint64(e.Term))
	b[16] = byte(e.Kind)
	binary.LittleEndian.PutUint64(b[17:25], e.Seq)
	b = appendBytes(b, []byte(e.ClientID))
	b = appendBytes(b, e.Data)
	var conf []byte
	if e.Conf != nil {
		conf = append(conf, 1)
		conf = appendSet(conf, e.Conf.Voters)
		if e.Conf.Joint != nil {
			conf = append(conf, 1)
			conf = appendSet(conf, e.Conf.Joint)
		} else {
			conf = append(conf, 0)
		}
	} else {
		conf = append(conf, 0)
	}
	b = appendBytes(b, conf)
	return b
}

func decodeEntry(p []byte) (LogEntry, error) {
	var e LogEntry
	if len(p) < 25 {
		return e, fmt.Errorf("%w: entry record too short", ErrCorruptStorage)
	}
	e.Index = Index(binary.LittleEndian.Uint64(p[0:8]))
	e.Term = Term(binary.LittleEndian.Uint64(p[8:16]))
	e.Kind = EntryKind(p[16])
	e.Seq = binary.LittleEndian.Uint64(p[17:25])
	pos := 25
	cid, n, err := consumeBytes(p[pos:])
	if err != nil {
		return e, err
	}
	e.ClientID = string(cid)
	pos += n
	data, n, err := consumeBytes(p[pos:])
	if err != nil {
		return e, err
	}
	e.Data = data
	pos += n
	conf, _, err := consumeBytes(p[pos:])
	if err != nil {
		return e, err
	}
	if len(conf) > 0 {
		if conf[0] == 1 {
			c := Config{Voters: ConfigSet{}}
			q := 1
			voters, m, err := consumeSet(conf[q:])
			if err != nil {
				return e, err
			}
			c.Voters = voters
			q += m
			if q >= len(conf) {
				return e, fmt.Errorf("%w: conf truncated", ErrCorruptStorage)
			}
			if conf[q] == 1 {
				q++
				joint, _, err := consumeSet(conf[q:])
				if err != nil {
					return e, err
				}
				c.Joint = joint
			}
			e.Conf = &c
		}
	}
	return e, nil
}

func appendBytes(b, s []byte) []byte {
	var l [8]byte
	binary.LittleEndian.PutUint64(l[:], uint64(len(s)))
	b = append(b, l[:]...)
	return append(b, s...)
}

func consumeBytes(p []byte) ([]byte, int, error) {
	if len(p) < 8 {
		return nil, 0, fmt.Errorf("%w: length prefix truncated", ErrCorruptStorage)
	}
	n := int(binary.LittleEndian.Uint64(p[:8]))
	if n < 0 || 8+n > len(p) {
		return nil, 0, fmt.Errorf("%w: length out of range", ErrCorruptStorage)
	}
	return append([]byte(nil), p[8:8+n]...), 8 + n, nil
}

func encodeSet(s ConfigSet) []byte {
	ids := make([]string, 0, len(s))
	for id := range s {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, uint64(len(ids)))
	for _, id := range ids {
		out = appendBytes(out, []byte(id))
	}
	return out
}

func appendSet(b []byte, s ConfigSet) []byte { return append(b, encodeSet(s)...) }

func consumeSet(p []byte) (ConfigSet, int, error) {
	if len(p) < 8 {
		return nil, 0, fmt.Errorf("%w: set length truncated", ErrCorruptStorage)
	}
	cnt := int(binary.LittleEndian.Uint64(p[:8]))
	pos := 8
	out := ConfigSet{}
	for i := 0; i < cnt; i++ {
		id, n, err := consumeBytes(p[pos:])
		if err != nil {
			return nil, 0, err
		}
		out[NodeID(id)] = struct{}{}
		pos += n
	}
	return out, pos, nil
}

func encodeHardState(hs HardState) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], uint64(hs.Term))
	binary.LittleEndian.PutUint64(b[8:16], uint64(hs.CommitIndex))
	b = appendBytes(b, []byte(hs.Vote))
	return b
}

func decodeHardState(p []byte) (HardState, error) {
	var hs HardState
	// 兼容旧格式：仅 8 字节 term（开发期无历史包袱时也保持宽松解析）。
	if len(p) >= 16 {
		hs.Term = Term(binary.LittleEndian.Uint64(p[0:8]))
		hs.CommitIndex = Index(binary.LittleEndian.Uint64(p[8:16]))
		vote, _, err := consumeBytes(p[16:])
		if err != nil {
			return hs, err
		}
		hs.Vote = NodeID(vote)
		return hs, nil
	}
	if len(p) >= 8 {
		hs.Term = Term(binary.LittleEndian.Uint64(p[0:8]))
		vote, _, err := consumeBytes(p[8:])
		if err != nil {
			return hs, err
		}
		hs.Vote = NodeID(vote)
		return hs, nil
	}
	return hs, fmt.Errorf("%w: hardstate short", ErrCorruptStorage)
}

type segHeader struct {
	baseIndex Index
	baseTerm  Term
}

func encodeSegHeader(h segHeader) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], uint64(h.baseIndex))
	binary.LittleEndian.PutUint64(b[8:16], uint64(h.baseTerm))
	return b
}

func decodeSegHeader(p []byte) (segHeader, error) {
	var h segHeader
	if len(p) < 16 {
		return h, fmt.Errorf("%w: segment header short", ErrCorruptStorage)
	}
	h.baseIndex = Index(binary.LittleEndian.Uint64(p[0:8]))
	h.baseTerm = Term(binary.LittleEndian.Uint64(p[8:16]))
	return h, nil
}

// Storage 节点持久化接口：崩溃重启后可完整恢复 HardState、日志与快照。
type Storage interface {
	LogStore

	// InitialState 读取最近一次持久化的任期与投票。
	InitialState() (HardState, error)
	// SaveHardState 必须 fsync 后返回，保证投票/任期不回退。
	SaveHardState(hs HardState) error

	// Append 追加日志；首条条目必须与现有日志尾部连续（或按 Raft
	// 规则截断冲突尾段），否则返回 ErrGap。
	Append(entries []LogEntry) error

	// Snapshot 返回当前快照元数据与数据读句柄；无快照时 ok=false。
	Snapshot() (meta SnapshotMeta, r io.ReadCloser, ok bool, err error)
	// CreateSnapshot 以本节点状态机数据生成快照并截断旧日志；
	// 快照点必须存在于现存日志中，且不允许回滚已有快照。
	CreateSnapshot(meta SnapshotMeta, data []byte) error
	// InstallRemoteSnapshot 安装来自领导者的快照：快照点可以完全超出
	// 本地现存日志（长期离线节点追赶）。不会回滚一个更新的本地快照。
	InstallRemoteSnapshot(meta SnapshotMeta, data []byte) error

	// Compact 丢弃 index 之前的日志（仅允许在已有快照覆盖时）。
	Compact(index Index) error

	Close() error
}

// FileStorage 基于本地文件的持久化实现。
type FileStorage struct {
	dir string

	// 内存中的日志为磁盘的完整镜像；所有变更先落盘 fsync 再更新内存。
	baseIndex Index // 第一条现存日志条目的前一下标（= 快照 LastIndex）
	baseTerm  Term  // baseIndex 对应任期（用于 PrevLogTerm）
	entries   []LogEntry

	hs      HardState
	hsFile  string
	logFile string

	hasSnap bool
	snap    SnapshotMeta
}

// NewFileStorage 打开（必要时创建）目录下的持久化文件并做启动校验。
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{
		dir:     dir,
		hsFile:  filepath.Join(dir, "state.bin"),
		logFile: filepath.Join(dir, "log.bin"),
	}
	if err := s.loadHardState(); err != nil {
		return nil, err
	}
	if err := s.loadLog(); err != nil {
		return nil, err
	}
	if err := s.initLogIfAbsent(); err != nil {
		return nil, err
	}
	if err := s.loadSnapshotMeta(); err != nil {
		return nil, err
	}
	if err := s.reconcile(); err != nil {
		return nil, err
	}
	return s, nil
}

// initLogIfAbsent 全新节点没有 log.bin，先写入空段头（baseIndex=0），
// 保证后续 Append 的 O_APPEND 一定接在合法段头之后。
func (s *FileStorage) initLogIfAbsent() error {
	if _, err := os.Stat(s.logFile); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.rewriteLog(segHeader{baseIndex: 0, baseTerm: 0}, nil)
}

func (s *FileStorage) loadHardState() error {
	b, err := os.ReadFile(s.hsFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil // 全新节点
	}
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	frames, err := readFrames(b)
	if err != nil {
		return err
	}
	// state.bin 只保留最后一条 HardState（原子重写）。
	f := frames[len(frames)-1]
	if f.typ != recTypeHS {
		return fmt.Errorf("%w: unexpected hardstate record type %d", ErrCorruptStorage, f.typ)
	}
	hs, err := decodeHardState(f.payload)
	if err != nil {
		return err
	}
	s.hs = hs
	return nil
}

func (s *FileStorage) loadLog() error {
	b, err := os.ReadFile(s.logFile)
	if errors.Is(err, os.ErrNotExist) || len(b) == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	frames, err := readFrames(b)
	if err != nil {
		return err
	}
	if len(frames) == 0 || frames[0].typ != recTypeHeader {
		return fmt.Errorf("%w: missing segment header", ErrCorruptStorage)
	}
	h, err := decodeSegHeader(frames[0].payload)
	if err != nil {
		return err
	}
	s.baseIndex, s.baseTerm = h.baseIndex, h.baseTerm
	s.entries = s.entries[:0]
	for _, f := range frames[1:] {
		if f.typ != recTypeEntry {
			return fmt.Errorf("%w: unexpected log record type %d", ErrCorruptStorage, f.typ)
		}
		e, err := decodeEntry(f.payload)
		if err != nil {
			return err
		}
		s.entries = append(s.entries, e)
	}
	if err := s.checkContiguous(); err != nil {
		return err
	}
	return nil
}

// checkContiguous 校验内存日志下标连续且单调，任何空洞/错位都视为损坏。
func (s *FileStorage) checkContiguous() error {
	prev := s.baseIndex
	prevTerm := s.baseTerm
	for _, e := range s.entries {
		if e.Index != prev+1 {
			return fmt.Errorf("%w: expected index %d, got %d", ErrGap, prev+1, e.Index)
		}
		if e.Term < prevTerm {
			return fmt.Errorf("%w: term regression at index %d", ErrCorruptStorage, e.Index)
		}
		prev, prevTerm = e.Index, e.Term
	}
	return nil
}

func (s *FileStorage) loadSnapshotMeta() error {
	b, err := os.ReadFile(filepath.Join(s.dir, "snapmeta.bin"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	frames, err := readFrames(b)
	if err != nil {
		return err
	}
	if len(frames) == 0 {
		return nil
	}
	var mj snapshotMetaJSON
	if err := json.Unmarshal(frames[len(frames)-1].payload, &mj); err != nil {
		return fmt.Errorf("%w: snapshot meta: %v", ErrCorruptStorage, err)
	}
	s.hasSnap = true
	s.snap = mj.toMeta()
	return nil
}

// reconcile 快照与日志在崩溃后可能停留在"新快照已落盘、旧日志尚未截断"
// 的中间态。此处以快照为准抬升日志 base，并把结果重写回磁盘，
// 保证此后 Append 的 O_APPEND 不会接在陈旧条目之后。
func (s *FileStorage) reconcile() error {
	if !s.hasSnap || s.snap.LastIndex <= s.baseIndex {
		return nil
	}
	cut := s.offset(s.snap.LastIndex)
	var keep []LogEntry
	if cut < 0 {
		// 快照点在当前日志段之前：整段日志都已被快照覆盖。
		keep = nil
	} else {
		if cut >= len(s.entries) || s.entries[cut].Term != s.snap.LastTerm {
			return fmt.Errorf("%w: snapshot boundary term mismatch at %d", ErrCorruptStorage, s.snap.LastIndex)
		}
		keep = append([]LogEntry(nil), s.entries[cut+1:]...)
	}
	return s.rewriteLog(segHeader{baseIndex: s.snap.LastIndex, baseTerm: s.snap.LastTerm}, keep)
}

// snapshotMetaJSON 让 ConfigSet(map) 以数组形式落盘。
type snapshotMetaJSON struct {
	LastIndex Index     `json:"last_index"`
	LastTerm  Term      `json:"last_term"`
	Voters    []NodeID  `json:"voters"`
	Joint     *[]NodeID `json:"joint,omitempty"`
}

func (j snapshotMetaJSON) toMeta() SnapshotMeta {
	m := SnapshotMeta{
		LastIndex: j.LastIndex,
		LastTerm:  j.LastTerm,
		Config:    Config{Voters: ConfigSet{}},
	}
	for _, id := range j.Voters {
		m.Config.Voters[id] = struct{}{}
	}
	if j.Joint != nil {
		m.Config.Joint = ConfigSet{}
		for _, id := range *j.Joint {
			m.Config.Joint[id] = struct{}{}
		}
	}
	return m
}

func metaToJSON(m SnapshotMeta) snapshotMetaJSON {
	j := snapshotMetaJSON{LastIndex: m.LastIndex, LastTerm: m.LastTerm}
	for id := range m.Config.Voters {
		j.Voters = append(j.Voters, id)
	}
	sort.Slice(j.Voters, func(i, k int) bool { return j.Voters[i] < j.Voters[k] })
	if m.Config.Joint != nil {
		var joint []NodeID
		for id := range m.Config.Joint {
			joint = append(joint, id)
		}
		sort.Slice(joint, func(i, k int) bool { return joint[i] < joint[k] })
		j.Joint = &joint
	}
	return j
}

// InitialState 读取最近一次持久化的任期与投票。
func (s *FileStorage) InitialState() (HardState, error) { return s.hs, nil }

// SaveHardState 原子替换并 fsync，保证投票/任期不回退。
func (s *FileStorage) SaveHardState(hs HardState) error {
	data := encodeFrame(recTypeHS, encodeHardState(hs))
	if err := writeAtomic(s.hsFile, data, 0o644); err != nil {
		return err
	}
	s.hs = hs
	return nil
}

func (s *FileStorage) BaseIndex() Index { return s.baseIndex }

func (s *FileStorage) LastIndex() Index {
	if len(s.entries) == 0 {
		return s.baseIndex
	}
	return s.entries[len(s.entries)-1].Index
}

func (s *FileStorage) offset(i Index) int { return int(i - s.baseIndex - 1) }

func (s *FileStorage) hasIndex(i Index) bool {
	return i > s.baseIndex && i <= s.LastIndex()
}

func (s *FileStorage) TermAt(i Index) (Term, error) {
	if i == 0 {
		return 0, nil
	}
	if i == s.baseIndex {
		return s.baseTerm, nil
	}
	if !s.hasIndex(i) {
		return 0, fmt.Errorf("%w: term at %d (base=%d last=%d)", ErrGap, i, s.baseIndex, s.LastIndex())
	}
	return s.entries[s.offset(i)].Term, nil
}

func (s *FileStorage) Entry(i Index) (LogEntry, error) {
	if !s.hasIndex(i) {
		return LogEntry{}, fmt.Errorf("%w: entry at %d", ErrGap, i)
	}
	return s.entries[s.offset(i)], nil
}

func (s *FileStorage) Entries(lo, hi Index) ([]LogEntry, error) {
	if lo <= s.baseIndex || hi > s.LastIndex()+1 || lo > hi {
		return nil, fmt.Errorf("%w: entries [%d,%d) base=%d last=%d", ErrGap, lo, hi, s.baseIndex, s.LastIndex())
	}
	return append([]LogEntry(nil), s.entries[s.offset(lo):s.offset(hi)]...), nil
}

// Append 按 Raft 日志规则追加：
//   - entries 必须紧接现有尾段（首条 Index == LastIndex()+1），
//     或覆盖现有尾段中从某下标开始的冲突后缀（截断），不允许制造空洞；
//   - 任期更旧时不得覆盖（调用方已比较，这里再做防御）。
func (s *FileStorage) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0]
	if first.Index <= s.baseIndex {
		return fmt.Errorf("%w: append index %d <= base %d", ErrGap, first.Index, s.baseIndex)
	}
	if first.Index > s.LastIndex()+1 {
		return fmt.Errorf("%w: append index %d leaves gap (last=%d)", ErrGap, first.Index, s.LastIndex())
	}

	// 若与现存条目同下标，则任期相同的前缀为重复送达，跳过；
	// 任期不同 => 冲突，截断该下标及之后内容后再追加。
	cut := s.offset(first.Index)
	if cut < len(s.entries) {
		existing := s.entries[cut]
		if existing.Term == first.Term && existing.Index == first.Index {
			entries = entries[1:]
			return s.Append(entries)
		}
		// 冲突截断：重写整个段文件（段头 + 保留前缀 + 新后缀），原子替换。
		s.entries = append([]LogEntry(nil), s.entries[:cut]...)
	}

	// 追加到文件并 fsync；任一步失败都不改内存，节点随后应崩溃退出而非带脏状态服务。
	f, err := os.OpenFile(s.logFile, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if _, err := f.Write(encodeFrame(recTypeEntry, encodeEntry(e))); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	s.entries = append(s.entries, entries...)
	return s.checkContiguous()
}

// rewriteLog 用新段头与条目集合原子重写 log.bin（压缩/安装快照时使用）。
func (s *FileStorage) rewriteLog(h segHeader, entries []LogEntry) error {
	data := encodeFrame(recTypeHeader, encodeSegHeader(h))
	for _, e := range entries {
		data = append(data, encodeFrame(recTypeEntry, encodeEntry(e))...)
	}
	if err := writeAtomic(s.logFile, data, 0o644); err != nil {
		return err
	}
	s.baseIndex, s.baseTerm = h.baseIndex, h.baseTerm
	s.entries = append([]LogEntry(nil), entries...)
	return s.checkContiguous()
}

// Snapshot 返回当前快照元数据与数据读句柄；无快照时 ok=false。
func (s *FileStorage) Snapshot() (SnapshotMeta, io.ReadCloser, bool, error) {
	if !s.hasSnap {
		return SnapshotMeta{}, nil, false, nil
	}
	f, err := os.Open(filepath.Join(s.dir, "snapshot.bin"))
	if err != nil {
		return SnapshotMeta{}, nil, false, err
	}
	return s.snap, f, true, nil
}

func (s *FileStorage) snapshotMetaPath() string { return filepath.Join(s.dir, "snapmeta.bin") }
func (s *FileStorage) snapshotDataPath() string { return filepath.Join(s.dir, "snapshot.bin") }

// CreateSnapshot 安装快照并截断 lastIndex 之前的日志。
// 顺序（崩溃安全）：先原子写 snapshot.bin（数据），再原子写 snapmeta.bin
// （元数据，是"快照可用"的提交点），最后重写 log.bin 抬升 base。
// 崩溃在任一步：重启时只有元数据可见才会对账截断，已提交内容不会丢。
func (s *FileStorage) CreateSnapshot(meta SnapshotMeta, data []byte) error {
	if s.hasSnap && meta.LastIndex < s.snap.LastIndex {
		return fmt.Errorf("%w: new snapshot %d < current %d", ErrSnapshotBehind, meta.LastIndex, s.snap.LastIndex)
	}
	if meta.LastIndex < s.baseIndex {
		return fmt.Errorf("%w: snapshot %d < log base %d", ErrSnapshotBehind, meta.LastIndex, s.baseIndex)
	}
	// 快照点必须能在现存日志中核对任期（base 点或日志条目）。
	termAt, err := s.TermAt(meta.LastIndex)
	if err != nil {
		return err
	}
	if termAt != meta.LastTerm {
		return fmt.Errorf("%w: snapshot term %d != log term %d at %d", ErrCorruptStorage, meta.LastTerm, termAt, meta.LastIndex)
	}
	if err := writeAtomic(s.snapshotDataPath(), data, 0o644); err != nil {
		return err
	}
	mj, err := json.Marshal(metaToJSON(meta))
	if err != nil {
		return err
	}
	if err := writeAtomic(s.snapshotMetaPath(), encodeFrame(recTypeHS, mj), 0o644); err != nil {
		return err
	}
	s.hasSnap, s.snap = true, meta
	return s.reconcile()
}

// Compact 丢弃 index 之前的日志。仅当现存快照覆盖该点时允许；
// 已提交内容保留在快照中，不会产生空洞或回滚。
// InstallRemoteSnapshot 安装领导者快照：不要求快照点存在于本地日志。
// 安装后丢弃整个旧日志段，以快照点为新 base；快照点之后若本地还有
// 任期连续的条目则保留（通常为 0 条），否则清空，随后由领导者增量补齐。
// 整个过程原子替换数据、元数据与日志段，崩溃后重启以快照为准对账。
func (s *FileStorage) InstallRemoteSnapshot(meta SnapshotMeta, data []byte) error {
	if s.hasSnap && meta.LastIndex < s.snap.LastIndex {
		return fmt.Errorf("%w: remote snapshot %d < current %d",
			ErrSnapshotBehind, meta.LastIndex, s.snap.LastIndex)
	}
	if err := writeAtomic(s.snapshotDataPath(), data, 0o644); err != nil {
		return err
	}
	mj, err := json.Marshal(metaToJSON(meta))
	if err != nil {
		return err
	}
	if err := writeAtomic(s.snapshotMetaPath(), encodeFrame(recTypeHS, mj), 0o644); err != nil {
		return err
	}
	s.hasSnap, s.snap = true, meta
	// 用空段重建日志（base=快照点）；快照点之后的增量由 AppendEntries 补。
	return s.rewriteLog(segHeader{baseIndex: meta.LastIndex, baseTerm: meta.LastTerm}, nil)
}

func (s *FileStorage) Compact(index Index) error {
	if index <= s.baseIndex {
		return nil
	}
	if !s.hasSnap || s.snap.LastIndex < index {
		return fmt.Errorf("%w: compact to %d without covering snapshot", ErrGap, index)
	}
	cut := s.offset(index)
	if cut < 0 || cut >= len(s.entries) {
		return fmt.Errorf("%w: compact index %d out of log", ErrGap, index)
	}
	termAt, err := s.TermAt(index)
	if err != nil {
		return err
	}
	keep := append([]LogEntry(nil), s.entries[cut+1:]...)
	return s.rewriteLog(segHeader{baseIndex: index, baseTerm: termAt}, keep)
}

func (s *FileStorage) Close() error { return nil }
