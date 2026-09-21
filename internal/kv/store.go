// Package kv 实现一个被复制的键值状态机：
// 确定性 Apply、JSON 快照/恢复，以及 (ClientID,Seq) 幂等去重。
package kv

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/h3yow/quorum-log-replication-go/internal/raft"
)

// Command 客户端写命令的 JSON 编码（Normal 条目的 Data）。
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

const (
	OpPut    = "put"
	OpDelete = "delete"
)

// Store 键值状态机。该类型不自带锁：它只由所属节点的单一事件循环
// （或恢复路径）串行调用；对外读也经由 ReadIndex 后在同一线程取值。
type Store struct {
	mu      sync.RWMutex
	applied raft.Index
	data    map[string]string
	// lastSeq 记录每个客户端最近一次已生效的序列号；
	// 快照中持久化，重启后重复请求仍不会二次生效。
	lastSeq map[string]uint64
}

// New 创建空状态机。
func New() *Store {
	return &Store{data: map[string]string{}, lastSeq: map[string]uint64{}}
}

// snapshotData 状态机快照的 JSON 形态。
type snapshotData struct {
	Applied raft.Index        `json:"applied"`
	Data    map[string]string `json:"data"`
	LastSeq map[string]uint64 `json:"last_seq"`
}

// Apply 确定性应用一条日志条目。
// 去重规则：同一 ClientID 下 Seq 必须单调；Seq <= 已见最大 Seq 视为
// 重复/过期请求，返回 duplicated=true 且不改变状态；乱序到达的更大 Seq
// 也不允许跳过空洞（防御性拒绝），保证重放与判定依据一致。
func (s *Store) Apply(e raft.LogEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.Kind != raft.EntryNormal {
		// 配置条目 / 非投票预告条目不进入键值状态机，但推进 applied。
		if e.Index > s.applied {
			s.applied = e.Index
		}
		return false, nil
	}
	if e.Index != s.applied+1 {
		return false, fmt.Errorf("kv: non-contiguous apply index %d, expected %d", e.Index, s.applied+1)
	}

	if e.ClientID != "" {
		if last, ok := s.lastSeq[e.ClientID]; ok && e.Seq <= last {
			s.applied = e.Index
			return true, nil // 重复/重试：不重复生效
		}
	}

	var cmd Command
	if len(e.Data) == 0 {
		// 领导者 no-op 空条目：确定性空操作，仅推进 applied。
		if e.ClientID != "" {
			s.lastSeq[e.ClientID] = e.Seq
		}
		s.applied = e.Index
		return false, nil
	}
	if err := json.Unmarshal(e.Data, &cmd); err != nil {
		// 命令解析失败仍要推进 applied：日志已提交，重放必须得到相同结果。
		s.applied = e.Index
		return false, fmt.Errorf("kv: bad command at index %d: %w", e.Index, err)
	}
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	default:
		// 未知操作按空操作确定性处理，并推进下标。
	}
	if e.ClientID != "" {
		s.lastSeq[e.ClientID] = e.Seq
	}
	s.applied = e.Index
	return false, nil
}

// Read 返回键的当前值；不存在时 ok=false。调用方必须在
// ReadIndex + 等待应用到该位置之后调用，保证读一致性。
func (s *Store) Read(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Data 返回当前全量数据的副本（观测/测试用）。
func (s *Store) Data() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Snapshot 导出 JSON 快照（含去重表与 applied 下标）。
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sd := snapshotData{
		Applied: s.applied,
		Data:    make(map[string]string, len(s.data)),
		LastSeq: make(map[string]uint64, len(s.lastSeq)),
	}
	for k, v := range s.data {
		sd.Data[k] = v
	}
	for k, v := range s.lastSeq {
		sd.LastSeq[k] = v
	}
	return json.Marshal(sd)
}

// Restore 从 JSON 快照恢复；恢复后仍可继续重放快照之后的日志。
func (s *Store) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) == 0 {
		s.applied, s.data, s.lastSeq = 0, map[string]string{}, map[string]uint64{}
		return nil
	}
	var sd snapshotData
	if err := json.Unmarshal(data, &sd); err != nil {
		return fmt.Errorf("kv: restore snapshot: %w", err)
	}
	if sd.Data == nil {
		sd.Data = map[string]string{}
	}
	if sd.LastSeq == nil {
		sd.LastSeq = map[string]uint64{}
	}
	s.applied, s.data, s.lastSeq = sd.Applied, sd.Data, sd.LastSeq
	return nil
}

// AppliedIndex 返回最近一次 Apply 的日志下标。
func (s *Store) AppliedIndex() raft.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}
