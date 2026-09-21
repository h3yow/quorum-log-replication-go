package raft

import "time"

// SingleConfig 构造一份普通（非联合）配置。
func SingleConfig(ids ...NodeID) Config {
	c := Config{Voters: ConfigSet{}}
	for _, id := range ids {
		c.Voters[id] = struct{}{}
	}
	return c
}

func setEqual(a, b ConfigSet) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if _, ok := b[id]; !ok {
			return false
		}
	}
	return true
}

// EqualConfig 比较两份配置：主集合相同且联合状态（Joint 是否存在、内容）相同。
func EqualConfig(a, b Config) bool {
	if !setEqual(a.Voters, b.Voters) {
		return false
	}
	if (a.Joint == nil) != (b.Joint == nil) {
		return false
	}
	if a.Joint != nil && !setEqual(a.Joint, b.Joint) {
		return false
	}
	return true
}

func quorum(size int) int { return size/2 + 1 }

// setHasQuorum 判断 alive 是否构成 s 的多数派。
func setHasQuorum(s ConfigSet, alive ConfigSet) bool {
	if len(s) == 0 {
		return false
	}
	votes := 0
	for id := range s {
		if _, ok := alive[id]; ok {
			votes++
		}
	}
	return votes >= quorum(len(s))
}

// HasQuorum 判断给定存活集合是否构成配置下的法定多数。
// 联合配置（Joint != nil）要求 C_old 与 C_new 同时满足多数。
func (c Config) HasQuorum(alive ConfigSet) bool {
	if !setHasQuorum(c.Voters, alive) {
		return false
	}
	if c.Joint != nil && !setHasQuorum(c.Joint, alive) {
		return false
	}
	return true
}

// Len 返回主投票集合大小。
func (c Config) Len() int { return len(c.Voters) }

// JointLen 返回新配置大小；非联合配置返回 0。
func (c Config) JointLen() int {
	if c.Joint == nil {
		return 0
	}
	return len(c.Joint)
}

// Contains 判断节点是否为当前配置下的投票成员。
// 联合期间属于 C_old 或 C_new 任一即算（与复制目标集合一致）。
func (c Config) Contains(id NodeID) bool {
	if _, ok := c.Voters[id]; ok {
		return true
	}
	if c.Joint != nil {
		if _, ok := c.Joint[id]; ok {
			return true
		}
	}
	return false
}

func cloneSet(s ConfigSet) ConfigSet {
	if s == nil {
		return nil
	}
	out := ConfigSet{}
	for id := range s {
		out[id] = struct{}{}
	}
	return out
}

// Clone 返回深拷贝，避免外部修改内部配置。
func (c Config) Clone() Config {
	out := Config{Voters: cloneSet(c.Voters)}
	if c.Joint != nil {
		out.Joint = cloneSet(c.Joint)
	}
	return out
}

// ValidateChange 校验成员变更目标配置：
//   - 新配置不能为空（空配置无法达成多数）；
//   - 旧配置非空时，新旧配置必须有交集——禁止单步替换全部投票成员
//     （Raft 要求通过联合共识逐步过渡，避免两套多数派并存）。
//
// 注意：移除领导者自身在语义上允许，配置提交后领导者自动降级为跟随者。
func ValidateChange(oldVoters, newVoters ConfigSet) error {
	if len(newVoters) == 0 {
		return ErrInvalidConfig
	}
	if len(oldVoters) == 0 {
		return nil
	}
	for id := range newVoters {
		if _, ok := oldVoters[id]; ok {
			return nil // 存在交集，变更合法
		}
	}
	return ErrInvalidConfig
}

// DefaultOptions 返回适合本地模拟的默认时序参数。
// 上层以 10ms 为一个 Tick 驱动时：选举超时 100~200ms，心跳 20ms。
func DefaultOptions() Options {
	return Options{
		ElectionMinTicks:  10,
		ElectionMaxTicks:  20,
		HeartbeatTicks:    2,
		SnapshotThreshold: 100,
		RPCTimeout:        100 * time.Millisecond,
	}
}
