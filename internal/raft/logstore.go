package raft

// LogStore 是纯追加日志的只读/追加接口（持久化实现见 FileStorage）。
// 所有下标连续：BaseIndex()..LastIndex()，压缩时整体抬升 BaseIndex。
type LogStore interface {
	BaseIndex() Index
	LastIndex() Index
	TermAt(i Index) (Term, error)
	Entry(i Index) (LogEntry, error)
	Entries(lo, hi Index) ([]LogEntry, error)
}
