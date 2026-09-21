package kv

import (
	"testing"

	"github.com/h3yow/quorum-log-replication-go/internal/raft"
)

func cmd(op, k, v string) []byte {
	return []byte(`{"op":"` + op + `","key":"` + k + `","value":"` + v + `"}`)
}

func TestApplyIdempotentAndReplay(t *testing.T) {
	s := New()
	mk := func(i uint64, cid string, seq uint64, data []byte) raft.LogEntry {
		return raft.LogEntry{Index: raft.Index(i), Term: 1, Kind: raft.EntryNormal, ClientID: cid, Seq: seq, Data: data}
	}
	if _, err := s.Apply(mk(1, "a", 1, cmd(OpPut, "k", "v1"))); err != nil {
		t.Fatal(err)
	}
	dup, err := s.Apply(mk(2, "a", 1, cmd(OpPut, "k", "v1")))
	if err != nil || !dup {
		t.Fatalf("重复请求应标记 duplicated，got dup=%v err=%v", dup, err)
	}
	// 旧序列号乱序到达：不生效。
	if dup, _ := s.Apply(mk(3, "a", 0, cmd(OpPut, "k", "stale"))); !dup {
		t.Fatal("旧序列号应被当作重复忽略")
	}
	if _, err := s.Apply(mk(4, "b", 1, cmd(OpPut, "k", "v2"))); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Read("k"); !ok || v != "v2" {
		t.Fatalf("最终值异常 %q/%v", v, ok)
	}
	// 快照/恢复后去重表仍生效。
	b, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s2 := New()
	if err := s2.Restore(b); err != nil {
		t.Fatal(err)
	}
	if dup, _ := s2.Apply(mk(5, "a", 1, cmd(OpPut, "k", "v1"))); !dup {
		t.Fatal("快照恢复后旧请求不应二次生效")
	}
	if v, _ := s2.Read("k"); v != "v2" {
		t.Fatalf("恢复后状态错误 %q", v)
	}
	// 恢复点 applied=4；重复条目按日志位置推进到 5，但命令不二次生效。
	if s2.AppliedIndex() != 5 {
		t.Fatalf("恢复后 applied 推进异常: %d", s2.AppliedIndex())
	}
}
