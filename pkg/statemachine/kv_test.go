package statemachine

import "testing"

func TestDedupAppliesOnce(t *testing.T) {
	k := NewKVStore()
	cmd := func(seq uint64) []byte {
		b, _ := Command{Op: CmdPut, Key: "x", Value: "v", ClientID: "c1", Seq: seq}.Encode()
		return b
	}
	r1, _ := k.Apply(cmd(1))
	if r1.Dup {
		t.Fatal("first apply must not be duplicate")
	}
	// Same (client,seq) retried/out-of-order: no second mutation, flagged Dup.
	r2, _ := k.Apply(cmd(1))
	if !r2.Dup {
		t.Fatal("repeat of same seq must be Dup and a no-op")
	}
	if got, _ := k.Get("x"); got != "v" {
		t.Fatalf("x=%q want v", got)
	}
	// Stale seq must not roll state back.
	stale, _ := Command{Op: CmdPut, Key: "x", Value: "OLD", ClientID: "c1", Seq: 1}.Encode()
	k.Apply(stale)
	if got, _ := k.Get("x"); got != "v" {
		t.Fatalf("stale retry rolled state back: %q", got)
	}
	// Next seq mutates.
	r3, _ := k.Apply(cmd(2))
	if r3.Dup {
		t.Fatal("higher seq must apply")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	k := NewKVStore()
	b, _ := Command{Op: CmdPut, Key: "a", Value: "1", ClientID: "z", Seq: 1}.Encode()
	k.Apply(b)
	snap, err := k.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	k2 := NewKVStore()
	if err := k2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if v, ok := k2.Get("a"); !ok || v != "1" {
		t.Fatalf("restored store missing a=1: %q %v", v, ok)
	}
}
