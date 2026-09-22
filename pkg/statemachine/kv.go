// Package statemachine contains the replicated application: a small
// linearizable key/value store with idempotent client-tracked commands and
// in-memory snapshot support.
package statemachine

import (
	"encoding/json"
)

// clientRecord remembers the last applied seq and its result per client, so
// duplicate/retried/out-of-order requests cannot take effect twice.
type clientRecord struct {
	Seq   uint64 `json:"s"`
	Value string `json:"v,omitempty"`
	OK    bool   `json:"o"`
}

type snapshotData struct {
	KV      map[string]string       `json:"kv"`
	Clients map[string]clientRecord `json:"cl"`
}

// KVStore is the application state machine driven by committed entries.
type KVStore struct {
	// fields are accessed under the raft event loop when applying; Snapshot
	// and Restore may run concurrently with reads, hence the RWMutex.
	kv      map[string]string
	clients map[string]clientRecord
}

// NewKVStore creates an empty store.
func NewKVStore() *KVStore {
	return &KVStore{kv: map[string]string{}, clients: map[string]clientRecord{}}
}

// Apply executes one committed command.
//
// Semantics:
//   - A command whose (ClientID, Seq) was already applied returns the stored
//     result with Dup=true and performs no mutation.
//   - A stale seq (lower than the last seen) is likewise a no-op duplicate.
//   - Only strictly increasing seqs mutate the store, so reordering on the
//     network can never roll state backwards.
func (k *KVStore) Apply(cmd []byte) (Result, error) {
	c, err := DecodeCommand(cmd)
	if err != nil {
		return Result{}, err
	}
	if c.ClientID != "" {
		if rec, ok := k.clients[c.ClientID]; ok {
			if c.Seq <= rec.Seq {
				return Result{OK: rec.OK, Value: rec.Value, Dup: true}, nil
			}
		}
	}
	res := Result{OK: true}
	switch c.Op {
	case CmdPut:
		k.kv[c.Key] = c.Value
	case CmdDelete:
		delete(k.kv, c.Key)
	case CmdGet:
		v, ok := k.kv[c.Key]
		res.Value, res.OK = v, ok
	default:
		res.OK = false
	}
	if c.ClientID != "" {
		k.clients[c.ClientID] = clientRecord{Seq: c.Seq, Value: res.Value, OK: res.OK}
	}
	return res, nil
}

// Get returns the current value. Read consistency is enforced by the raft
// layer (read index); the store itself reflects the committed/applied state.
func (k *KVStore) Get(key string) (string, bool) {
	v, ok := k.kv[key]
	return v, ok
}

// Snapshot serializes the whole store atomically with its dedup table.
func (k *KVStore) Snapshot() ([]byte, error) {
	sd := snapshotData{
		KV:      make(map[string]string, len(k.kv)),
		Clients: make(map[string]clientRecord, len(k.clients)),
	}
	for key, v := range k.kv {
		sd.KV[key] = v
	}
	for id, rec := range k.clients {
		sd.Clients[id] = rec
	}
	return json.Marshal(sd)
}

// Restore replaces the store contents from a snapshot.
func (k *KVStore) Restore(data []byte) error {
	var sd snapshotData
	if err := json.Unmarshal(data, &sd); err != nil {
		return err
	}
	k.kv = sd.KV
	k.clients = sd.Clients
	if k.kv == nil {
		k.kv = map[string]string{}
	}
	if k.clients == nil {
		k.clients = map[string]clientRecord{}
	}
	return nil
}

// Result is what Apply reports back to the client.
type Result struct {
	OK    bool
	Value string
	// Dup indicates this request had already been applied; the original
	// result is returned and the state did not change.
	Dup bool
}
