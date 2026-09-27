package statemachine

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

// ContentDigest is a deterministic hex SHA-256 over the exact committed
// key/value contents of the store (keys sorted). Two nodes that converged to
// the same confirmed history always produce the same digest regardless of
// timing; the client de-duplication table is intentionally excluded because it
// records request metadata rather than confirmed history.
//
// This is a read-only observation: it must never be invoked while the owning
// raft event loop mutates the map. Callers reach it through the raft status
// cache (populated on the loop) or after a node has stopped.
func (k *KVStore) ContentDigest() string {
	keys := make([]string, 0, len(k.kv))
	for key := range k.kv {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		h.Write([]byte(key))
		h.Write([]byte{0})
		h.Write([]byte(k.kv[key]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// SnapshotKV returns a copy of the applied key/value map. Callers must ensure
// the owning event loop is not concurrently mutating the store (post-stop or
// quiesced verification only).
func (k *KVStore) SnapshotKV() (map[string]string, bool) {
	out := make(map[string]string, len(k.kv))
	for key, v := range k.kv {
		out[key] = v
	}
	return out, true
}
