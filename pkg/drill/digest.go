package drill

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// reportSchema is bumped when the report body shape changes.
const reportSchema = "qlog-drill/v1"

// canonicalDigest returns the hex SHA-256 of the canonical JSON encoding of
// the deterministic fact body. encoding/json already emits struct fields in
// declaration order and sorts map keys, and the drill fills every slice in a
// fixed order, so identical historical facts always hash identically.
// Timestamps and durations are never part of this body.
func (f Fact) canonicalDigest() (string, error) {
	b, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
