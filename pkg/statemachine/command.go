package statemachine

import "encoding/json"

// Command kinds encoded inside an EntryNormal payload.
const (
	CmdPut    = "put"
	CmdDelete = "del"
	CmdGet    = "get"
)

// Command is the wire form of a client operation. The (ClientID, Seq) pair
// makes retries idempotent: applying the same pair twice does not mutate the
// store twice and returns the original recorded result.
type Command struct {
	Op       string `json:"op"`
	Key      string `json:"k"`
	Value    string `json:"v,omitempty"`
	ClientID string `json:"cid"`
	Seq      uint64 `json:"seq"`
}

// Encode serializes a command for embedding in a log entry.
func (c Command) Encode() ([]byte, error) { return json.Marshal(c) }

// DecodeCommand parses an entry payload back into a Command.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}
