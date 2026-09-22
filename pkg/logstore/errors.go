package logstore

import "errors"

// Distinguishable, persisted-state failure reasons. A node must refuse to
// serve rather than continue in an uncertain state when these are returned.
var (
	// ErrCorruptLog is returned when the WAL fails CRC validation or its
	// framing cannot be decoded.
	ErrCorruptLog = errors.New("logstore: corrupt log record (crc/framing mismatch)")
	// ErrTruncatedLog is returned when the WAL ends in a torn/partial write.
	ErrTruncatedLog = errors.New("logstore: truncated/torn trailing record")
	// ErrCorruptSnapshot is returned when snapshot bytes fail validation.
	ErrCorruptSnapshot = errors.New("logstore: corrupt snapshot (checksum mismatch)")
	// ErrSnapOutOfDate is returned when installing a snapshot older than the
	// current snapshot state.
	ErrSnapOutOfDate = errors.New("logstore: snapshot older than current state")
)
