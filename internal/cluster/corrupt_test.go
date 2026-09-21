package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/h3yow/quorum-log-replication-go/internal/raft"
)

// 持久化文件被截断 / 损坏时，重启必须以可区分原因拒绝。
func TestRestartRejectsCorruptAndTruncated(t *testing.T) {
	c := testCluster(t, "n1", "n2", "n3")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Put(ctx, "c", 1, "k", "v"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		file    string
		mutate  func(b []byte) []byte
		wantErr error
	}{
		{"truncated", "log.bin", func(b []byte) []byte { return b[:len(b)-3] }, raft.ErrTruncatedStorage},
		{"corrupt", "log.bin", func(b []byte) []byte {
			// 翻转载荷中部字节，保持长度不变，稳定触发 CRC 不匹配。
			b[len(b)/2] ^= 0xFF
			return b
		}, raft.ErrCorruptStorage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := c.DataDir("n1")
			p := filepath.Join(dir, tc.file)
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			c.Crash("n1")
			if err := os.WriteFile(p, tc.mutate(b), 0o644); err != nil {
				t.Fatal(err)
			}
			err = c.Restart(ctx, "n1")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
		})
	}
}
