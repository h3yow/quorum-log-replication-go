package drill

import (
	"context"

	"github.com/h3yow/quorum-log-replication-go/pkg/cluster"
)

// frame carries the per-run dependencies shared by every scenario.
type frame struct {
	ctx    context.Context
	c      *cluster.Cluster
	faults Faults
	input  Input
}
