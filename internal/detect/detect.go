package detect

import (
	"context"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

type Detector interface {
	Inspect(ctx context.Context, evt models.FirehoseEvent) []Hit
}

type Hit struct {
	RuleID   string
	Severity string
	Reason   string
	Evidence map[string]string
}
