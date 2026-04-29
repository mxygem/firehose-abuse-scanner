package pipeline

import (
	"context"
	"log/slog"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

// LogHandler is a no-op  handler used during development.
// Replace with the real abuse detection ETL in a later release.
type LogHandler struct{}

func (h *LogHandler) Handle(_ context.Context, evt models.FirehoseEvent) error {
	l := slog.Default()

	l.Debug("event received",
		"id", evt.ID,
		"did", evt.DID,
		"kind", evt.Kind,
		"created_at", evt.CreatedAt,
		"received_at", evt.ReceivedAt,
	)

	return nil
}
