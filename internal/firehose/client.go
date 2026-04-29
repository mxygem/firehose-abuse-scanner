package firehose

import (
	"context"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

type Client interface {
	// Subscribe starts emitting events onto the returned channel.
	// The channel is closed when ctx is cancelled or a fatal
	Subscribe(ctx context.Context) (<-chan models.FirehoseEvent, error)

	// Name returns a human-readable identifier for logging.
	Name() string
}
