package pipeline

import (
	"context"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
)

type ScyllaHandler struct {
	store storage.Storer
}

func NewScyllaHandler(store storage.Storer) *ScyllaHandler {
	return &ScyllaHandler{store: store}
}

func (h *ScyllaHandler) Handle(ctx context.Context, evt models.FirehoseEvent) error {
	return h.store.InsertEvent(ctx, evt)
}
