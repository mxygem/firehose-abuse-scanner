package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/mxygem/firehose-abuse-scanner/internal/detect"
	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/mxygem/firehose-abuse-scanner/internal/storage"
)

// CompositeHandler is the production pipeline.Handler: it durably writes
// each event, runs every detector against it, and persists a flagged row
// for each hit. Detectors run sequentially per event because the worker
// pool already parallelizes across events.
type CompositeHandler struct {
	store     storage.Storer
	detectors []detect.Detector
}

func NewCompositeHandler(store storage.Storer, detectors ...detect.Detector) *CompositeHandler {
	return &CompositeHandler{store: store, detectors: detectors}
}

func (h *CompositeHandler) Handle(ctx context.Context, evt models.FirehoseEvent) error {
	if err := h.store.InsertEvent(ctx, evt); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	if len(h.detectors) == 0 {
		return nil
	}

	var errs []error
	for _, d := range h.detectors {
		for _, hit := range d.Inspect(ctx, evt) {
			rec := storage.FlaggedRecord{
				EventID:    evt.ID,
				DID:        evt.DID,
				Kind:       evt.Kind,
				Text:       evt.Text,
				ReceivedAt: evt.ReceivedAt,
				RuleID:     hit.RuleID,
				Severity:   hit.Severity,
				Reason:     hit.Reason,
				Evidence:   hit.Evidence,
			}
			if err := h.store.InsertFlagged(ctx, rec); err != nil {
				errs = append(errs, fmt.Errorf("insert flagged (%s): %w", hit.RuleID, err))
			}
		}
	}
	return errors.Join(errs...)
}
