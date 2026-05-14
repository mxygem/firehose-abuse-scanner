package detect

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateRule_BelowThreshold(t *testing.T) {
	r := NewRateRule(time.Second, 5, 100, SeverityMedium.String())
	for i := 0; i < 4; i++ {
		hits := r.Inspect(context.Background(), didEvt("did:plc:a"))
		assert.Empty(t, hits)
	}
}

func TestRateRule_HitsAtThreshold(t *testing.T) {
	r := NewRateRule(time.Second, 5, 100, SeverityMedium.String())

	var lastHits []Hit
	for i := 0; i < 5; i++ {
		lastHits = r.Inspect(context.Background(), didEvt("did:plc:a"))
	}
	require.Len(t, lastHits, 1)
	assert.Equal(t, RuleRateSpike, lastHits[0].RuleID)
	assert.Equal(t, SeverityMedium.String(), lastHits[0].Severity)
	assert.Equal(t, "5", lastHits[0].Evidence["count"])
	assert.Equal(t, "5", lastHits[0].Evidence["threshold"])
}

func TestRateRule_SlidingWindowExpiresOldEvents(t *testing.T) {
	r := NewRateRule(time.Second, 3, 100, SeverityMedium.String())

	base := time.Unix(1_000_000, 0)
	current := base
	r.now = func() time.Time { return current }

	for i := 0; i < 2; i++ {
		assert.Empty(t, r.Inspect(context.Background(), didEvt("did:plc:a")))
	}

	current = base.Add(2 * time.Second)
	for i := 0; i < 2; i++ {
		assert.Empty(t, r.Inspect(context.Background(), didEvt("did:plc:a")),
			"old events should have aged out, hit %d", i)
	}

	hits := r.Inspect(context.Background(), didEvt("did:plc:a"))
	require.Len(t, hits, 1)
	assert.Equal(t, "3", hits[0].Evidence["count"])
}

func TestRateRule_PerDIDIsolation(t *testing.T) {
	r := NewRateRule(time.Second, 3, 100, SeverityLow.String())

	for i := 0; i < 2; i++ {
		assert.Empty(t, r.Inspect(context.Background(), didEvt("did:plc:a")))
		assert.Empty(t, r.Inspect(context.Background(), didEvt("did:plc:b")))
	}

	hitsA := r.Inspect(context.Background(), didEvt("did:plc:a"))
	require.Len(t, hitsA, 1, "did:plc:a should hit on its 3rd event")

	// did:plc:b only saw 2 events, still below threshold.
	r2 := NewRateRule(time.Second, 3, 100, SeverityLow.String())
	for i := 0; i < 5; i++ {
		r2.Inspect(context.Background(), didEvt("did:plc:noisy"))
	}
	assert.Empty(t, r2.Inspect(context.Background(), didEvt("did:plc:quiet")),
		"quiet DID should not inherit the noisy DID's count")
}

func TestRateRule_EvictsLRU(t *testing.T) {
	r := NewRateRule(time.Hour, 2, 2, SeverityLow.String())

	r.Inspect(context.Background(), didEvt("did:plc:a"))
	r.Inspect(context.Background(), didEvt("did:plc:b"))
	r.Inspect(context.Background(), didEvt("did:plc:c"))

	r.mu.Lock()
	_, hasA := r.index["did:plc:a"]
	_, hasB := r.index["did:plc:b"]
	_, hasC := r.index["did:plc:c"]
	size := r.lru.Len()
	r.mu.Unlock()

	assert.Equal(t, 2, size, "LRU size capped at maxDIDs")
	assert.False(t, hasA, "oldest entry should have been evicted")
	assert.True(t, hasB)
	assert.True(t, hasC)
}

func TestRateRule_EmptyDIDSkipped(t *testing.T) {
	r := NewRateRule(time.Second, 1, 100, SeverityLow.String())
	hits := r.Inspect(context.Background(), models.FirehoseEvent{DID: "", Kind: models.EventPost})
	assert.Empty(t, hits)
}

func TestRateRule_ConcurrentSafe(t *testing.T) {
	r := NewRateRule(time.Second, 50, 1000, SeverityLow.String())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				did := "did:plc:" + strconv.Itoa((workerID*200+j)%50)
				r.Inspect(context.Background(), didEvt(did))
			}
		}(i)
	}
	wg.Wait()
}

func didEvt(did string) models.FirehoseEvent {
	return models.FirehoseEvent{DID: did, Kind: models.EventPost}
}
