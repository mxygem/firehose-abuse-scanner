package detect

import (
	"container/list"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

const RuleRateSpike = "rate.spike"

// rateEntry holds the recent event timestamps for a single DID. The slice
// is kept trimmed to entries within the configured window on every Inspect
// call so memory stays bounded by (rate × window) per active DID.
type rateEntry struct {
	did   string
	times []time.Time
}

// RateRule flags DIDs that emit at least `threshold` events within the
// sliding `window`. State is bounded by `maxDIDs` entries, evicted in LRU
// order so a long-running scanner cannot accumulate unbounded per-DID
// state when the firehose is dominated by transient authors.
type RateRule struct {
	window    time.Duration
	threshold int
	maxDIDs   int
	severity  RuleSeverity
	now       func() time.Time

	mu    sync.Mutex
	lru   *list.List
	index map[string]*list.Element
}

// NewRateRule constructs a rule with the given window/threshold. maxDIDs
// caps in-memory state; non-positive arguments fall back to sane defaults
// so misconfiguration cannot disable the detector silently.
func NewRateRule(window time.Duration, threshold, maxDIDs int, severity string) *RateRule {
	if window <= 0 {
		window = time.Second
	}
	if threshold < 1 {
		threshold = 1
	}
	if maxDIDs < 1 {
		maxDIDs = 1024
	}
	return &RateRule{
		window:    window,
		threshold: threshold,
		maxDIDs:   maxDIDs,
		severity:  toSeverity(severity),
		now:       time.Now,
		lru:       list.New(),
		index:     make(map[string]*list.Element, maxDIDs),
	}
}

func (r *RateRule) Inspect(_ context.Context, evt models.FirehoseEvent) []Hit {
	if evt.DID == "" {
		return nil
	}
	now := r.now()
	cutoff := now.Add(-r.window)

	r.mu.Lock()
	entry := r.touch(evt.DID)

	drop := 0
	for drop < len(entry.times) && entry.times[drop].Before(cutoff) {
		drop++
	}
	if drop > 0 {
		entry.times = entry.times[drop:]
	}
	entry.times = append(entry.times, now)
	count := len(entry.times)
	r.mu.Unlock()

	if count < r.threshold {
		return nil
	}
	return []Hit{{
		RuleID:   RuleRateSpike,
		Severity: r.severity.String(),
		Reason:   fmt.Sprintf("%d events in %s (threshold %d)", count, r.window, r.threshold),
		Evidence: map[string]string{
			"count":     strconv.Itoa(count),
			"window":    r.window.String(),
			"threshold": strconv.Itoa(r.threshold),
		},
	}}
}

// touch returns the entry for did, creating it if absent and evicting the
// least-recently-used entry when capacity is exceeded. Caller must hold r.mu.
func (r *RateRule) touch(did string) *rateEntry {
	if elem, ok := r.index[did]; ok {
		r.lru.MoveToFront(elem)
		return elem.Value.(*rateEntry)
	}
	entry := &rateEntry{did: did}
	r.index[did] = r.lru.PushFront(entry)
	if r.lru.Len() > r.maxDIDs {
		back := r.lru.Back()
		if back != nil {
			delete(r.index, back.Value.(*rateEntry).did)
			r.lru.Remove(back)
		}
	}
	return entry
}
