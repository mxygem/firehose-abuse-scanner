package detect

import (
	"container/list"
	"context"
	"fmt"
	"hash/maphash"
	"strconv"
	"sync"
	"time"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

const RuleRateSpike = "rate.spike"

const rateShards = 64

type rateEntry struct {
	did   string
	times []time.Time
}

type rateShard struct {
	mu    sync.Mutex
	lru   *list.List
	index map[string]*list.Element
}

// RateRule flags DIDs that emit at least `threshold` events within the
// sliding `window`. State is sharded across rateShards independent maps to
// eliminate mutex contention under high throughput — at 1M events/sec each
// shard handles ~15K ops/sec.
type RateRule struct {
	window    time.Duration
	threshold int
	maxDIDs   int
	severity  RuleSeverity
	now       func() time.Time

	seed   maphash.Seed
	shards [rateShards]rateShard
}

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
	r := &RateRule{
		window:    window,
		threshold: threshold,
		maxDIDs:   maxDIDs,
		severity:  toSeverity(severity),
		now:       time.Now,
		seed:      maphash.MakeSeed(),
	}
	for i := range r.shards {
		r.shards[i] = rateShard{
			lru:   list.New(),
			index: make(map[string]*list.Element, maxDIDs/rateShards+1),
		}
	}
	return r
}

func (r *RateRule) Inspect(_ context.Context, evt models.FirehoseEvent) []Hit {
	if evt.DID == "" {
		return nil
	}
	now := r.now()
	cutoff := now.Add(-r.window)

	shard := &r.shards[r.shardFor(evt.DID)]
	shard.mu.Lock()
	entry := r.touch(shard, evt.DID)

	drop := 0
	for drop < len(entry.times) && entry.times[drop].Before(cutoff) {
		drop++
	}
	if drop > 0 {
		entry.times = entry.times[drop:]
	}
	entry.times = append(entry.times, now)
	count := len(entry.times)
	shard.mu.Unlock()

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

func (r *RateRule) shardFor(did string) uint64 {
	var h maphash.Hash
	h.SetSeed(r.seed)
	h.WriteString(did)
	return h.Sum64() % uint64(len(r.shards))
}

func (r *RateRule) touch(shard *rateShard, did string) *rateEntry {
	if elem, ok := shard.index[did]; ok {
		shard.lru.MoveToFront(elem)
		return elem.Value.(*rateEntry)
	}
	entry := &rateEntry{did: did}
	shard.index[did] = shard.lru.PushFront(entry)
	if shard.lru.Len() > r.maxDIDs/rateShards+1 {
		back := shard.lru.Back()
		if back != nil {
			delete(shard.index, back.Value.(*rateEntry).did)
			shard.lru.Remove(back)
		}
	}
	return entry
}
