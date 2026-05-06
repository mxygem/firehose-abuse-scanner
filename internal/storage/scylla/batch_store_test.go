package scylla

import (
	"hash/maphash"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mxygem/firehose-abuse-scanner/internal/models"
)

// newRoutingStore builds a BatchStore stub wired only with the fields the
// routing functions read. Avoids spinning up gocql/flush goroutines for what
// are pure-function tests.
func newRoutingStore(didShards, minuteShards int) *BatchStore {
	return &BatchStore{
		seed:         maphash.MakeSeed(),
		didShards:    make([]batchBuffer, didShards),
		minuteShards: make([]batchBuffer, minuteShards),
	}
}

func TestBatchStore_ShardForDID_Stable(t *testing.T) {
	s := newRoutingStore(64, 64)
	for _, did := range []string{
		"did:plc:abc",
		"did:plc:def",
		"did:plc:ghi",
		"",
		"did:plc:0000000000000001",
	} {
		first := s.shardForDID(did)
		for i := 0; i < 100; i++ {
			require.Equal(t, first, s.shardForDID(did),
				"shardForDID(%q) must be stable", did)
		}
		require.GreaterOrEqual(t, first, 0)
		require.Less(t, first, len(s.didShards))
	}
}

func TestBatchStore_ShardForDID_Distributes(t *testing.T) {
	// 5000 distinct DIDs over 64 shards: every shard should land at least
	// one event. Tightens to "no shard left empty" — a weak fairness check
	// that catches a pathologically collapsed hash.
	s := newRoutingStore(64, 64)
	hits := make([]int, len(s.didShards))
	for i := 0; i < 5000; i++ {
		did := generateDID(i)
		hits[s.shardForDID(did)]++
	}
	for i, h := range hits {
		require.Greater(t, h, 0, "shard %d received zero DIDs", i)
	}
}

func TestBatchStore_ShardForMinute_Stable(t *testing.T) {
	s := newRoutingStore(64, 64)
	bucket := time.Date(2026, 5, 6, 12, 34, 0, 0, time.UTC)
	for _, kind := range []models.EventKind{
		models.EventPost,
		models.EventRepost,
		models.EventFollow,
		models.EventProfile,
	} {
		first := s.shardForMinute(kind, bucket)
		for i := 0; i < 100; i++ {
			require.Equal(t, first, s.shardForMinute(kind, bucket),
				"shardForMinute(%s, %v) must be stable", kind, bucket)
		}
		require.GreaterOrEqual(t, first, 0)
		require.Less(t, first, len(s.minuteShards))
	}
}

func TestBatchStore_ShardForMinute_DiffersByBucket(t *testing.T) {
	// Different minute buckets for the same kind should mostly land on
	// different shards. We don't insist every bucket differs (collisions
	// happen with hash%N) but the set across 1000 buckets must cover most
	// of the shard space.
	s := newRoutingStore(64, 64)
	hits := make(map[int]struct{})
	bucket := time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 1000; i++ {
		hits[s.shardForMinute(models.EventPost, bucket.Add(time.Duration(i)*time.Minute))] = struct{}{}
	}
	// 1000 buckets across 64 shards: in practice all 64 are hit. Allow a
	// small slack — anything below 50 is suspect.
	require.GreaterOrEqual(t, len(hits), 50)
}

func TestBatchStore_AppendShard_FlushesAtMaxSize(t *testing.T) {
	s := &BatchStore{maxSize: 3}
	shard := &batchBuffer{buf: make([]models.FirehoseEvent, 0, s.maxSize)}

	// Below maxSize: returns nil, leaves contents in shard.
	require.Nil(t, s.appendShard(shard, models.FirehoseEvent{ID: "a"}))
	require.Nil(t, s.appendShard(shard, models.FirehoseEvent{ID: "b"}))
	require.Len(t, shard.buf, 2)

	// At maxSize: returns the full slice, resets the shard.
	out := s.appendShard(shard, models.FirehoseEvent{ID: "c"})
	require.Len(t, out, 3)
	require.Equal(t, []string{"a", "b", "c"}, ids(out))
	require.Empty(t, shard.buf)
	require.Equal(t, s.maxSize, cap(shard.buf))
}

func TestBatchStore_SwapShardBuffer(t *testing.T) {
	s := &BatchStore{maxSize: 8}
	shard := &batchBuffer{buf: []models.FirehoseEvent{{ID: "a"}, {ID: "b"}}}

	out := s.swapShardBuffer(shard)
	require.Equal(t, []string{"a", "b"}, ids(out))
	require.Empty(t, shard.buf)

	// Empty swap returns nil.
	require.Nil(t, s.swapShardBuffer(shard))
}

func generateDID(i int) string {
	const hex = "0123456789abcdef"
	out := []byte("did:plc:0000000000000000")
	for j := 0; j < 16 && i > 0; j++ {
		out[len(out)-1-j] = hex[i&0xf]
		i >>= 4
	}
	return string(out)
}

func ids(events []models.FirehoseEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.ID
	}
	return out
}
