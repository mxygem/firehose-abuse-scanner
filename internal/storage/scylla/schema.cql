-- firehose-abuse-scanner schema
--
-- Single-node demo: SimpleStrategy + RF=1. In production this would be
-- NetworkTopologyStrategy with RF=3 across racks, matching how Bluesky
-- runs Scylla for the atproto data plane.
--
-- All statements are idempotent (IF NOT EXISTS) so the bootstrap path
-- in internal/storage/scylla can apply this on every startup.

CREATE KEYSPACE IF NOT EXISTS firehose_scanner
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };

USE firehose_scanner;

-- events_by_did
--
-- Recent raw events for a single author. Backs the "show me what this DID
-- has been doing" lookup used by moderation review and the per-DID rate
-- detector (Phase 2).
--
-- Partition key: did
--   atproto DIDs (e.g. did:plc:abc123...) are naturally high-cardinality —
--   billions of users, so writes spread evenly across the cluster with no
--   risk of a hot partition. Each DID's activity is bounded by human pace,
--   so partitions stay small even for prolific posters; no extra bucketing
--   needed.
--
-- Clustering: received_at DESC, id
--   Newest-first matches the dominant read pattern (recent activity).
--   id breaks ties when two events land in the same millisecond.
CREATE TABLE IF NOT EXISTS events_by_did (
    did         text,
    received_at timestamp,
    id          text,
    kind        text,
    text        text,
    langs       list<text>,
    links       list<text>,
    created_at  timestamp,
    PRIMARY KEY (did, received_at, id)
) WITH CLUSTERING ORDER BY (received_at DESC, id ASC);

-- events_by_minute
--
-- Time-window scans across all events of a given kind, e.g. "all posts in
-- the last 5 minutes" for rate-spike analysis or a live dashboard.
--
-- Partition key: (kind, bucket_minute)
--   Partitioning by minute alone would funnel every writer onto the same
--   node for that minute — textbook hot partition. Compounding with kind
--   spreads write load across event types, and the minute bucket caps each
--   partition to ~one minute of throughput. Reads for a window of N minutes
--   touch N partitions per kind, which is fine for the demo's scan sizes.
--
-- Clustering: received_at DESC, id
--   Same reasoning as events_by_did — newest-first reads.
CREATE TABLE IF NOT EXISTS events_by_minute (
    kind          text,
    bucket_minute timestamp,
    received_at   timestamp,
    id            text,
    did           text,
    text          text,
    langs         list<text>,
    links         list<text>,
    created_at    timestamp,
    PRIMARY KEY ((kind, bucket_minute), received_at, id)
) WITH CLUSTERING ORDER BY (received_at DESC, id ASC);

-- flagged_events
--
-- Detector hits — which abuse rules fired against which events. This is
-- the read path for the moderation surface (CLI subcommand in Phase 3).
-- One row per (event, rule) pair so a single event tripping multiple rules
-- produces multiple rows.
--
-- Partition key: bucket_hour
--   Flagged events should be a small fraction of total throughput, so an
--   hourly bucket is wide enough to be efficient without becoming hot. If
--   hit volume grows we'd narrow this to bucket_minute or compound it with
--   severity, mirroring the events_by_minute pattern.
--
-- Clustering: received_at DESC, id, rule_id
--   "Recent hits" is the dominant read. rule_id in the cluster key keeps
--   multiple rules firing on the same event from colliding.
CREATE TABLE IF NOT EXISTS flagged_events (
    bucket_hour timestamp,
    received_at timestamp,
    id          text,
    rule_id     text,
    did         text,
    kind        text,
    text        text,
    severity    text,
    reason      text,
    evidence    map<text, text>,
    PRIMARY KEY (bucket_hour, received_at, id, rule_id)
) WITH CLUSTERING ORDER BY (received_at DESC, id ASC, rule_id ASC);
