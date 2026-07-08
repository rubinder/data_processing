-- Distributed impression schema for the impressions_cluster.
--
-- impressions_local : the physical, per-shard ReplicatedMergeTree table.
-- impressions       : a Distributed table that fans reads/writes out across
--                     both shards. Aggregations over it run concurrently on
--                     each node and the initiator merges the partial states,
--                     so per-impression grouping stays correct even though
--                     rows are sharded with rand().

CREATE TABLE IF NOT EXISTS default.impressions_local ON CLUSTER impressions_cluster
(
    user_id       String,
    impression_id String,
    page_type     UInt8,
    date          Date,
    hour          UInt8,
    minute        UInt8,
    second        UInt8,
    event_type    LowCardinality(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/impressions_local', '{replica}')
ORDER BY (page_type, date, hour, impression_id);

CREATE TABLE IF NOT EXISTS default.impressions ON CLUSTER impressions_cluster
AS default.impressions_local
ENGINE = Distributed(impressions_cluster, default, impressions_local, rand());
