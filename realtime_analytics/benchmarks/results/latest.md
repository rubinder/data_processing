# ClickHouse tuning benchmark

- generated: `2026-07-25T15:24:38`
- backend: `chdb`
- rows: `12,000,000`
- tenant under test: `acct_0000`
- window: `2026-06-01 00:00:00` .. `2026-06-08 00:00:00`
- timed rounds: `15` interleaved across all stages, after `4` warmup passes

## Tenant dashboard (1 account, 7 days, daily rollup)

| stage | p50 ms | p95 ms | stdev | rows read | bytes read | vs naive | correct |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| `v0_naive` Naive: all String, JSON blob, ORDER BY tuple() | 156.09 | 181.09 | 13.41 | 12,000,000 | 2012.5 MB | 1.0x | yes |
| `v1_typed` + Physical types, LowCardinality, codecs | 41.59 | 49.47 | 4.58 | 12,000,000 | 167.9 MB | 3.7x | yes |
| `v2_sorted` + Sorting key (account_id, event_ts) | 4.75 | 7.09 | 1.19 | 90,112 | 1.3 MB | 25.5x | yes |
| `v3_partitioned` + PARTITION BY toYYYYMM(event_ts) | 4.81 | 7.1 | 1.07 | 90,112 | 1.3 MB | 25.5x | yes |
| `v4_indexed` + Skipping indexes (bloom_filter, minmax) | 4.71 | 5.44 | 0.47 | 90,112 | 1.3 MB | 33.3x | yes |
| `v5_matview` + Materialized views (pre-aggregation) | 2.05 | 2.41 | 0.17 | 128 | 0.0 MB | 75.1x | yes |
| `v6_projection` Alternative: projections instead of MV/bloom | 5.06 | 5.98 | 0.85 | 90,112 | 1.3 MB | 30.3x | yes |

## Conversation drill-down (single UUID needle)

| stage | p50 ms | p95 ms | stdev | rows read | bytes read | vs naive | correct |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| `v0_naive` Naive: all String, JSON blob, ORDER BY tuple() | 20.68 | 26.04 | 2.89 | 12,000,000 | 468.1 MB | 1.0x | yes |
| `v1_typed` + Physical types, LowCardinality, codecs | 9.24 | 10.21 | 0.7 | 12,000,000 | 192.0 MB | 2.6x | yes |
| `v2_sorted` + Sorting key (account_id, event_ts) | 9.88 | 10.47 | 0.64 | 12,000,000 | 192.1 MB | 2.5x | yes |
| `v3_partitioned` + PARTITION BY toYYYYMM(event_ts) | 9.96 | 10.83 | 0.71 | 12,000,000 | 192.1 MB | 2.4x | yes |
| `v4_indexed` + Skipping indexes (bloom_filter, minmax) | 2.89 | 3.67 | 0.5 | 186,479 | 3.1 MB | 7.1x | yes |
| `v6_projection` Alternative: projections instead of MV/bloom | 1.69 | 2.01 | 0.26 | 24,576 | 0.5 MB | 13.0x | yes |

## Platform-wide health (all tenants, 7 days)

| stage | p50 ms | p95 ms | stdev | rows read | bytes read | vs naive | correct |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| `v0_naive` Naive: all String, JSON blob, ORDER BY tuple() | 260.76 | 285.65 | 24.24 | 12,000,000 | 1906.1 MB | 1.0x | yes |
| `v1_typed` + Physical types, LowCardinality, codecs | 38.22 | 42.87 | 3.09 | 12,000,000 | 156.0 MB | 6.7x | yes |
| `v2_sorted` + Sorting key (account_id, event_ts) | 19.73 | 21.31 | 1.34 | 4,135,376 | 43.3 MB | 13.4x | yes |
| `v3_partitioned` + PARTITION BY toYYYYMM(event_ts) | 14.11 | 15.56 | 0.92 | 2,469,328 | 32.0 MB | 18.4x | yes |
| `v4_indexed` + Skipping indexes (bloom_filter, minmax) | 13.39 | 14.86 | 0.91 | 2,472,486 | 32.1 MB | 19.2x | yes |
| `v5_matview` + Materialized views (pre-aggregation) | 1.47 | 2.0 | 0.25 | 91 | 0.0 MB | 142.8x | yes |
| `v6_projection` Alternative: projections instead of MV/bloom | 14.06 | 15.97 | 1.14 | 2,472,784 | 32.1 MB | 17.9x | yes |

## Slow responses by model/channel (latency > 5s)

| stage | p50 ms | p95 ms | stdev | rows read | bytes read | vs naive | correct |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| `v0_naive` Naive: all String, JSON blob, ORDER BY tuple() | 190.0 | 204.31 | 13.61 | 12,000,000 | 2052.9 MB | 1.0x | yes |
| `v1_typed` + Physical types, LowCardinality, codecs | 48.45 | 55.07 | 4.32 | 12,000,000 | 169.9 MB | 3.7x | yes |
| `v2_sorted` + Sorting key (account_id, event_ts) | 4.02 | 5.75 | 0.8 | 90,112 | 1.4 MB | 35.5x | yes |
| `v3_partitioned` + PARTITION BY toYYYYMM(event_ts) | 4.11 | 4.83 | 0.37 | 90,112 | 1.4 MB | 42.3x | yes |
| `v4_indexed` + Skipping indexes (bloom_filter, minmax) | 4.2 | 5.07 | 0.54 | 90,112 | 1.4 MB | 40.3x | yes |
| `v6_projection` Alternative: projections instead of MV/bloom | 4.39 | 5.49 | 0.56 | 90,112 | 1.4 MB | 37.2x | yes |

## Storage

| table | rows | on disk | uncompressed | ratio | parts |
| --- | ---: | ---: | ---: | ---: | ---: |
| `events_v0_naive` | 12,000,000 | 1003.2 MB | 5075.1 MB | 5.06x | 1 |
| `events_v1_typed` | 12,000,000 | 405.3 MB | 1229.0 MB | 3.03x | 1 |
| `events_v2_sorted` | 12,000,000 | 399.0 MB | 1229.1 MB | 3.08x | 1 |
| `events_v3_partitioned` | 12,000,000 | 400.8 MB | 1229.1 MB | 3.07x | 3 |
| `events_v4_indexed` | 12,000,000 | 404.5 MB | 1229.1 MB | 3.04x | 3 |
| `conversation_daily` | 18,010 | 10.7 MB | 25.3 MB | 2.37x | 3 |
| `events_v6_projected` | 12,000,000 | 770.0 MB | 1229.1 MB | 1.6x | 3 |
| `agent_hourly` | 1,116,404 | 28.5 MB | 72.2 MB | 2.53x | 3 |
| `platform_daily` | 91 | 0.2 MB | 0.4 MB | 1.6x | 1 |
