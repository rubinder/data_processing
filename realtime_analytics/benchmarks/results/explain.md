# Why the queries got faster: ClickHouse's own index analysis

Output of `EXPLAIN indexes = 1`, which reports how many parts and
granules survive each pruning step. A granule is 8192 rows; granules
not selected are never read from disk.

- generated: `2026-07-25T17:29:42`
- rows: `2,000,000` (ratios are what matter, and hold at any size)
- tenant: `acct_0000`
- query condition cache disabled, so an unindexed table cannot borrow a previous run's granule matches

Regenerate with `python benchmarks/explain_evidence.py`.

## Tenant dashboard — WHERE account_id = ... AND event_ts IN (7 days)

The sorting key is (account_id, event_ts), so this filter is a key prefix and collapses to a mark range.

| stage | indexes consulted | parts | granules read |
| --- | --- | ---: | ---: |
| `v1_typed` | none — `ORDER BY tuple()` | all | **246**/246 (full scan) |
| `v2_sorted` | PrimaryKey | 1/1 | **2**/245 |
| `v3_partitioned` | Partition, PrimaryKey | 1/1 | **2**/247 |
| `v4_indexed` | Partition, PrimaryKey | 1/1 | **2**/247 |

<details><summary>Raw plan — <code>v2_sorted</code></summary>

```
            ReadFromMergeTree (default.events_v2_sorted)
            Indexes:
              PrimaryKey
                Keys:
                  account_id
                  event_ts
                Condition: and((event_ts in (-Inf, '1780891200')), and((event_ts in ['1780286400', +Inf)), (account_id in ['acct_0000', 'acct_0000'])))
                Parts: 1/1
                Granules: 2/245
                Search Algorithm: binary search
              Ranges: 1
```
</details>

## Platform-wide — WHERE event_ts IN (7 days), no tenant filter

No tenant filter, so the leading key column buys nothing and only PARTITION BY can prune.

| stage | indexes consulted | parts | granules read |
| --- | --- | ---: | ---: |
| `v1_typed` | none — `ORDER BY tuple()` | all | **246**/246 (full scan) |
| `v2_sorted` | PrimaryKey | 1/1 | **201**/245 |
| `v3_partitioned` | Partition, PrimaryKey | 1/1 | **73**/247 |
| `v4_indexed` | Partition, PrimaryKey | 1/1 | **72**/247 |

<details><summary>Raw plan — <code>v3_partitioned</code></summary>

```
            ReadFromMergeTree (default.events_v3_partitioned)
            Indexes:
              Min-Max
                Keys:
                  event_ts
                Condition: and((event_ts in (-Inf, '1780891200')), (event_ts in ['1780286400', +Inf)))
                Parts: 1/3
                Granules: 80/247
              Partition
                Keys:
                  toYYYYMM(event_ts)
                Condition: and((toYYYYMM(event_ts) in (-Inf, 202606]), (toYYYYMM(event_ts) in [202606, +Inf)))
                Parts: 1/1
                Granules: 80/80
              PrimaryKey
                Keys:
                  event_ts
                Condition: and((event_ts in (-Inf, '1780891200')), (event_ts in ['1780286400', +Inf)))
                Parts: 1/1
                Granules: 73/80
                Search Algorithm: generic exclusion search
              Ranges: 4
```
</details>

## Conversation drill-down — WHERE conversation_id = <uuid>

A high-cardinality needle: neither the sorting key nor the partition key applies, which is what the bloom filter is for.

| stage | indexes consulted | parts | granules read |
| --- | --- | ---: | ---: |
| `v1_typed` | none — `ORDER BY tuple()` | all | **246**/246 (full scan) |
| `v2_sorted` | PrimaryKey | 1/1 | **245**/245 |
| `v3_partitioned` | Partition, PrimaryKey | 3/3 | **247**/247 |
| `v4_indexed` | Partition, PrimaryKey, idx_conversation | 2/3 | **4**/247 |

<details><summary>Raw plan — <code>v4_indexed</code></summary>

```
        ReadFromMergeTree (default.events_v4_indexed)
        Indexes:
          Min-Max
            Condition: true
            Parts: 3/3
            Granules: 247/247
          Partition
            Condition: true
            Parts: 3/3
            Granules: 247/247
          PrimaryKey
            Condition: true
            Parts: 3/3
            Granules: 247/247
          Skip
            Name: idx_conversation
            Description: bloom_filter GRANULARITY 1
            Parts: 2/3
            Granules: 4/247
          Ranges: 4
```
</details>

