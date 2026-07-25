# Partition granularity: pruning benefit vs per-part cost

- generated: `2026-07-25T15:18:05`
- rows: `20,000,000`, tenant: `acct_0000`
- `15` interleaved rounds after `4` warmup passes

## tenant_dashboard

| partition key | parts | p50 ms | p95 ms | rows read |
| --- | ---: | ---: | ---: | ---: |
| no partitioning | 1 | 7.25 | 9.8 | 147,456 |
| monthly (~3 partitions) | 3 | 10.56 | 12.02 | 147,456 |
| weekly (~14 partitions) | 14 | 19.56 | 21.98 | 147,456 |
| daily (~91 partitions) | 91 | 79.75 | 92.83 | 172,032 |

## platform_wide

| partition key | parts | p50 ms | p95 ms | rows read |
| --- | ---: | ---: | ---: | ---: |
| no partitioning | 1 | 26.68 | 28.86 | 4,749,837 |
| monthly (~3 partitions) | 3 | 23.62 | 26.17 | 3,154,946 |
| weekly (~14 partitions) | 14 | 24.81 | 28.05 | 1,555,832 |
| daily (~91 partitions) | 91 | 87.93 | 92.33 | 1,555,832 |

