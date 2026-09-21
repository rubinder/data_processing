# Single-node engine benchmark

Generated 2026-09-15T09:30:51 on macOS-26.6.2-arm64-arm-64bit (8 cores), Python 3.10.11, Polars 1.44.2, DuckDB 1.5.5.

Dataset: 8,161,390 event rows in 144 partition files, 92.5 MB of zstd Parquet (2 day(s) x 24 hours x 3 page types).

Median wall-clock of 3 runs, seconds:

| analysis | eager | lazy | streaming | duckdb |
|---|---|---|---|---|
| funnel | 0.241 | 0.225 | 0.447 | 0.299 |
| hourly-traffic | 0.699 | 0.724 | 1.487 | 0.754 |
| page-type-summary | 0.663 | 0.687 | 1.421 | 0.772 |
| user-engagement | 0.717 | 0.729 | 1.324 | 0.934 |
| hourly-traffic@page_type=1 | 0.229 | 0.202 | 0.267 | 0.233 |
| sink-events-d-plus | 0.161 | 0.150 | 0.136 | 0.218 |

Peak resident set size of the measuring subprocess, MB:

| analysis | eager | lazy | streaming | duckdb |
|---|---|---|---|---|
| funnel | 1517.3 | 1340.1 | 2377.9 | 1446.5 |
| hourly-traffic | 2435.1 | 2476.8 | 3453.3 | 2673.8 |
| page-type-summary | 2448.7 | 2473.3 | 3595.3 | 2700.9 |
| user-engagement | 2638.2 | 2655.4 | 3371.0 | 2915.5 |
| hourly-traffic@page_type=1 | 1163.0 | 878.9 | 1369.1 | 1207.2 |
| sink-events-d-plus | 1061.7 | 614.4 | 502.1 | 1019.4 |

Verification: all variants agree with `lazy` on every analysis.
