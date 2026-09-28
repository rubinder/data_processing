# Engines — the four analyses, best of 7 runs, 3000 Iceberg rows

| analysis | Trino over Iceberg, computed | Trino over PostgreSQL gold, precomputed | native PostgreSQL gold, precomputed |
|---|---|---|---|
| funnel_analysis | 178.1 ms (median 210.0) | 21.2 ms (median 28.9) | 0.1 ms (median 0.2) |
| page_type_summary | 295.9 ms (median 435.0) | 22.3 ms (median 23.3) | 0.2 ms (median 0.2) |
| user_engagement | 218.6 ms (median 256.2) | 23.4 ms (median 39.1) | 0.3 ms (median 0.4) |
| hourly_traffic | 110.8 ms (median 122.9) | 21.9 ms (median 25.0) | 0.2 ms (median 0.3) |
