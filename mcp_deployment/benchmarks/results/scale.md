# Scale — synthetic templates through load, pgvector sync, search and execution (50 queries each, `hash-v1-384`, HNSW cosine)

| templates | catalog rows | load + lint | first sync (embed + write) | no-op re-sync | search p50 | search p95 | planner uses HNSW | search p50, index forced | execute p50 | execute p95 |
|---|---|---|---|---|---|---|---|---|---|---|
| 20 | 60 | 13 ms | 44 ms | 1 ms | 0.5 ms | 0.6 ms | no | 0.5 ms | 0.6 ms | 0.8 ms |
| 100 | 140 | 63 ms | 96 ms | 1 ms | 0.6 ms | 0.7 ms | no | 0.6 ms | 0.6 ms | 0.8 ms |
| 500 | 540 | 307 ms | 540 ms | 2 ms | 1.6 ms | 1.8 ms | no | 1.9 ms | 0.8 ms | 1.5 ms |
| 2000 | 2040 | 1251 ms | 2160 ms | 4 ms | 4.7 ms | 5.6 ms | no | 4.5 ms | 0.6 ms | 1.1 ms |
| 10000 | 10040 | 6269 ms | 13008 ms | 24 ms | 0.7 ms | 0.9 ms | yes | 0.6 ms | 0.6 ms | 0.7 ms |
