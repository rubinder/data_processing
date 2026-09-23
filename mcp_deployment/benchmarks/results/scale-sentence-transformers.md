# Scale — synthetic templates through load, pgvector sync, search and execution (50 queries each, `sentence-transformers/all-MiniLM-L6-v2`, HNSW cosine)

| templates | catalog rows | load + lint | first sync (embed + write) | no-op re-sync | search p50 | search p95 | planner uses HNSW | search p50, index forced | execute p50 | execute p95 |
|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 540 | 324 ms | 1873 ms | 3 ms | 15.0 ms | 18.4 ms | no | 15.0 ms | 0.7 ms | 1.0 ms |
