# Concurrency — 400 mixed search/run calls per cell, as mcp_reader, hashing embedder

| mode | concurrent callers | calls/s | p50 | p95 | max |
|---|---|---|---|---|---|
| connection per call | 1 | 188 | 5.2 ms | 6.6 ms | 20.0 ms |
| connection per call | 8 | 491 | 15.7 ms | 23.9 ms | 42.0 ms |
| connection per call | 32 | 595 | 49.9 ms | 76.5 ms | 134.9 ms |
| pool of 8 | 1 | 1586 | 0.6 ms | 0.8 ms | 7.9 ms |
| pool of 8 | 8 | 3639 | 2.1 ms | 3.2 ms | 5.0 ms |
| pool of 8 | 32 | 3569 | 8.4 ms | 11.6 ms | 13.1 ms |
| pool of 32 | 1 | 1730 | 0.5 ms | 0.7 ms | 8.5 ms |
| pool of 32 | 8 | 3621 | 2.1 ms | 3.5 ms | 4.5 ms |
| pool of 32 | 32 | 2908 | 10.5 ms | 13.9 ms | 22.1 ms |
