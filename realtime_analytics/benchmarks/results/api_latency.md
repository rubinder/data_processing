# API latency

- generated: `2026-07-25T15:25:12`
- mode: `embedded`
- requests per endpoint: `200`
- SLO: p95 < 100.0ms -> **PASS** (worst p95 1.19ms)

| endpoint | p50 ms | p95 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: |
| `/v1/accounts/acct_0000/summary?days=7` | 0.91 | 1.0 | 1.08 | 1.09 |
| `/v1/accounts/acct_0000/summary?days=30` | 0.96 | 1.13 | 1.34 | 1.64 |
| `/v1/accounts/acct_0000/agent-latency?hours=168` | 1.0 | 1.09 | 1.17 | 1.18 |
| `/v1/accounts/acct_0000/hourly?hours=168` | 1.04 | 1.12 | 1.23 | 1.28 |
| `/v1/accounts/acct_0000/intents?days=7` | 1.09 | 1.17 | 1.26 | 1.27 |
| `/v1/conversations/b38f3041-1bd9-6391-c6a7-562329e8a29f` | 1.11 | 1.19 | 1.27 | 1.28 |
