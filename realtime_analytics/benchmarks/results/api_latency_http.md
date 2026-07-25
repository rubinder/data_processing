# API latency

- generated: `2026-07-25T15:55:53`
- mode: `http`
- requests per endpoint: `200`
- SLO: p95 < 100.0ms -> **PASS** (worst p95 8.47ms)

| endpoint | p50 ms | p95 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: |
| `/v1/accounts/acct_0000/summary?days=7` | 5.37 | 6.32 | 7.72 | 7.76 |
| `/v1/accounts/acct_0000/summary?days=30` | 5.47 | 7.83 | 16.82 | 21.8 |
| `/v1/accounts/acct_0000/agent-latency?hours=168` | 5.92 | 7.75 | 9.93 | 16.21 |
| `/v1/accounts/acct_0000/hourly?hours=168` | 5.13 | 6.78 | 9.82 | 14.98 |
| `/v1/accounts/acct_0000/intents?days=7` | 6.05 | 8.15 | 9.78 | 9.87 |
| `/v1/conversations/3d755eff-85fa-f035-b777-17a7c5b447fb` | 4.3 | 8.47 | 14.85 | 18.12 |
