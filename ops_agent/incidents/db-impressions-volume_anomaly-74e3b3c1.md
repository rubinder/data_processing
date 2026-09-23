# Volume Anomaly — db.impressions

**Severity:** breaking

**Detected as of:** 2026-06-05

## What was observed

latest snapshot added 5 rows, below 50% of trailing median 486

## Why this severity

Latest batch added 5 rows against a trailing median of 486. Downstream aggregates will be silently wrong rather than obviously missing.

## Evidence

```
{'rows_in_latest_snapshot': 5, 'median': 486}
```
