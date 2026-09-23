# Monitor Breach — db.impressions

**Severity:** breaking

**Detected as of:** 2026-06-05

## What was observed

[raw_impressions_row_count] 5 is 1% of trailing median 486

## Why this severity

Monitor 'raw_impressions_row_count' breached on db.impressions: [raw_impressions_row_count] 5 is 1% of trailing median 486 A monitor breach is already a judged verdict; there is no additive reading of a check that has failed.

## Evidence

```
{'monitor': 'raw_impressions_row_count', 'kind': 'row_count', 'column': None, 'metric': 5.0, 'baseline': 486.0, 'status': 'breach'}
```
