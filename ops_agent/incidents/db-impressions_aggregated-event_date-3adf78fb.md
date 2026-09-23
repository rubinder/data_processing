# Monitor Breach — db.impressions_aggregated

**Severity:** breaking

**Detected as of:** 2026-06-05

## What was observed

[db.impressions_aggregated_arrival_gap] 1 missing period(s): 2026-06-05

## Why this severity

Monitor 'db.impressions_aggregated_arrival_gap' breached on db.impressions_aggregated: [db.impressions_aggregated_arrival_gap] 1 missing period(s): 2026-06-05 A monitor breach is already a judged verdict; there is no additive reading of a check that has failed.

## Evidence

```
{'monitor': 'db.impressions_aggregated_arrival_gap', 'kind': 'arrival_gap', 'column': 'event_date', 'metric': 1.0, 'baseline': None, 'status': 'breach'}
```
