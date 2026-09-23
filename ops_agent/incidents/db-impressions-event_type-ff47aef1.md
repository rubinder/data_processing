# Schema Drift — db.impressions

**Severity:** renaming

**Detected as of:** 2026-06-05

## What was observed

column 'event_type' was renamed to 'event_name' (field id 5); the contract still declares the old name

## Why this severity

'event_type' was renamed to 'event_name'. Both are field id 5: Iceberg resolves columns by ID, not by name, so every file written under the old name still reads back correctly and no data was rewritten or lost. What is wrong is the published contract, which still declares 'event_type'. That needs a recorded decision, not a rollback.

## Evidence

```
{'change': 'renamed', 'column': 'event_type', 'renamed_to': 'event_name', 'field_id': 5, 'declared_type': 'string', 'observed_type': 'string', 'type_compatible': True}
```
