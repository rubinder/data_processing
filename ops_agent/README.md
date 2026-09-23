# Ops agent — contracts, monitors and a rules-first LangGraph agent over Iceberg

An operations layer for the Iceberg impression tables that
[`iceberg_deployment/`](../iceberg_deployment/) creates: declared **data
contracts**, per-feed **monitors** with persisted baselines, **arrival SLAs**,
a **LangGraph agent** that senses schema drift, enum drift, volume collapse and
staleness against those contracts, and a **daily report** that diffs findings
day over day. Dry-run by default, offline by default, and the LLM is only
consulted for findings no rule matches — never in the tests.

Ported from a standalone lakehouse project and pointed at this repository's
event model, so the agent watches the same `db.impressions` table the Spark
jobs, dbt models, DuckDB, ClickHouse and Polars modules all read.

```bash
./deploy.sh test     # 72 tests: 67 rules tests on a fake engine, 5 Spark tests on real Iceberg tables
./deploy.sh demo     # the whole story: seed, clean run, evolve the schema, drift, report
./deploy.sh agent    # run the agent against ./iceberg-warehouse (add --execute to file GitHub issues)
```

---

## What "agentic ops" means here

The agent does not generate SQL and does not decide policy. It reads Iceberg
metadata, diffs it against YAML contracts humans wrote, applies deterministic
rules to decide severity, records everything it saw to `ops.*` tables, and
takes one bounded action for actionable findings: write an incident file and
(only with `--execute`) file a GitHub issue. The graph:

```
sense ──> monitor ──> classify ──> persist ──┬──> act ──> END
                                              └──> END
```

- **sense** — for each feed layer with `agent_watch: true`, build an
  `ObservedState` from table metadata (schema history with field IDs,
  per-snapshot row counts) plus one column-projected scan for the freshness
  column and any watched enum columns, then diff it against the contract.
- **monitor** — every monitor declared in `feeds/*.yaml`, judged against its
  own persisted history; every arrival SLA; only `breach` becomes a finding.
- **classify** — rules first (below); an LLM is consulted only for a finding no
  rule matches, and only when `ANTHROPIC_API_KEY` is set.
- **persist** — one row in `ops.agent_runs` on every path, findings or not, so
  "clean" and "never ran" are distinguishable; one row per finding in
  `ops.finding_log`, keyed on a stable `finding_key`.
- **act** — incident file with a stable slug (a recurring finding updates one
  file rather than spawning many) and a dry-run `gh issue create`.

### Severity is a vocabulary, not a number

| Severity | What it means | What happens |
| --- | --- | --- |
| `breaking` | data may be lost, a cast will fail, a batch collapsed, data is stale, a monitor breached | incident + (opt-in) issue |
| `renaming` | a column was renamed; **the field ID is unchanged** so no data moved, but the published contract now names a column that does not exist | incident + (opt-in) issue: needs a recorded decision, not a rollback |
| `widening` | `int -> long` and friends; every existing value still fits | recorded, reported |
| `additive` | a new column | recorded, reported |
| `enum_drift` | a new value in a watched categorical column | recorded, reported, **never blocks** |
| `benign` | nothing to do | recorded |

**Renames are paired by Iceberg field ID.** Iceberg's whole claim is that a
rename is metadata-only because the ID does not move. An agent that reads
names only sees a breaking drop plus an unrelated add and sends a human
looking for data that was never lost. `sensors.build_rename_map` groups every
historical name by field ID across the table's schema history, so `a -> b -> c`
resolves in one hop and a watch declared on the old name keeps reading the same
field after the rename instead of going blind.

**Volume is judged per snapshot on the append-only table.** Cumulative
`count(*)` cannot see a collapse on a table that only grows: five batches of
~490 rows followed by a batch of 5 still reads "~2,000 vs baseline". The
`raw_impressions_row_count` monitor uses `source: snapshot_added` and reads the
rows added by the latest non-rebuild snapshot from the manifest summary. The
aggregated table is fully rebuilt every run, so there `count(*)` is the right
signal and per-snapshot added-records would read every rebuild as a spike;
`snapshot_details()` marks rebuilds with `is_full_rebuild` so the two are never
confused.

**Enum drift is not a gate.** `accepted_values` in a contract is fail-closed:
it stops the build, which is right for `page_type` (the funnel models and the
partition layout branch on it). `enum_watch` is the opposite: upstream adding
an event stage breaks nothing today, but every funnel that groups on
`event_type` now has a bucket nobody declared. So it is recorded and reported,
and a watch that *cannot* read its column is itself `breaking` — a check that
displays green while measuring nothing is a defect, not a pass.

---

## Layout

```
feeds/impressions.yaml          the feed: layers, contracts, agent watch, monitors, arrival SLA
contracts/raw_impressions.yaml  schema in Iceberg types, fail-closed expectations, enum watches
contracts/aggregated_impressions.yaml
ops_agent/
  tables.py      every table by name (DDL + Arrow schema); feeds are validated against it
  engine.py      LakehouseEngine protocol + SparkEngine over iceberg_deployment's catalog
  contracts.py   contract loader and the fail-closed validator (gates the aggregated build)
  feeds.py       feed loader; every config error is loud and names the key
  monitors.py    monitor shape and verdict rules (ratio vs median, robust-z via MAD, absolute)
  runner.py      run every monitor, persist to ops.monitor_results, load baselines, route alerts
  arrival.py     "did it arrive at all": lag, calendar gaps, thin periods
  alerts.py      warn/breach -> alerts, deduped and throttled across runs via ops.alert_log
  sensors.py     ObservedState from metadata; detect(); rename pairing; enum drift
  classifier.py  the severity rules; optional LLM fallback (claude-opus-5) behind an API key
  actions.py     incident files with stable slugs; dry-run `gh issue create`
  graph.py       the LangGraph graph and CLI
  report.py      daily report: reads ops.*, diffs day over day, never recomputes
  lakehouse.py   seed the raw table; contract-gated rebuild of the aggregated table
  demo.py        the printed walkthrough below
tests/
  fake_engine.py in-memory engine: Arrow tables + DuckDB SQL, Iceberg-shaped snapshot metadata
  test_*.py      rules on the fake engine; test_spark_integration.py on real Iceberg tables
incidents/       written by the agent (the demo's four are committed as evidence)
reports/         written by the report (the demo's two days are committed)
```

Two tables carry the story. `db.impressions` is the append-only event table
`iceberg_deployment` owns (its DDL is imported, not copied). `db.impressions_aggregated`
is one row per impression, rebuilt from it on every run and **gated by its
contract before the overwrite lands** — validate, then publish — so a build
that violates the contract leaves the previous good table in place
(`test_aggregated_build_is_contract_gated`).

**One engine protocol, two implementations.** Production is Spark 3.5 over the
Iceberg catalog `iceberg_deployment` configures (local filesystem or REST on
MinIO, chosen by `ICEBERG_CATALOG_TYPE`). The fast tests run the same rules and
the same monitor SQL through an in-memory engine backed by DuckDB. That pair
caught a real dialect difference before it reached production: Spark types
`sum(...) * 1.0 / count(*)` as `DECIMAL(38,16)`, DuckDB as `DOUBLE`; the engine
now maps decimals and the ratio monitors cast explicitly.

---

## The demo, as it ran (2026-09-22)

`./deploy.sh demo` against a fresh local warehouse. Three seed batches, the
aggregated build, three monitor runs for baselines, a clean agent run; then
three metadata-only schema changes and one collapsed batch carrying an
unregistered `event_type`; then the monitors, the agent and the report one
logical day later.

```console
=== 1. seed three batches, build the aggregated table
  batch 1: appended 486 rows (snapshot 1)
  batch 2: appended 492 rows (snapshot 2)
  batch 3: appended 486 rows (snapshot 3)
  aggregated: 600 impressions, contract-gated before publish
  logical date (newest event): 2026-06-04

=== 2. monitors x3 for baselines, then a clean agent run
  [ ] aggregated_duplicate_impressions: 0 duplicates
  [ ] aggregated_mean_funnel_depth: 2.44 matches constant baseline 2.44
  [ ] aggregated_page_type_3_share: 0.33 matches constant baseline 0.33
  [ ] aggregated_row_count: 600 vs median 600
  [ ] aggregated_user_cardinality: 50 vs median 50
  [ ] db.impressions_aggregated_type_event_date: declared date, observed date32[day]
  ... (eight column-type checks, one per contract field)
  [ ] raw_impressions_null_impression_id_rate: 0 matches constant baseline 0
  [ ] raw_impressions_row_count: 486 vs median 486
  alerts routed: 0
  as_of=2026-06-04 findings=0
  report -> reports/daily-2026-06-04.md

=== 3. evolve the schema and inject drift (no data file rewritten)
  v2: ADD COLUMN campaign_id STRING
  v3: ALTER COLUMN page_type TYPE BIGINT
  v4: RENAME COLUMN event_type TO event_name (field id 5 -> 5)
  appended a collapsed batch: 5 rows, event_name='g'
  data files now: 37; snapshots: 4

=== 4. monitors + agent one day later (2026-06-05), then the report
  [X] raw_impressions_row_count: 5 is 1% of trailing median 486
  [ ] aggregated_row_count: 600 vs median 600
  ... (every other monitor unchanged)
  -> DRY-RUN: BREACH -> [breach] raw_impressions_row_count on db.impressions
  as_of=2026-06-05 findings=7
  [widening] column 'page_type' type changed: int -> long
  [renaming] column 'event_type' was renamed to 'event_name' (field id 5); the contract still declares the old name
  [additive] column 'campaign_id' present in table but not declared in contract
  [breaking] latest snapshot added 5 rows, below 50% of trailing median 486
  [enum_drift] 1 new value(s) in 'event_type': g
  [breaking] [raw_impressions_row_count] 5 is 1% of trailing median 486
  [breaking] [db.impressions_aggregated_arrival_gap] 1 missing period(s): 2026-06-05
  -> incident: db-impressions-event_type-ff47aef1.md
  -> DRY-RUN: would run `gh issue create ...` -> [renaming] schema_drift on db.impressions
  -> incident: db-impressions-volume_anomaly-74e3b3c1.md
  -> DRY-RUN: would run `gh issue create ...` -> [breaking] volume_anomaly on db.impressions
  -> incident: db-impressions-raw_impressions_row_count-77a420c5.md
  -> DRY-RUN: would run `gh issue create ...` -> [breaking] monitor_breach on db.impressions
  -> incident: db-impressions_aggregated-event_date-3adf78fb.md
  -> DRY-RUN: would run `gh issue create ...` -> [breaking] monitor_breach on db.impressions_aggregated
  report -> reports/daily-2026-06-05.md (diffed against 2026-06-04)
```

Reading the seven findings:

- **The rename is one finding**, classified `renaming`, and the enum watch
  declared on `event_type` still read the renamed column (that is how it saw
  `g`). Field id 5 before and after; no data file was rewritten — 37 data
  files were 37 data files before the `ALTER`s.
- **The widening is `widening`**, not breaking: every existing `page_type`
  value fits in a `long`.
- **The collapse shows up three ways** — the sensor's per-snapshot check
  (5 rows against a median of 486), the persisted-baseline monitor
  (`raw_impressions_row_count`, breach at 1 % of median, routed as one dry-run
  alert), and the arrival SLA on the aggregated table, which has not been
  rebuilt for the new day. Three checks, three mechanisms, one incident each.
- **The two reports** are committed: [`daily-2026-06-04.md`](reports/daily-2026-06-04.md)
  is `CLEAN`; [`daily-2026-06-05.md`](reports/daily-2026-06-05.md) is
  `ATTENTION REQUIRED` and its diff section reads *7 new, 0 cleared, 0 still
  open* against the clean day. The report reads `ops.*`; it computes nothing.

---

## Running it against your own warehouse

```bash
./deploy.sh seed 200        # append one batch to db.impressions (idempotent ids per batch)
./deploy.sh build           # rebuild db.impressions_aggregated, contract-gated
./deploy.sh monitor         # run + persist monitors, route alerts (dry-run; --execute to mark delivered)
./deploy.sh arrival         # the arrival SLA checks alone
./deploy.sh agent           # dry-run; --execute files GitHub issues through `gh`
./deploy.sh report          # reports/daily-<as_of>.md
AS_OF_DATE=2026-06-09 ./deploy.sh agent   # reproduce a stale feed
ICEBERG_CATALOG_TYPE=rest ./deploy.sh agent   # the REST catalog from ../iceberg_deployment
```

The logical date is the newest `event_ts` in `db.impressions`, never the wall
clock: the data is generated for fixed dates, so a wall-clock freshness check
would fail permanently and get muted, which is how freshness monitoring dies.

Adding a feed is one file in `feeds/` (and its contracts). Every table a feed
names must exist in `ops_agent/tables.py`; a typo is a load-time
`FeedConfigError` that names the file, the key and the valid choices, not a
"table not found" swallowed into a breach at run time. An empty `feeds/`
directory is also an error: zero checks reporting clean is the failure this
module exists to prevent.

## What was deliberately not ported

The source project also carried its own PyIceberg lakehouse build, its own
quality utilities and a stock-return forecast. The first two duplicate what
[`iceberg_deployment/`](../iceberg_deployment/) and
[`spark_applications/`](../spark_applications/) already do with more
coverage; the forecast was off-theme for this repository. The `trading`
calendar went with it — impressions arrive daily.
