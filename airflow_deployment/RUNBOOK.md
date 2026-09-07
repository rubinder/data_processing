# Reprocessing runbook — impression pipeline

Operational procedures for `impression_pipeline` (hourly `api_pull` x3
page_types -> `aggregation`) and the dataset-triggered
`impression_quality_checks`. Everything here relies on three properties of the
Spark jobs, so check them before trusting any step:

| property | where it lives | what it buys you |
| --- | --- | --- |
| deterministic `job_id = impression_<page_type>_<date>_<hour>` | `api_pull.py` | a re-run targets exactly one status record and one raw partition |
| dynamic partition overwrite | `utils/storage.py`, `utils/session.py` | re-writing an hour replaces only that hour's partition |
| staged raw landing + `_manifest.json` | `utils/landing.py` | a failed re-run cannot orphan a raw file or leave a stale manifest; the manifest is rewritten on success |

Re-running any hour any number of times is therefore safe. The cost is API
calls to the web server and Spark time, not correctness.

All commands run inside the scheduler container:

```bash
cd airflow_deployment
docker compose exec airflow-scheduler bash
```

## 1. Reprocess one hour

Use when: a single run failed and was not retried, a source fix landed for one
hour, or a quality check flagged one hour.

```bash
# Clear the whole run for 2026-03-04 13:00 (pull + aggregation). The cleared
# tasks re-queue immediately; downstream is included so aggregation re-runs
# on the re-pulled data.
airflow tasks clear impression_pipeline \
  --start-date 2026-03-04T13:00:00 --end-date 2026-03-04T13:00:00 \
  --downstream --yes
```

What happens: three `api_pull` runs re-fetch page_types 1-3 for that hour,
stage the raw file, overwrite `processed/impressions/page_type=*/date=.../hour=13`,
promote the raw file and rewrite the manifest; `aggregation` then overwrites
`output/impressions_aggregated` for that hour. Status files flip
`in_progress -> completed` again. The aggregated dataset event fires once,
which triggers exactly one `impression_quality_checks` run for that hour
(section 6).

UI equivalent: Grid view -> the run -> "Clear" with *Downstream* ticked.

## 2. Reprocess a range

Use when: an upstream outage spanned several hours, or the pipeline was paused.

Two options, with different semantics:

```bash
# (a) Clear existing runs in a window. Keeps the DagRun rows, re-queues
#     tasks. Best when the runs exist and just need re-executing.
airflow tasks clear impression_pipeline \
  --start-date 2026-03-04T00:00:00 --end-date 2026-03-04T23:00:00 \
  --downstream --yes

# (b) Backfill: creates runs that never existed (e.g. the DAG was paused or
#     start_date moved back). --reset-dagruns also re-runs existing ones.
airflow dags backfill impression_pipeline \
  --start-date 2026-03-04 --end-date 2026-03-05 \
  --reset-dagruns --yes
```

Throttle: `max_active_runs=3` in the DAG bounds how many hours run at once.
Each hour makes three API requests to the web server, so a 24-hour backfill
is 72 pulls; raise `max_active_runs` only if the web server and the Spark
cluster have headroom. The pulls retry with exponential backoff
(`fetch_impression_data`), so transient API errors during a backfill heal
themselves.

`catchup=True` means simply un-pausing the DAG after an outage also
backfills the gap. Prefer that over a manual backfill when the gap is recent.

## 3. Reprocess one page_type

Use when: only one page_type's API response was bad.

```bash
# Mapped task instances are addressed by map index (0,1,2 = page_type 1,2,3).
airflow tasks clear impression_pipeline \
  --start-date 2026-03-04T13:00:00 --end-date 2026-03-04T13:00:00 \
  --task-regex '^pull_impressions$' --downstream --yes
```

Then in the UI, clear only the failed map index if you do not want all three
re-pulled (CLI clear operates on the whole mapped task). `aggregation` is
included via `--downstream` because it reads all three page_types.

## 4. Quarantine ratio spike

Symptom: `api_pull` fails with
`quarantine ratio X% (...) exceeds the 1.00% threshold`, or
`impression_quality_checks.check_volume` fails with the same message from the
manifest.

1. Inspect the rejects. Locally:
   `data/quarantine/impressions/impression_<pt>_<date>_<hour>/` (parquet, one
   directory per job_id; on AWS `s3://<bucket>/quarantine/...`). The
   `_corrupt_record` column holds the raw CSV line for unparseable rows; rows
   with a null required column parsed but broke the contract.
2. Decide:
   - **Source changed shape** (new column, renamed header, quoting change):
     fix the contract in `utils/schema.py` + the web server contract, deploy,
     then re-run the hour (section 1). Do not raise the threshold to make it
     pass.
   - **Genuinely bad rows from the source** (a small burst): if the share is
     tolerable for that hour, re-run with `MAX_QUARANTINE_RATIO` raised *for
     that run only* (set it in the SparkSubmitOperator env or `.env`, then
     revert). The good rows land; the bad rows stay in quarantine for the
     source owner.
3. The landing was aborted, so there is no raw file and no manifest for the
   hour, and the status file says `failed`. Nothing to clean up before the
   re-run.

## 5. Freshness / SLA miss

Symptom: `check_freshness` fails (`newest completed run ... is H:MM old, SLA is
90 min`), or Airflow's SLA miss callback fires for `aggregate_impressions`.

1. Is the scheduler alive? `docker compose ps`, `airflow jobs check
   --job-type SchedulerJob`.
2. Is the DAG paused? `airflow dags list` -> `paused`. Un-pausing triggers
   catch-up.
3. Are runs failing? Grid view. A failing `api_pull` for one page_type blocks
   `aggregation` for that hour; fix per section 4 or the task log.
4. Is the web server up? `curl "$API_BASE_URL/impression?page_type=1&date=...&hour=..."`.
5. If the pipeline is healthy but slow, the SLA itself may be wrong: 90 min
   assumes hourly runs finish well within the hour. Adjust
   `FRESHNESS_SLA_MINUTES` in `.env` deliberately, not to silence a page.

## 6. Datasets after a reprocess

`aggregate_impressions` has `outlets=[IMPRESSIONS_AGGREGATED]`; every
successful run emits one dataset event, and `impression_quality_checks`
(`schedule=[IMPRESSIONS_AGGREGATED]`) runs once per event.

- Clearing one hour -> one event -> one quality-check run for that hour. The
  check resolves the (date, hour) from the triggering run's logical date, so
  it checks the reprocessed hour, not "now".
- Backfilling 24 hours -> 24 events -> 24 quality-check runs, serialised by
  `max_active_runs=1`. That is the intended behaviour; do not pause the
  consumer during a backfill or the events queue up and fire at once when
  un-paused (Airflow 2.x consumes all pending events into a single run in
  that case, and the check then only sees the newest partition).
- Clearing only `pull_impressions` (not downstream) emits events on
  `IMPRESSIONS_RAW`/`IMPRESSIONS_PROCESSED` but not on the aggregated
  dataset, so the checks do not run. Use `--downstream`.
- Manual trigger of the checks for an arbitrary hour:
  `airflow dags trigger impression_quality_checks --conf '{"date": "2026-03-04", "hour": 13}'`.

## 7. Verification checklist after a reprocess

```bash
# status files: every job_id for the hour says completed
cat data/status/impression_*_2026-03-04_13.json

# manifests: rewritten, counts reconcile, committed_at is recent
cat data/raw/impressions/page_type=*/date=2026-03-04/hour=13/_manifest.json
#   rows_read == rows_written + rows_quarantined
#   raw_sha256 changes only if the API returned different bytes

# no leftovers under staging (a crashed run would leave its job_id here)
ls data/raw/impressions/_staging/ 2>/dev/null

# quality checks ran for the hour and passed
airflow dags list-runs -d impression_quality_checks --state success | head
```

On AWS, the same files live under `s3://<landing-bucket>/raw/...` and status in
the DynamoDB `StatusTrackingTable`; the `impression_quality_checks` tasks skip
in `SPARK_MODE=aws` (they read the local layout), so verify with the S3/Dynamo
console or extend the checks with boto3 readers.
