"""DAG: Impression quality checks (dataset-triggered).

``schedule=[IMPRESSIONS_AGGREGATED]``: this DAG has no clock. It runs once
each time ``impression_pipeline.aggregate_impressions`` succeeds and emits an
event on the aggregated dataset, so the checks see data that has actually
landed rather than data that a cron offset hoped had landed. A backfill of one
hour emits one event for that hour and triggers exactly one check run for it;
``catchup`` is meaningless for dataset schedules and is left False.

Two independent checks, both pure functions in ``quality_checks.py``:

* ``check_freshness`` - newest ``completed`` status file must be younger than
  ``FRESHNESS_SLA_MINUTES``. Guards against the "no news" failure mode that
  datasets cannot express (a producer that never succeeds never emits).
* ``check_volume`` - for each page_type, this hour's ``rows_written`` against
  the median of the same hour on the previous ``BASELINE_DAYS`` days, plus a
  ``rows_quarantined / rows_read`` ceiling. Missing baselines warn; a missing
  current manifest fails.

Partition resolution: Airflow 2.x datasets are table-level URIs with no
partition information, so the checked (date, hour) comes from the logical
date of the *producer* run attached to the triggering dataset event
(``context["triggering_dataset_events"][uri][i].source_dag_run``). A manual
trigger has no events; it then honours ``params.date``/``params.hour`` or,
failing that, the current UTC hour.

The checks read the local storage layout only (``PIPELINE_DATA_DIR``, shared
with the Spark driver via a bind mount, see docker-compose.yaml). In
``SPARK_MODE=aws`` status lives in DynamoDB and manifests on S3; the tasks
skip with a message rather than pretend.
"""

import json
import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from datasets import IMPRESSIONS_AGGREGATED
from quality_checks import (
    baseline_manifest_paths,
    check_freshness,
    check_volume,
    manifest_path,
    resolve_partition,
    status_dir,
)

PAGE_TYPES = ["1", "2", "3"]
SPARK_MODE = os.environ.get("SPARK_MODE", "local")
DATA_DIR = os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data")
FRESHNESS_SLA_MINUTES = int(os.environ.get("FRESHNESS_SLA_MINUTES", "90"))
BASELINE_DAYS = int(os.environ.get("VOLUME_BASELINE_DAYS", "7"))
MIN_RATIO = float(os.environ.get("VOLUME_MIN_RATIO", "0.5"))
MAX_RATIO = float(os.environ.get("VOLUME_MAX_RATIO", "2.0"))
MAX_QUARANTINE_RATIO = float(
    os.environ.get("MAX_QUARANTINE_RATIO", "0.01")
)


def _log(event: str, **fields) -> None:
    """Single-line JSON, same shape as spark_applications observability."""
    print(json.dumps({"event": event, **fields}, default=str))


def _require_local_mode() -> None:
    if SPARK_MODE != "local":
        raise AirflowSkipException(
            f"quality checks read the local data layout; SPARK_MODE="
            f"{SPARK_MODE} keeps status in DynamoDB and manifests on S3"
        )


def _target_partition(context: dict) -> tuple[str, str]:
    """(date, hour) from params, else triggering events, else now."""
    params = context.get("params") or {}
    if params.get("date") and params.get("hour") not in (None, ""):
        return str(params["date"]), f"{int(params['hour']):02d}"

    logical_dates = []
    events = context.get("triggering_dataset_events") or {}
    for uri_events in events.values():
        for event in uri_events:
            run = getattr(event, "source_dag_run", None)
            if run is not None and run.logical_date is not None:
                logical_dates.append(run.logical_date)
    return resolve_partition(logical_dates, now=datetime.now(timezone.utc))


def run_freshness_check(**context) -> float:
    _require_local_mode()
    age = check_freshness(status_dir(DATA_DIR), FRESHNESS_SLA_MINUTES)
    _log(
        "freshness_ok", age_seconds=age.total_seconds(),
        sla_minutes=FRESHNESS_SLA_MINUTES,
    )
    return age.total_seconds()


def run_volume_check(**context) -> dict:
    _require_local_mode()
    date, hour = _target_partition(context)
    _log("volume_check_start", date=date, hour=hour)
    results = {}
    for page_type in PAGE_TYPES:
        result = check_volume(
            manifest_path(DATA_DIR, page_type, date, hour),
            baseline_manifest_paths(
                DATA_DIR, page_type, date, hour, days=BASELINE_DAYS
            ),
            min_ratio=MIN_RATIO,
            max_ratio=MAX_RATIO,
            max_quarantine_ratio=MAX_QUARANTINE_RATIO,
        )
        for warning in result.warnings:
            _log("volume_warning", page_type=page_type, message=warning)
        _log("volume_ok", page_type=page_type, **result.as_dict())
        results[page_type] = result.as_dict()
    return results


with DAG(
    dag_id="impression_quality_checks",
    description="Freshness + volume checks, triggered by the aggregated "
                "impressions dataset",
    schedule=[IMPRESSIONS_AGGREGATED],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 0},
    params={"date": "", "hour": ""},
    tags=["impressions", "quality", "datasets"],
) as dag:

    freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=run_freshness_check,
    )

    volume = PythonOperator(
        task_id="check_volume",
        python_callable=run_volume_check,
    )

    # Independent: a stale pipeline and a volume anomaly are different
    # diagnoses and should surface separately.
    [freshness, volume]
