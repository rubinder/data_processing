"""DAG: Impression pipeline (hourly, backfillable).

Runs the real Spark jobs end to end for one (date, hour):

    api_pull (one mapped task per page_type)  ->  aggregation

Staff-level orchestration concerns this DAG demonstrates, in contrast to the
hello-world DAGs:

- **Backfills.** ``schedule="@hourly"`` + ``catchup=True`` means a cleared run
  or a new start_date replays every missing hour. ``max_active_runs`` bounds
  how many hours backfill concurrently so a replay doesn't overwhelm the
  cluster.
- **Idempotency.** Each run derives its ``--date``/``--hour`` from the logical
  date, and the Spark jobs use dynamic partition overwrite (see
  spark_applications/DECISIONS.md #2). Re-running an hour replaces exactly that
  hour's partitions, so retries and backfills are safe to repeat.
- **Failure handling.** Retries with exponential backoff, a per-task SLA, and a
  failure callback hook for alerting.
- **Fan-out.** ``api_pull`` is dynamically mapped over page_types instead of
  three copy-pasted tasks.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

PAGE_TYPES = ["1", "2", "3"]

# Deployed location of the packaged spark_applications jobs inside the image.
APP_ROOT = os.environ.get(
    "SPARK_APP_ROOT", "/opt/airflow/dags/spark_scripts"
)
API_PULL_APP = f"{APP_ROOT}/api_pull.py"
AGGREGATION_APP = f"{APP_ROOT}/aggregation.py"
SPARK_MODE = os.environ.get("SPARK_MODE", "local")


def _alert_on_failure(context: dict) -> None:
    """Failure callback hook — wire to Slack/PagerDuty in production."""
    ti = context.get("task_instance")
    print(
        f"ALERT: task {ti.task_id if ti else '?'} failed for run "
        f"{context.get('logical_date')}"
    )


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "sla": timedelta(minutes=30),
    "on_failure_callback": _alert_on_failure,
    "depends_on_past": False,
}

with DAG(
    dag_id="impression_pipeline",
    description="Hourly, backfillable impression pull + aggregation",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=True,
    max_active_runs=3,
    default_args=default_args,
    tags=["spark", "impressions", "etl"],
) as dag:

    # {{ ds }} -> YYYY-MM-DD, {{ logical_date }} -> hour of the scheduled run.
    pull_impressions = SparkSubmitOperator.partial(
        task_id="pull_impressions",
        conn_id="spark_local",
        application=API_PULL_APP,
        name="api_pull",
    ).expand(
        application_args=[
            [
                "--mode", SPARK_MODE,
                "--page_type", page_type,
                "--date", "{{ ds }}",
                "--hour", "{{ logical_date.strftime('%H') }}",
            ]
            for page_type in PAGE_TYPES
        ]
    )

    aggregate_impressions = SparkSubmitOperator(
        task_id="aggregate_impressions",
        conn_id="spark_local",
        application=AGGREGATION_APP,
        name="aggregation",
        application_args=[
            "--mode", SPARK_MODE,
            "--date", "{{ ds }}",
            "--hour", "{{ logical_date.strftime('%H') }}",
        ],
    )

    pull_impressions >> aggregate_impressions
