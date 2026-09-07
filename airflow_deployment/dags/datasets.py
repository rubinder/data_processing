"""Shared Airflow ``Dataset`` definitions for the impression pipeline.

Why datasets (data-aware scheduling) instead of a second clock
---------------------------------------------------------------
The natural way to run "quality checks after the hourly pipeline" is another
``@hourly`` DAG offset by some guessed delay. That couples two schedules by
wall-clock luck: if the pipeline runs late the checks read stale data, if it
runs early the checks wait for nothing. With datasets the producer task
declares ``outlets=[IMPRESSIONS_AGGREGATED]`` and the consumer DAG declares
``schedule=[IMPRESSIONS_AGGREGATED]``; the consumer runs when the data has
actually landed, once per producer success, and not otherwise.

The second win is backfills. Clearing one hour of ``impression_pipeline``
re-runs ``aggregate_impressions`` for that hour, which emits one dataset event
carrying that run's logical date, which triggers exactly one consumer run for
that hour. No consumer-side backfill bookkeeping is needed.

Limits in Airflow 2.x
---------------------
* A dataset is identified only by its URI. There is no partition-awareness:
  the URI is table-level (``.../processed/impressions``, not
  ``.../hour=05``) and the *logical date on the triggering run* carries the
  partition. Consumers must read ``triggering_dataset_events`` to find it.
* Every mapped task instance that lists a dataset as an outlet emits an event
  on success, so a task mapped over three page_types produces three events per
  run. Subscribe to the single-instance task's dataset (aggregated) when you
  want one consumer run per pipeline run.
* Events are emitted on task *success* only; a failed producer never triggers
  the consumer, which is what you want for quality checks but means "no news"
  must be caught by a freshness check, not by the dataset machinery.
* Airflow 3 renames this to ``Asset`` and adds partitioning/watchers. Keeping
  all URIs in this one module means that migration touches one file.

The URIs are derived from the environment so the same DAG code points at the
local shared volume or at S3:

* ``SPARK_MODE=local`` (default): ``file://<PIPELINE_DATA_DIR>/<layer>/<table>``
* ``SPARK_MODE=aws``: ``s3://<S3_BUCKET>/<layer>/<table>``

These match the OpenLineage dataset naming the Spark listener produces for the
same paths (namespace ``s3://bucket`` / ``file``, name ``/layer/table``), so
the Airflow datasets and the Marquez lineage graph line up.
"""

import os

from airflow.datasets import Dataset

SPARK_MODE = os.environ.get("SPARK_MODE", "local")
PIPELINE_DATA_DIR = os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data")
S3_BUCKET = (
    os.environ.get("S3_BUCKET")
    or os.environ.get("S3_LANDING_BUCKET")
    or "unset-bucket"
)


def dataset_uri(layer: str, table: str) -> str:
    """Build the URI for ``<layer>/<table>`` in the active storage mode.

    ``layer`` is one of the LocalStorageAdapter/AwsStorageAdapter prefixes
    (``raw``, ``processed``, ``output``, ``quarantine``, ``status``).
    """
    if SPARK_MODE == "aws":
        return f"s3://{S3_BUCKET}/{layer}/{table}"
    return f"file://{PIPELINE_DATA_DIR}/{layer}/{table}"


# Raw gzip CSV as landed by api_pull, one directory per (page_type, date,
# hour), each with data.csv.gz and _manifest.json.
IMPRESSIONS_RAW = Dataset(dataset_uri("raw", "impressions"))

# Contract-conforming rows, parquet partitioned by page_type/date/hour
# (api_pull's write_partitioned).
IMPRESSIONS_PROCESSED = Dataset(dataset_uri("processed", "impressions"))

# Hourly aggregates written by aggregation.py (write_output).
IMPRESSIONS_AGGREGATED = Dataset(dataset_uri("output", "impressions_aggregated"))
