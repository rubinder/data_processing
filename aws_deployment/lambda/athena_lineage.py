"""Lambda: run an Athena verification query and emit its OpenLineage events.

Athena has no OpenLineage integration of its own, so this is the pipeline's
last hop made visible. Invoked by the Step Function after the processed table
has been recrawled, it:

1. derives the (page_type, date, hour) partition from the landed S3 key;
2. runs ``SELECT count(*) ...`` on the ``processed`` table for that partition
   through the workgroup (so the bytes-scanned cutoff applies);
3. emits an OpenLineage START + COMPLETE (or FAIL) run event naming the Glue
   table as input and the Athena result object as output, with the row count
   and ``DataScannedInBytes`` as run facets;
4. returns the count so the Step Function output carries it.

Stdlib only: the OpenLineage RunEvent is plain JSON over HTTP, so the Lambda
needs no packaged dependencies. When ``OPENLINEAGE_URL`` is unset the events
are logged instead of posted, so the query still runs and the shape of what
would be sent is visible in CloudWatch.
"""

import json
import os
import re
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

import boto3

PRODUCER = "https://github.com/rubinder/data_processing/aws_deployment/lambda/athena_lineage.py"
SCHEMA_URL = (
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
)
PARTITION_RE = re.compile(
    r"page_type=(?P<page_type>[^/]+)/date=(?P<date>\d{4}-\d{2}-\d{2})"
    r"/hour=(?P<hour>\d{1,2})/"
)


def partition_from_key(key: str) -> dict:
    """``raw/impressions/page_type=1/date=2026-09-06/hour=10/data.csv.gz``
    -> ``{"page_type": "1", "date": "2026-09-06", "hour": "10"}``."""
    match = PARTITION_RE.search(key)
    if not match:
        raise ValueError(f"key carries no page_type/date/hour partition: {key}")
    parts = match.groupdict()
    # All three stay strings: the crawler registers Hive-style partition keys
    # as string columns, and Athena rejects `hour = 10` against a varchar
    # partition with TYPE_MISMATCH (seen on the first live run).
    return {
        "page_type": parts["page_type"],
        "date": parts["date"],
        "hour": parts["hour"],
    }


def count_query(database: str, table: str, partition: dict) -> str:
    """Partition-pruned count; the filters are on partition columns only so
    Athena answers from metadata and scans ~0 bytes. Partition keys are
    strings in the catalog, so every comparison is quoted."""
    return (
        f'SELECT count(*) AS rows FROM "{database}"."{table}" '
        f"WHERE page_type = '{partition['page_type']}' "
        f"AND date = '{partition['date']}' "
        f"AND hour = '{partition['hour']}'"
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_event(
    event_type: str,
    run_id: str,
    job_name: str,
    namespace: str,
    inputs: list,
    outputs: list,
    run_facets: dict | None = None,
    parent: dict | None = None,
) -> dict:
    """Build an OpenLineage RunEvent (spec 2.x) as a plain dict."""
    run = {"runId": run_id, "facets": dict(run_facets or {})}
    if parent:
        run["facets"]["parent"] = {
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json",
            "run": {"runId": parent["runId"]},
            "job": {"namespace": namespace, "name": parent["jobName"]},
        }
    return {
        "eventType": event_type,
        "eventTime": _now(),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": run,
        "job": {"namespace": namespace, "name": job_name},
        "inputs": inputs,
        "outputs": outputs,
    }


def athena_dataset(region: str, database: str, table: str) -> dict:
    """Input dataset named the way the Glue/Spark listener names catalog
    tables, so Marquez joins this run to the Glue ETL's output."""
    return {
        "namespace": f"awsathena://athena.{region}.amazonaws.com",
        "name": f"{database}.{table}",
    }


def s3_dataset(s3_uri: str) -> dict:
    bucket, _, key = s3_uri.removeprefix("s3://").partition("/")
    return {"namespace": f"s3://{bucket}", "name": f"/{key}"}


def emit(url: str | None, event: dict, timeout: float = 5.0) -> str:
    """POST the event to ``<url>/api/v1/lineage``; log it when no URL.

    Lineage must never fail the pipeline: any transport error is logged and
    swallowed, matching the Spark listener's behaviour.
    """
    if not url:
        print(json.dumps({"lineage": "not posted (OPENLINEAGE_URL unset)",
                          "event": event}, default=str))
        return "logged"
    request = urllib.request.Request(
        f"{url.rstrip('/')}/api/v1/lineage",
        data=json.dumps(event, default=str).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return f"posted ({response.status})"
    except (urllib.error.URLError, TimeoutError) as exc:
        print(json.dumps({"lineage": "post failed", "error": str(exc)}))
        return "failed"


def run_athena(athena, query: str, database: str, workgroup: str,
               poll_seconds: float = 1.0, max_wait: float = 120.0) -> dict:
    """Start the query, wait for it, return the execution description."""
    execution_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )["QueryExecutionId"]
    waited = 0.0
    while True:
        execution = athena.get_query_execution(QueryExecutionId=execution_id)
        state = execution["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return execution["QueryExecution"]
        if waited >= max_wait:
            raise TimeoutError(f"Athena query {execution_id} still {state}")
        time.sleep(poll_seconds)
        waited += poll_seconds


def first_cell(athena, execution_id: str) -> int:
    rows = athena.get_query_results(QueryExecutionId=execution_id)["ResultSet"]["Rows"]
    # Row 0 is the header.
    return int(rows[1]["Data"][0]["VarCharValue"])


def lambda_handler(event, context):
    region = os.environ.get("AWS_REGION", "us-east-1")
    database = os.environ["GLUE_DATABASE"]
    table = os.environ.get("PROCESSED_TABLE", "processed")
    workgroup = os.environ["ATHENA_WORKGROUP"]
    namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "data_processing")
    url = os.environ.get("OPENLINEAGE_URL") or None
    job_name = f"athena.{table}_partition_count"

    partition = partition_from_key(event["key"])
    query = count_query(database, table, partition)
    run_id = str(uuid.uuid4())
    inputs = [athena_dataset(region, database, table)]
    parent = None
    if event.get("execution_name"):
        parent = {"runId": str(uuid.uuid5(uuid.NAMESPACE_URL, event["execution_name"])),
                  "jobName": "step_function.data-processing-pipeline"}
    sql_facet = {"sql": {
        "_producer": PRODUCER,
        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
        "query": query,
    }}

    emit(url, run_event("START", run_id, job_name, namespace, inputs, [],
                        run_facets=sql_facet, parent=parent))

    athena = boto3.client("athena", region_name=region)
    try:
        execution = run_athena(athena, query, database, workgroup)
    except Exception as exc:  # noqa: BLE001 - reported as a FAIL event, then re-raised
        emit(url, run_event("FAIL", run_id, job_name, namespace, inputs, [],
                            run_facets={**sql_facet, "errorMessage": {
                                "_producer": PRODUCER,
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
                                "message": str(exc), "programmingLanguage": "python"}},
                            parent=parent))
        raise

    status = execution["Status"]
    stats = execution.get("Statistics", {})
    output_uri = execution["ResultConfiguration"]["OutputLocation"]
    if status["State"] != "SUCCEEDED":
        reason = status.get("StateChangeReason", status["State"])
        emit(url, run_event("FAIL", run_id, job_name, namespace, inputs,
                            [s3_dataset(output_uri)],
                            run_facets={**sql_facet, "errorMessage": {
                                "_producer": PRODUCER,
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
                                "message": reason, "programmingLanguage": "sql"}},
                            parent=parent))
        raise RuntimeError(f"Athena query failed: {reason}")

    rows = first_cell(athena, execution["QueryExecutionId"])
    facets = {
        **sql_facet,
        "athena": {
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/BaseFacet.json",
            "queryExecutionId": execution["QueryExecutionId"],
            "dataScannedInBytes": stats.get("DataScannedInBytes", 0),
            "engineExecutionTimeInMillis": stats.get("EngineExecutionTimeInMillis", 0),
            "rows": rows,
            "partition": partition,
        },
    }
    outcome = emit(url, run_event("COMPLETE", run_id, job_name, namespace, inputs,
                                  [s3_dataset(output_uri)], run_facets=facets,
                                  parent=parent))
    result = {
        "partition": partition,
        "rows": rows,
        "queryExecutionId": execution["QueryExecutionId"],
        "dataScannedInBytes": stats.get("DataScannedInBytes", 0),
        "lineage": outcome,
    }
    print(json.dumps({"event": "athena_verified", **result}, default=str))
    return result
