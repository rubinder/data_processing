# Lineage across Spark, S3, Glue, Athena and dbt

OpenLineage is a run-event protocol: a job emits `START` and `COMPLETE`
(or `FAIL`) events naming its **inputs** and **outputs** as datasets
(`namespace` + `name`), and the backend (Marquez here) stitches the events
into a graph. Nothing in the pipeline changes shape; each engine gets a
listener or wrapper that watches what it already does.

Everything is opt-in through one variable so a laptop run pays nothing:

| variable | read by | effect when set |
| --- | --- | --- |
| `OPENLINEAGE_URL` | Spark (`utils/session.py`), Airflow compose, dbt `deploy.sh`, `aws_deployment/scripts/deploy.sh`, Glue/EMR template parameter | events are POSTed to `<url>/api/v1/lineage` |
| `OPENLINEAGE_NAMESPACE` | same | job namespace (default `data_processing`) |
| `OPENLINEAGE_DISABLED` | Airflow only | provider off unless `false` |

Inside Docker the URL is `http://marquez-api:5000`; from the host,
`http://localhost:5005`.

## Layer by layer

### Spark jobs (`api_pull`, `aggregation`)

`utils/session.py::openlineage_conf` adds, only when the URL is set:

```
spark.extraListeners             io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type http
spark.openlineage.transport.url  $OPENLINEAGE_URL
spark.openlineage.namespace      $OPENLINEAGE_NAMESPACE
spark.openlineage.appName        ApiPull | Aggregation
spark.jars.packages             += io.openlineage:openlineage-spark_2.12:1.53.0
```

The listener watches the query-execution events and reports every read and
write as a dataset:

| what the job does | dataset namespace | dataset name |
| --- | --- | --- |
| `read_csv` on the staged raw gzip | `file` or `s3://<bucket>` | `/data/raw/impressions/_staging/<job_id>` |
| `write_partitioned` parquet | `file` / `s3://<bucket>` | `/data/processed/impressions` |
| `write_output` parquet | `file` / `s3://<bucket>` | `/data/output/impressions_aggregated` |
| Delta on Databricks | `dbfs` / the catalog | `<catalog>.<schema>.impressions` |

Jobs are named `<appName>.<action>` (for example
`api_pull.execute_insert_into_hadoop_fs_relation_command`). The listener
attaches a `SchemaDatasetFacet` (columns and types, which is how the contract
in `utils/schema.py` becomes visible per run) and, for parquet/Delta writes,
`OutputStatisticsOutputDatasetFacet` (rows, bytes). On Databricks attach the
jar as a cluster library; `spark.jars.packages` is not honoured on a running
cluster.

The staged-path input is a feature, not a wart: it is exactly the path the
table was built from, and the `_manifest.json` written at commit time carries
the same `job_id`, so a run in Marquez can be matched to a manifest.

### Airflow

`apache-airflow-providers-openlineage` reports each task instance as a run of
job `<dag_id>.<task_id>` in the Airflow namespace. For `SparkSubmitOperator`
the provider injects `spark.openlineage.parentRunId` / `parentJobName`
into the submitted job, so the Spark job's own runs appear *nested under* the
Airflow task run rather than as unrelated jobs. Enable with
`OPENLINEAGE_DISABLED=false` in `airflow_deployment/../.env`.

The `Dataset` URIs in `airflow_deployment/dags/datasets.py` are chosen to
match the listener's naming (`file://<data_dir>/processed/impressions` and
`s3://<bucket>/processed/impressions`), so the Airflow scheduling graph and
the Marquez lineage graph describe the same objects.

### S3 and Glue (AWS path)

S3 needs no emitter: it appears as the dataset namespace `s3://<bucket>` on
every Spark/Glue read and write. The Glue ETL job and the EMR cluster in
`aws_deployment/cloudformation/main.yaml` take an `OpenLineageUrl` parameter;
when set, Glue gets the same listener through `--extra-jars` (the jar is
uploaded to the deployment bucket by `scripts/deploy.sh` because Glue cannot
resolve Maven coordinates) and `--conf`, and EMR through `spark-defaults` with
`spark.jars.packages`. The Glue job then reports
`s3://<landing>/raw/<key>` -> `s3://<processed>/processed/` with the same
facets as the local jobs.

Observed on a live `emr-7.13.0` cluster (2026-09-06): the image already puts
`/usr/share/aws/datazone-openlineage-spark/lib/*` on both
`spark.driver.extraClassPath` and `spark.executor.extraClassPath`. That is
Amazon's OpenLineage build for DataZone/SageMaker lineage, and it defines the
same `io.openlineage.spark.agent.OpenLineageSparkListener` class. Adding
`io.openlineage:openlineage-spark_2.12` via `spark.jars.packages` therefore
puts two versions of the listener on the classpath. Prefer the shipped jar
on EMR: set only the `spark.extraListeners` / `spark.openlineage.*` properties
and drop `spark.jars.packages` (the template's `HasOpenLineage` branch is
where to change that), or pin the Maven version to match the shipped one.

### Athena

There is no Athena integration: Athena is a query service and emits nothing.
Two practical options, in order of preference:

1. **Emit from the orchestrator.** The Step Function / Lambda that runs a
   query knows the SQL, the input tables and the output location. Use the
   `openlineage-python` client to send a `RunEvent` around
   `start_query_execution` / `get_query_execution`:

   ```python
   from openlineage.client import OpenLineageClient
   from openlineage.client.event_v2 import (
       InputDataset, Job, OutputDataset, Run, RunEvent, RunState,
   )
   from openlineage.client.uuid import generate_new_uuid

   client = OpenLineageClient(url=os.environ["OPENLINEAGE_URL"])
   run = Run(runId=str(generate_new_uuid()))
   job = Job(namespace="data_processing", name="athena.impressions_hourly_ctas")
   client.emit(RunEvent(
       eventType=RunState.START, eventTime=now_iso(), run=run, job=job,
       inputs=[InputDataset(namespace="awsathena://athena.us-east-1.amazonaws.com",
                            name="data_processing_db.impressions")],
       outputs=[OutputDataset(namespace="s3://processed-bucket",
                              name="/athena-results/impressions_hourly")],
       producer="aws_deployment/lambda",
   ))
   # ... run the query, poll get_query_execution, then emit COMPLETE or FAIL
   ```

   `data_scanned_in_bytes` from `get_query_execution` fits naturally in a
   custom run facet and is the FinOps number (`aws_deployment/FINOPS.md`).
2. **Use the Glue Data Catalog as the join key.** Athena tables *are* Glue
   Catalog tables, and the Spark/Glue listener names catalog-backed datasets
   by their Glue database and table. Naming the Athena dataset
   `data_processing_db.impressions` in the manual events above makes Marquez
   join the two sides.

### dbt

`openlineage-dbt` provides `dbt-ol`, a wrapper that runs dbt and then reads
`target/manifest.json` and `run_results.json` to emit one run per model, seed,
snapshot and test, with the `ref`/`source` graph as inputs/outputs
(`postgres://dbt-postgres:5432` namespace, `data_processing.raw.impressions`
style names) and column-level lineage where the SQL allows. `dbt-ol` is in the
dbt image; `dbt_deployment/deploy.sh` switches to it whenever
`OPENLINEAGE_URL` is set, so `./deploy.sh run` / `test` / `source-freshness`
all report. The source-freshness result rides along as a dataset facet.

## End to end, locally

```bash
lineage_deployment/deploy.sh up                     # Marquez API on host :5005 (container :5000) / UI :3000
lineage_deployment/deploy.sh smoke                  # 201 START, 201 COMPLETE

export OPENLINEAGE_URL=http://localhost:5005        # host-side Spark
cd spark_applications
uv run python -m spark_applications.api_pull --mode local --page_type 1 --date 2026-01-01 --hour 10
uv run python -m spark_applications.aggregation --mode local --date 2026-01-01 --hour 10

cd ../dbt_deployment
OPENLINEAGE_URL=http://marquez-api:5000 ./deploy.sh run   # container-side URL

# Airflow: set OPENLINEAGE_DISABLED=false in .env, restart, un-pause impression_pipeline
```

Open http://localhost:3000, namespace `data_processing`: the `api_pull` job
writes `processed/impressions`, `aggregation` reads it and writes
`impressions_aggregated`; the dbt jobs hang off `raw.impressions`. With
Airflow on, each Spark run nests under its task.

## Gaps

- **Athena is manual.** Until the query runner emits events, the graph stops
  at the Glue catalog table.
- **Column-level lineage** exists for Spark (built-in facet in the listener
  for DataFrame lineage) and dbt (from compiled SQL), not for the manual
  Athena events unless you populate the facet yourself.
- **Two URLs.** Host-side emitters use `localhost:5005`, container-side
  `marquez-api:5000`; a wrong one fails silently (the listener logs and
  continues, by design, so lineage can never fail a job).
- **Marquez is a dev backend here**: single Postgres, no auth, volume kept
  across `down`. For anything shared, put it behind auth or use a managed
  OpenLineage consumer.
