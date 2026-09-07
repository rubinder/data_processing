# Re-measuring on a real cluster

The debugging cases and `production_metrics.py` run on a laptop in seconds,
which is exactly why their *timings* are not evidence. `local[*]` has no
network, so a shuffle is a memory copy; per-row Python serialization is
dwarfed by JVM start-up; and the fixtures are sized so every partition stays
under `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`, so AQE's
skew split never fires. The *plan shapes* (join operator, exchange count,
partitioning keys, `PartitionFilters`) are exact at any scale and are what
`DEBUGGING.md` relies on. The runtime numbers below are still to be measured.

## What to run

`debugging/cluster_measure.py` is the measurement entry point. It runs the
legs of case 01, case 07 and the production before/after, and for each leg
records wall-clock and, from the driver's Spark REST API, the heaviest
stage's max-vs-median task duration and shuffle-read bytes, disk spill, and
whether AQE split a skewed partition. It prints the markdown table below to
stdout, so on EMR the numbers are in the step's `stdout.gz`.

```bash
python -m spark_applications.debugging.cluster_measure --case 1 --case 7 --production --rows 20000000
```

`run.py` (plan evidence) also takes `--rows`; both need the fixtures to be
generated at a size where the hot partition clears
`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (64MB in
`utils/session.py`). At 2e7 rows with 80% on one key the hot partition is
roughly 1GB of shuffle, which is enough; 2e8 makes the straggler painful
enough to feel.

```bash
# Case 01: skewed join. At 2e8 rows with 80% on one key the hot partition is
# ~10GB; that is the regime where the sort-merge straggler is measured in
# tens of minutes and AQE's split actually triggers.
python -m spark_applications.debugging.run --case 1 --rows 200000000

# Case 07: Python UDF vs pandas UDF vs native. Per-row pickling cost is
# linear; at 2e8 rows the gap is minutes, not the milliseconds seen locally.
python -m spark_applications.debugging.run --case 7 --rows 200000000

# Production aggregation before/after (baseline, plain join under case-01
# conditions, broadcast, salted).
python -m spark_applications.debugging.production_metrics --rows 200000000
```

`--rows` is ignored by case 03 (driven by a table on disk).

**Run on 2026-09-06** against the stack's `emr-7.13.0` cluster (1 master +
2 core + 1 Spot task, m5.xlarge; 3 executors x 4g x 2 cores). Results and
their reading are in `DEBUGGING.md`, "Cluster re-measurement". Two settings
the run needed that are not in the snippet below: `--driver-memory 6g`
(client-mode default heap is 1g and died at once) and
`--conf spark.executor.memoryOverhead=2g` for case 07 (Python workers live
in the overhead; the default 384MB got them killed).

### EMR

The cluster created by `aws_deployment/cloudformation/main.yaml` runs EMR 7.x,
whose default interpreter is Python 3.9; the stack's bootstrap action
(`aws_deployment/emr/bootstrap_python311.sh`) installs 3.11 and
`spark-defaults` points PySpark at it, because this package is written for
the 3.10 baseline.

```bash
cd spark_applications
uv build --wheel                       # dist/spark_applications-*.whl
aws s3 cp dist/spark_applications-0.1.0-py3-none-any.whl s3://$DEPLOYMENT_BUCKET/deployment/
printf 'from spark_applications.debugging.cluster_measure import main\n\nmain()\n' > entry.py
aws s3 cp entry.py s3://$DEPLOYMENT_BUCKET/deployment/cluster_measure_entry.py

aws emr add-steps --cluster-id $EMR_CLUSTER_ID --steps '[{
  "Name": "cluster-measure",
  "Type": "CUSTOM_JAR", "Jar": "command-runner.jar", "ActionOnFailure": "CONTINUE",
  "Args": ["spark-submit", "--deploy-mode", "client",
           "--py-files", "s3://'$DEPLOYMENT_BUCKET'/deployment/spark_applications-0.1.0-py3-none-any.whl",
           "--conf", "spark.dynamicAllocation.enabled=false",
           "--conf", "spark.executor.instances=3",
           "s3://'$DEPLOYMENT_BUCKET'/deployment/cluster_measure_entry.py",
           "--case", "1", "--case", "7", "--production", "--rows", "20000000"]
}]'
# results: s3://$DEPLOYMENT_BUCKET/emr-logs/<cluster>/steps/<step>/stdout.gz
```

Client deploy mode keeps the driver on the master so the step's stdout holds
the table. `dynamicAllocation=false` with a fixed executor count keeps the
managed-scaling policy from resizing the cluster mid-measurement.

### Databricks

Upload the wheel as a cluster library, then a Python job task with the
`spark_applications.debugging.run` entry point and parameters
`["--case", "1", "--rows", "200000000"]`. Databricks Runtime 17.3 has AQE on
by default; the case turns it off for the "broken" leg itself.

## What to record

Per case, per leg (broken / fixed / AQE), from the Spark UI **SQL** tab of
the run (not `explain()`; AQE rewrites only appear in the executed plan):

| metric | where |
| --- | --- |
| wall-clock of the action | Jobs tab, job duration |
| max task duration vs median | Stages tab -> the join/aggregate stage -> Summary Metrics |
| shuffle read bytes, max vs median | same summary; the straggler ratio is the skew signal |
| shuffle write bytes total | Stages tab |
| spill (memory / disk) | Stages tab; non-zero spill on one task is the OOM-in-waiting |
| `AQEShuffleRead` type | SQL tab -> executed plan; `coalesced` vs `skewed` |
| executor peak memory | Executors tab |

Fill in `DEBUGGING.md` with a table like:

| case | leg | rows | wall-clock | max task / median | shuffle read max / median | spill |
| --- | --- | --- | --- | --- | --- | --- |
| 01 | broken (SMJ) | 2e8 | | | | |
| 01 | fixed (BHJ) | 2e8 | | | | |
| 01 | AQE only | 2e8 | | | | |
| 07 | Python UDF | 2e8 | | | n/a | |
| 07 | pandas UDF | 2e8 | | | n/a | |
| 07 | native | 2e8 | | | n/a | |
| prod | before (SMJ) | 2e8 | | | | |
| prod | broadcast | 2e8 | | | | |
| prod | salted | 2e8 | | | | |

## What to expect, and what would change a conclusion

- **Case 01.** The broken leg should show one task with shuffle-read tens to
  hundreds of times the median and a wall-clock dominated by it. The fixed leg
  has no shuffle on the join key at all. The AQE leg is the interesting one:
  above the 64MB threshold set in `utils/session.py` the plan should show
  `AQEShuffleRead skewed` and the straggler should split into ~N sub-tasks.
  If it does *not*, the threshold or `skewedPartitionFactor` needs revisiting
  before trusting AQE as a fallback.
- **Case 07.** Expect Python UDF > pandas UDF >> native. Measured: 12.9s /
  8.2s / 1.6s at 10M rows, so native is 8x and the pandas UDF only 1.6x the
  plain UDF for this cheap-per-row function. If pandas UDF is not clearly
  faster, check `spark.sql.execution.arrow.maxRecordsPerBatch` and whether
  the UDF is doing per-element Python work inside the batch (which defeats
  vectorisation).
- **Production before/after.** Broadcast should remove the two join
  exchanges entirely; salted should show the shuffle on `salted_key` with
  even task sizes and a modestly higher total shuffle-write than broadcast
  (the exploded dimension). If salted is *slower* than the plain SMJ at scale,
  `SALT_RANGE` is too high for the dimension size.
