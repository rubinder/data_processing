# FinOps: the cost story for the AWS deployment

This document explains the cost levers built into `cloudformation/main.yaml`
and `glue/etl_job.py`, why each one is there, how to measure it, and what it
costs you in return. There are no measured savings in here: nothing in this
repo has been run against a production-sized dataset yet, and quoting numbers
we did not measure would be worse than quoting none. The last section is the
checklist of what we would record on a real run.

Unit prices below are written as symbols (`$P_athena`, `$P_dpu`, ...) on
purpose. They differ by region and change over time; look them up on the
pricing page for the account's region when you plug numbers in.

Related decisions on the Spark side:

- `spark_applications/DECISIONS.md` #2 (compaction, dynamic overwrite,
  explicit schema, no `count()`): the Glue job mirrors all four.
- `spark_applications/DEBUGGING.md` case 03 (partition pruning lost to a
  UDF): pruning is the mechanism behind "scanned bytes" below.
- `spark_applications/DEBUGGING.md` case 04 (small-files explosion): every
  extra file is an extra LIST/GET on every read.

## Where the money goes

For a pipeline of this shape the bill has five components, in roughly the
order they matter once data volume is non-trivial:

| Component | Billed on | Lever in this repo |
| --------- | --------- | ------------------ |
| Athena | bytes scanned per query | zstd parquet, partition pruning, workgroup cutoff |
| S3 | GB-month by storage class, requests, retrievals | lifecycle tiering, compaction, quarantine/results expiry |
| Glue | DPU-hours per job run | G.1X + auto scaling, timeout, one read pass, no count() |
| EMR | instance-hours (EC2 + EMR uplift) | Spot task nodes, managed scaling, idle auto-termination |
| Everything else | Lambda invocations, Step Function transitions, Batch Fargate seconds, DynamoDB on-demand | negligible at this scale; not tuned |

The compute levers are one-off per run; the storage/scan levers compound,
because every byte written badly is paid for again on every query and every
month it sits there. That is why the parquet codec and partition layout come
first.

## Lever 1: parquet compression (zstd)

**What changed.** Glue (`--conf spark.sql.parquet.compression.codec=zstd`),
the per-write option in `etl_job.py`, and the EMR `spark-defaults`
classification all write zstd parquet instead of Spark's default snappy.

**Why.** Parquet bytes on S3 are paid three times: stored (S3 GB-month),
scanned (Athena, per byte), and read (Spark I/O time, which is DPU-hours or
instance-hours). zstd typically produces noticeably smaller files than snappy
on wide string-heavy data such as impressions, at a higher compression CPU
cost on write and a comparable decompression cost on read. The write happens
once; the reads happen forever. Athena engine version 3 reads zstd parquet
natively.

**How to measure.**

- Bytes: `aws s3 ls --recursive --summarize s3://<processed>/processed/`
  before and after, on the same input file. Or `SUM(size)` from S3 Inventory
  if it is on.
- Write CPU: Glue `ExecutionTime` and `DPUSeconds` for the run
  (`aws glue get-job-run --job-name <job> --run-id <id>`), same input, both
  codecs. The Spark UI (`--enable-spark-ui` is on) shows the write stage's
  task time directly.
- Read: Athena `Statistics.DataScannedInBytes` for the same query on both
  tables (see Lever 3).

**Trade-offs.** More CPU per written byte. On a CPU-bound write stage this
lengthens the run and costs DPU-seconds; the expectation is that the scan
savings dominate on any table queried more than a handful of times. If a
table is write-once-read-never, snappy is the cheaper choice for it.

## Lever 2: S3 lifecycle and layout

**What changed.** Both buckets have `LifecycleConfiguration` rules
(comments in the template explain each):

| Bucket | Prefix | Rule |
| ------ | ------ | ---- |
| landing | `raw/` | STANDARD_IA at 30 days, GLACIER_IR at 90 |
| processed | `processed/` | INTELLIGENT_TIERING at 30 days |
| processed | `quarantine/` | expire at 90 days |
| processed | `athena-results/` | expire at 7 days |
| both | all | abort incomplete multipart uploads after 7 days |
| both | all | expire noncurrent versions after 14 days (no-op while `BucketVersioning=Suspended`) |

The Glue job also repartitions by the partition keys before writing, so each
`page_type=/date=/hour=` directory holds one parquet file per run instead of
one per Spark task (DECISIONS.md #2, DEBUGGING.md case 04).

**Why.**

- Raw files are read once by the pipeline and then only on a replay. IA
  roughly halves the storage price for infrequently read data, GLACIER_IR
  cuts it again and still returns objects in milliseconds, so a backfill
  job can read them with no restore step.
- Processed parquet has an access pattern we cannot predict per partition
  (recent hot, old cold, a backfill makes an old one hot again).
  Intelligent Tiering follows the actual pattern with no retrieval fee.
- Quarantine and Athena result sets are scratch that is never cleaned up
  by hand.
- Small files cost LIST and GET requests on every read and inflate Athena
  planning time; compaction is cheaper than any storage class change.

**How to measure.**

- Cost Explorer, filter `Service = S3`, group by `Usage Type`
  (`TimedStorage-ByteHrs`, `TimedStorage-SIA-ByteHrs`,
  `TimedStorage-GIR-ByteHrs`, `TimedStorage-INT-*`, `Requests-Tier1/2`),
  filtered by the `Project`/`Environment`/`CostCenter` tags. Activate the
  three keys as cost allocation tags in Billing first; activation is not
  retroactive.
- CloudWatch `AWS/S3` `BucketSizeBytes` per `StorageType` dimension shows
  the tiering happen day by day.
- File counts per partition: `aws s3 ls s3://<processed>/processed/page_type=1/date=2026-01-01/hour=7/ | wc -l`
  should be 1 per run.

**Trade-offs.**

- IA and GLACIER_IR add a per-GB retrieval charge and a minimum storage
  duration (30 and 90 days respectively). A replay-heavy period can make IA
  more expensive than STANDARD for that month.
- S3 does not transition objects smaller than 128 KB to IA; tiny raw files
  stay in STANDARD regardless of the rule (another reason to prefer fewer,
  larger uploads).
- Intelligent Tiering charges a small per-object monitoring fee, which is
  why the rule waits 30 days and why compaction matters more with it on.
- The `raw/` rule only applies to objects uploaded under `raw/`; uploads to
  the bucket root are still processed but never tiered.

## Lever 3: Athena scanned bytes and the workgroup

**What changed.** An `AWS::Athena::WorkGroup` (`<ProjectName>-<Environment>`)
with `EnforceWorkGroupConfiguration=true`, `PublishCloudWatchMetricsEnabled`,
Athena engine version 3, a results prefix that expires after a week, and
`BytesScannedCutoffPerQuery` (parameter `AthenaBytesScannedCutoffBytes`,
default 10 GiB).

**Why.** Athena's price is linear in bytes scanned, so it is the one AWS
service where a query's cost is visible before it is paid. The cutoff turns
"someone ran `SELECT *` without a partition filter" from a bill into a
cancelled query. Everything that reduces scanned bytes is the same list as
the Spark performance list: columnar format, compression, partition pruning
(DEBUGGING.md case 03: a UDF on the partition column defeats pruning and the
scan silently goes from one partition to the whole table).

**Arithmetic.**

```
athena_cost(query) = max(bytes_scanned, 10 MiB) / 1 TiB * $P_athena
```

with `$P_athena` the per-TB price for the region. The 10 MiB floor means a
partition-pruned point query costs the same as a query over 10 MiB; below
that, fewer bytes buys nothing. DDL, failed queries and cancelled queries
(including cutoff cancellations) are not billed for scanned bytes.

**How to measure.**

- Per query: `aws athena get-query-execution --query-execution-id <id>`,
  fields `Statistics.DataScannedInBytes`, `EngineExecutionTimeInMillis`,
  `TotalExecutionTimeInMillis`. This is the number to record before/after a
  layout or codec change, on the same SQL.
- Per workgroup over time: CloudWatch `AWS/Athena`, metrics
  `ProcessedBytes` (sum per period) and `TotalExecutionTime`, dimension
  `WorkGroup`. Multiply the period sum by `$P_athena / 1 TiB`.
- Pruning check: `EXPLAIN` in Athena, or compare `DataScannedInBytes` for
  the same query with and without the partition predicate.

**Trade-offs.** The cutoff is a blunt instrument: a legitimate full-table
scan larger than the limit fails and has to be re-run in a workgroup with a
higher limit (or the parameter raised). Set it to the largest scan the
business actually needs plus headroom, not to the smallest one you can get
away with. Enforcing the workgroup configuration also means clients cannot
choose their own result location, which is the point but surprises people
who had one configured.

## Lever 4: Glue DPU-hours

**What changed.** `GlueVersion` 5.0 (Spark 3.5, parameterised), `G.1X`
workers, `NumberOfWorkers` as the auto-scaling ceiling
(`--enable-auto-scaling`), `Timeout` 60 minutes, `MaxRetries` 1,
`MaxConcurrentRuns` 1, `--enable-metrics` and
`--enable-observability-metrics`. The script itself does one read pass
(explicit schema, no `inferSchema`), one cached materialisation for the
valid/quarantine split, and no `count()` on the write lineage.

**Why.** Glue bills per DPU-second while workers are allocated, with a
per-run minimum. Three things waste DPU-seconds on a job like this:
workers idle while the driver plans (fixed allocation without auto
scaling); a second full pass over the input to infer types; and `count()`
calls that recompute the lineage. A stuck run is the fourth and the
`Timeout` caps it. `MaxRetries=1` is affordable because the write is
idempotent (dynamic partition overwrite), so a retry after a transient
failure cannot duplicate data.

**Arithmetic.**

```
glue_cost(run) = DPU_hours * $P_dpu
DPU_hours      = DPUSeconds / 3600          (from get-job-run)
                 = sum over workers of (seconds allocated) * DPU_per_worker / 3600
```

`G.1X` is 1 DPU per worker. With auto scaling `DPUSeconds` reflects workers
actually allocated over time, not `NumberOfWorkers * ExecutionTime`. Glue
applies a minimum billed duration per run (1 minute for Glue 2.0+ Spark
jobs at the time of writing; check the pricing page), which is why
many tiny runs are worse than fewer larger ones.

**How to measure.**

- Per run: `aws glue get-job-run --job-name <job> --run-id <id>` returns
  `ExecutionTime` (seconds), `DPUSeconds`, `MaxCapacity`, `NumberOfWorkers`.
  `DPUSeconds / ExecutionTime` is the average DPUs in use; compare it with
  `NumberOfWorkers` to see how much auto scaling saved.
- CloudWatch namespace `Glue`, dimensions `JobName`, `JobRunId`, `Type`:
  `glue.driver.aggregate.numCompletedTasks`,
  `glue.driver.aggregate.bytesRead`, `glue.driver.aggregate.shuffleBytesWritten`,
  `glue.driver.aggregate.elapsedTime`. Fewer completed tasks for the same
  input after removing `inferSchema` and `count()` is the direct evidence
  that passes were removed.
- Observability metrics (`glue.driver.workerUtilization`,
  `glue.driver.skewness.*`) show whether the worker ceiling is too high
  (low utilisation) or too low (utilisation pinned, long run).
- Cost Explorer: `Service = Glue`, `Usage Type` containing `DPU`, grouped
  by the `Project` tag.

**Trade-offs.** Auto scaling has a floor of one worker for the driver and
scales on observed load, so a very short job pays start-up time for
executors it barely uses. A higher `NumberOfWorkers` ceiling shortens wall
time but is only cheaper if the job is actually parallel-bound. Bookmarks
are enabled but inert (the read is `spark.read`, not a `DynamicFrame` with a
`transformation_ctx`); this costs nothing and keeps the option open.

## Lever 5: EMR right-sizing

**What changed.** Master and core on demand, a `TaskInstanceGroups` entry
on Spot (`Market: SPOT`, no bid price so capped at on-demand), a
`ManagedScalingPolicy` with min/max in instances (parameters
`EmrManagedScalingMinUnits` / `MaxUnits`), `MaximumOnDemandCapacityUnits`
and `MaximumCoreCapacityUnits` pinned to the core count so everything
above the floor is Spot task capacity, `AutoTerminationPolicy` with a
one-hour idle timeout, `StepConcurrencyLevel` 2, release `emr-7.13.0`
(Spark 3.5.6). The `spark-defaults` classification mirrors
`spark_applications/utils/session.py` (AQE, dynamic overwrite, zstd).

**Why.** EMR cost is instance-hours times (EC2 price + EMR uplift). The
three levers, in order of typical impact: do not run when idle (auto
termination); do not run more nodes than the current step needs (managed
scaling); pay Spot prices for the nodes that can be lost safely (task
nodes hold no HDFS blocks, so an interruption costs a task retry, not the
job). Master and core stay on demand because losing the driver or a shuffle
file fails the job outright, and a failed job is paid for twice.

**Arithmetic.**

```
emr_cost = sum over instances of hours * ($P_ec2(type, market) + $P_emr_uplift(type))
```

Spot price is set by the market and varies by AZ and hour; the EMR uplift
is the same for Spot and on demand. Instance-hours are billed per second
with a one-minute minimum.

**How to measure.**

- EMR console, cluster details, "Instance hours" and the per-instance-group
  breakdown; `aws emr describe-cluster --cluster-id <id>` gives
  `NormalizedInstanceHours`.
- Spot savings: EC2 console Spot Requests "Savings summary", or Cost
  Explorer `Service = EC2`, `Purchase Option = Spot` vs `On Demand`,
  filtered by the cluster's tags (EMR propagates cluster tags to its EC2
  instances).
- Scaling behaviour: CloudWatch `AWS/ElasticMapReduce`
  `TotalUnitsRequested`, `TotalUnitsRunning`, `YARNMemoryAvailablePercentage`,
  `ContainerPendingRatio`; the cluster's "Events" tab lists scale-in/out.
- Idle time: `IsIdle` metric; anything sustained at 1 before the
  auto-termination fires is money.

**Trade-offs.**

- Spot interruptions on task nodes lengthen a job (task retries, shuffle
  refetch) and can starve it if the whole task group is reclaimed; the
  core floor guarantees progress, slowly.
- The managed scaling floor (`MinUnits`) is paid for continuously while the
  cluster is up; set it to what the smallest expected job needs, not what
  the largest one does.
- Auto termination requires `KeepJobFlowAliveWhenNoSteps=true` (set), and a
  terminated cluster has to be recreated (a stack update with a new release
  label replaces the cluster anyway). For a bursty schedule, EMR Serverless
  removes both problems and is the logical next step; it is not in this
  template.
- `spark.jars.packages` for OpenLineage resolves from Maven Central at
  submit time, so nodes need outbound internet; a NAT gateway has its own
  hourly and per-GB cost.

## Cost-allocation tags

Every taggable resource in the template carries `Project`, `Environment`
and `CostCenter`. IAM roles, Lambda, Step Functions, Glue job and crawler,
Batch compute environment / queue / job definition, EMR (propagated to its
EC2 instances), DynamoDB, S3 buckets, the Athena workgroup and the Batch
security group are all covered. `AWS::Glue::Database` and
`AWS::IAM::InstanceProfile` do not support tags.

Before the tags are useful in Cost Explorer they must be activated under
Billing > Cost allocation tags; data is tagged from activation onwards only.
S3 storage cost also needs the buckets' tags (present) and, for object-level
attribution, S3 Storage Lens or Inventory.

## What we would measure on a real run

One representative day of input, run twice (before/after the change under
test), same inputs, same region, recorded in a table next to the change:

- [ ] Input: file count, total bytes, row count (from the job's
      `job_complete` log line: `rows_read`, `rows_written`,
      `rows_quarantined`).
- [ ] Output: bytes under `processed/` (`aws s3 ls --summarize`), file
      count per partition (expect 1), zstd vs snappy bytes on the same
      input.
- [ ] Glue: `ExecutionTime`, `DPUSeconds`, `NumberOfWorkers`,
      `glue.driver.aggregate.numCompletedTasks`,
      `glue.driver.aggregate.bytesRead`, `workerUtilization`.
- [ ] Athena: three canonical queries (point lookup with all three
      partition predicates, one-day aggregate, full-table count),
      `DataScannedInBytes` and `TotalExecutionTimeInMillis` each, before
      and after; confirm the cutoff cancels an unfiltered `SELECT *` on a
      table larger than the limit.
- [ ] S3: `BucketSizeBytes` by `StorageType` on day 0, 31, 91 to confirm
      the transitions fire; `Requests-Tier2` (GET/LIST) per query before
      and after compaction.
- [ ] EMR: `NormalizedInstanceHours` for the same step on the old fixed
      cluster vs managed scaling + Spot; Spot interruption count from the
      cluster events; time from last step to auto-termination.
- [ ] Cost Explorer, 7 days after activation: cost grouped by `Project`
      then by `Service`, to check every line item is attributed (an
      unattributed line means an untagged resource).
- [ ] OpenLineage (if enabled): one run's lineage graph in Marquez shows
      the S3 input, the `processed/` output and (if any) the quarantine
      dataset, so a cost question ("who reads this table?") can be
      answered from lineage rather than from CloudTrail.
