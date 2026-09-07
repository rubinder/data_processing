# AWS Deployment

Deploys the full data processing pipeline infrastructure to AWS using CloudFormation.

The cost decisions behind the template (compression, lifecycle, Athena
workgroup, Glue DPU-hours, EMR Spot/scaling) and how to measure them are in
[FINOPS.md](FINOPS.md).

## Architecture

The CloudFormation template (`cloudformation/main.yaml`) provisions:

- **S3 Buckets**: Landing bucket (incoming data) and processed data bucket (parquet output), both with public access blocked, SSE-S3, optional versioning and lifecycle rules (see below)
- **Lambda**: Triggered by S3 events on the landing bucket, starts the Step Function pipeline
- **Step Function**: Orchestrates the pipeline: encoding check (Batch) -> schema crawl (Glue Crawler) -> ETL (Glue Job); retries the Glue start on `ConcurrentRunsExceededException`
- **AWS Batch (Fargate)**: Runs a containerized encoding check on incoming files, in an egress-only security group in `VpcId`
- **AWS Glue**: Crawler for schema discovery and a Glue 5.0 (Spark 3.5) ETL job writing partitioned zstd parquet with dynamic partition overwrite and a quarantine prefix for malformed rows
- **Athena WorkGroup**: Enforced configuration, per-query scanned-bytes cutoff, CloudWatch metrics, engine version 3, results under `s3://<processed>/athena-results/`
- **EMR Cluster**: Spark 3.5 (emr-7.x) with on-demand master/core, Spot task nodes, managed scaling and idle auto-termination
- **DynamoDB**: Status tracking table with an `id` hash key
- **IAM Roles**: Least-privilege roles for each service
- **Tags**: `Project`, `Environment`, `CostCenter` on every taggable resource for cost allocation

## Directory Structure

- `cloudformation/main.yaml` - CloudFormation template with all AWS resources
- `lambda/trigger_step_function.py` - Lambda handler that triggers the Step Function
- `batch/check_encoding.py` - Encoding check script run as a Batch job
- `batch/Dockerfile` - Container image for the Batch encoding check
- `glue/etl_job.py` - Glue ETL job script (explicit schema, quarantine, dynamic overwrite, zstd, structured log)
- `scripts/deploy.sh` - Shell script to package, upload, and deploy
- `scripts/deploy.py` - Python script (boto3) to create/update the CloudFormation stack
- `FINOPS.md` - Cost levers, measurement recipes and trade-offs

## Parameters

| Parameter | Default | Purpose |
| --------- | ------- | ------- |
| `ProjectName` | `data-processing` | Resource name prefix, OpenLineage namespace, `Project` tag |
| `Environment` | `dev` | `dev`/`staging`/`prod`; Athena workgroup name suffix and `Environment` tag |
| `CostCenter` | `data-platform` | `CostCenter` tag |
| `DeploymentBucket` | (required) | Artifacts: Lambda zip, Glue script, OpenLineage jar, Glue temp/event logs, EMR logs |
| `LambdaS3Key`, `GlueScriptS3Key` | `deployment/...` | Artifact keys |
| `BatchJobImage` | (required) | ECR image for the encoding check |
| `VpcId`, `SubnetIds` | (required) | Network for Batch and EMR |
| `EmrKeyPair` | `""` | Optional EC2 key pair (omitted from the cluster when empty) |
| `BucketVersioning` | `Suspended` | `Enabled` keeps overwritten partition files for 14 days (lifecycle-bounded) |
| `GlueVersion` | `5.0` | `4.0` (Spark 3.3), `5.0` (Spark 3.5.4), `5.1` (Spark 3.5.6) |
| `GlueNumberOfWorkers` | `2` | Auto-scaling ceiling of G.1X workers |
| `GlueJobTimeoutMinutes` | `60` | Run timeout |
| `GlueMaxConcurrentRuns` | `1` | Serialises writes; the state machine retries when exceeded |
| `AthenaBytesScannedCutoffBytes` | `10737418240` (10 GiB) | Per-query cutoff enforced by the workgroup (min 10 MiB) |
| `EmrReleaseLabel` | `emr-7.13.0` | Any emr-7.x is Spark 3.5 |
| `EmrInstanceType` | `m5.xlarge` | Master, core and task nodes |
| `EmrCoreInstanceCount` | `2` | On-demand core nodes; also the managed-scaling on-demand and core ceilings |
| `EmrTaskInstanceCount` | `1` | Initial Spot task nodes |
| `EmrManagedScalingMinUnits` / `MaxUnits` | `2` / `10` | Managed scaling floor/ceiling in instances |
| `EmrIdleTimeoutSeconds` | `3600` | Auto-terminate after this idle period |
| `OpenLineageUrl` | `""` | OpenLineage HTTP endpoint; empty disables lineage on Glue and EMR |
| `OpenLineageSparkVersion` | `1.53.0` | `io.openlineage:openlineage-spark_2.12` version |

## S3 lifecycle

| Bucket | Prefix | Rule |
| ------ | ------ | ---- |
| landing | `raw/` | STANDARD_IA after 30 days, GLACIER_IR after 90 |
| processed | `processed/` | INTELLIGENT_TIERING after 30 days |
| processed | `quarantine/` | expire after 90 days |
| processed | `athena-results/` | expire after 7 days |
| both | all | abort incomplete multipart uploads after 7 days; expire noncurrent versions after 14 days |

Upload raw files under `raw/` in the landing bucket to get the tiering rule
(any key triggers the pipeline; only `raw/` is tiered).

## Glue ETL job

`glue/etl_job.py` reads the file named by the Step Function
(`--source_bucket`, `--source_key`) with an explicit impression schema (a
copy of `spark_applications/.../utils/schema.py`), splits unparseable rows to
`s3://<target>/quarantine/<run_id>/` (`run_id` = the Step Function execution
name, or Glue's `JOB_RUN_ID` on a manual run), and writes
`s3://<target>/processed/` partitioned by `page_type/date/hour` with dynamic
partition overwrite, one zstd file per partition. It emits one
`job_start` and one `job_complete` JSON log line (row counts, duration) and
no `count()`-based progress prints.

Job-level Spark settings live in the job's single `--conf` default argument
(`k=v --conf k=v ...`, the pattern Glue expects); the template comments
explain it.

## OpenLineage

Set `OPENLINEAGE_URL` in `.env` (e.g. a Marquez endpoint) to turn lineage on
for both engines:

- **Glue**: `deploy.sh` downloads `openlineage-spark_2.12-<version>.jar` from
  Maven Central and uploads it to
  `s3://$DEPLOYMENT_BUCKET/deployment/jars/`; the job gets `--extra-jars`
  plus the `spark.extraListeners` / `spark.openlineage.*` settings appended
  to `--conf`.
- **EMR**: `spark-defaults` gets `spark.jars.packages` (resolved from Maven
  Central at submit time, so the nodes need outbound internet) and the same
  listener settings.

Leave `OPENLINEAGE_URL` unset and none of this is rendered into the stack.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Docker (for building the Batch job image)
- uv (Python package manager)
- A `.env` file in the project root with:
  - `DEPLOYMENT_BUCKET` - S3 bucket for deployment artifacts
  - `BATCH_JOB_IMAGE` - ECR image URI for the Batch encoding check
  - `VPC_ID` - VPC ID for the Batch security group
  - `SUBNET_IDS` - Comma-separated subnet IDs
  - `EMR_KEY_PAIR` (optional) - EC2 key pair for EMR instances
  - `ENVIRONMENT` (optional, default `dev`) - `dev`/`staging`/`prod`
  - `COST_CENTER` (optional, default `data-platform`)
  - `OPENLINEAGE_URL` (optional) - enables OpenLineage on Glue and EMR
  - `OPENLINEAGE_SPARK_VERSION` (optional, default `1.53.0`)

## How to Deploy

```bash
./scripts/deploy.sh
```

This script:
1. Packages the Lambda function into a zip file
2. Uploads the Lambda zip, Glue ETL script, and CloudFormation template to S3
   (and the OpenLineage jar when `OPENLINEAGE_URL` is set)
3. Runs `deploy.py` to create or update the CloudFormation stack

## Validation

```bash
uvx cfn-lint aws_deployment/cloudformation/main.yaml
python -m py_compile aws_deployment/glue/etl_job.py
```


## EMR Python interpreter

EMR 7.x defaults to Python 3.9; the Spark jobs in this repo target 3.10. The
stack therefore runs `emr/bootstrap_python311.sh` as a bootstrap action
(parameter `EmrBootstrapScriptKey`, uploaded and set by `scripts/deploy.sh`;
`EMR_BOOTSTRAP=false ./scripts/deploy.sh` disables it) and points
`spark.pyspark.python` at `/usr/bin/python3.11`. The measurement protocol
that uses the cluster is in `../spark_applications/spark_applications/debugging/CLUSTER_RUN.md`.

`scripts/deploy.sh` sources `../.env`; placeholder `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` values copied from `.env.example` are ignored so they
cannot shadow the CLI profile.
