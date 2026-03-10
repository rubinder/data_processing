# AWS Deployment

Deploys the full data processing pipeline infrastructure to AWS using CloudFormation.

## Architecture

The CloudFormation template (`cloudformation/main.yaml`) provisions:

- **S3 Buckets**: Landing bucket (incoming data) and processed data bucket (parquet output)
- **Lambda**: Triggered by S3 events on the landing bucket, starts the Step Function pipeline
- **Step Function**: Orchestrates the pipeline: encoding check (Batch) -> schema crawl (Glue Crawler) -> ETL (Glue Job)
- **AWS Batch (Fargate)**: Runs a containerized encoding check on incoming files
- **AWS Glue**: Crawler for schema discovery and ETL job for transforming data to parquet format in Athena
- **EMR Cluster**: Spark cluster (1 master + 2 core m5.xlarge nodes) for running Spark applications
- **DynamoDB**: Status tracking table with an `id` hash key
- **IAM Roles**: Least-privilege roles for each service

## Directory Structure

- `cloudformation/main.yaml` - CloudFormation template with all AWS resources
- `lambda/trigger_step_function.py` - Lambda handler that triggers the Step Function
- `batch/check_encoding.py` - Encoding check script run as a Batch job
- `batch/Dockerfile` - Container image for the Batch encoding check
- `glue/etl_job.py` - Glue ETL job script
- `scripts/deploy.sh` - Shell script to package, upload, and deploy
- `scripts/deploy.py` - Python script (boto3) to create/update the CloudFormation stack

## Prerequisites

- AWS CLI configured with appropriate credentials
- Docker (for building the Batch job image)
- uv (Python package manager)
- A `.env` file in the project root with:
  - `DEPLOYMENT_BUCKET` - S3 bucket for deployment artifacts
  - `BATCH_JOB_IMAGE` - ECR image URI for the Batch encoding check
  - `VPC_ID` - VPC ID for Batch compute
  - `SUBNET_IDS` - Comma-separated subnet IDs
  - `EMR_KEY_PAIR` (optional) - EC2 key pair for EMR instances

## How to Deploy

```bash
./scripts/deploy.sh
```

This script:
1. Packages the Lambda function into a zip file
2. Uploads the Lambda zip, Glue ETL script, and CloudFormation template to S3
3. Runs `deploy.py` to create or update the CloudFormation stack
