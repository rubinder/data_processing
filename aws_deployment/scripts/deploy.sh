#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_DIR}/.." && pwd)"

# Load environment variables
if [ -f "${REPO_ROOT}/.env" ]; then
    set -a
    source "${REPO_ROOT}/.env"
    set +a
fi

# Required environment variables
: "${DEPLOYMENT_BUCKET:?Set DEPLOYMENT_BUCKET in .env}"
: "${BATCH_JOB_IMAGE:?Set BATCH_JOB_IMAGE in .env}"
: "${VPC_ID:?Set VPC_ID in .env}"
: "${SUBNET_IDS:?Set SUBNET_IDS in .env}"
EMR_KEY_PAIR="${EMR_KEY_PAIR:-}"

DEPLOY_PREFIX="deployment"
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "${TEMP_DIR}"' EXIT

echo "=== Packaging Lambda function ==="
cd "${PROJECT_DIR}/lambda"
zip -r "${TEMP_DIR}/lambda.zip" trigger_step_function.py
cd "${PROJECT_DIR}"

echo "=== Uploading artifacts to S3 ==="
aws s3 cp "${TEMP_DIR}/lambda.zip" \
    "s3://${DEPLOYMENT_BUCKET}/${DEPLOY_PREFIX}/lambda.zip"

aws s3 cp "${PROJECT_DIR}/glue/etl_job.py" \
    "s3://${DEPLOYMENT_BUCKET}/${DEPLOY_PREFIX}/etl_job.py"

aws s3 cp "${PROJECT_DIR}/cloudformation/main.yaml" \
    "s3://${DEPLOYMENT_BUCKET}/${DEPLOY_PREFIX}/main.yaml"

TEMPLATE_URL="https://${DEPLOYMENT_BUCKET}.s3.amazonaws.com/${DEPLOY_PREFIX}/main.yaml"

echo "=== Deploying CloudFormation stack ==="
cd "${PROJECT_DIR}"
uv run python scripts/deploy.py \
    --template-url "${TEMPLATE_URL}" \
    --deployment-bucket "${DEPLOYMENT_BUCKET}" \
    --batch-job-image "${BATCH_JOB_IMAGE}" \
    --vpc-id "${VPC_ID}" \
    --subnet-ids "${SUBNET_IDS}" \
    --emr-key-pair "${EMR_KEY_PAIR}"

echo "=== Deployment complete ==="
