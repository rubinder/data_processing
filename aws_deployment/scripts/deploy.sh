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

# .env.example ships placeholder credentials. If they were copied verbatim
# they now shadow the CLI profile / SSO session and every call fails with
# InvalidAccessKeyId, so drop them and let the AWS CLI credential chain win.
if [[ "${AWS_ACCESS_KEY_ID:-}" == your_* || "${AWS_SECRET_ACCESS_KEY:-}" == your_* ]]; then
    echo "Ignoring placeholder AWS credentials from .env; using the CLI credential chain."
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
fi

# Required environment variables
: "${DEPLOYMENT_BUCKET:?Set DEPLOYMENT_BUCKET in .env}"
: "${BATCH_JOB_IMAGE:?Set BATCH_JOB_IMAGE in .env}"
: "${VPC_ID:?Set VPC_ID in .env}"
: "${SUBNET_IDS:?Set SUBNET_IDS in .env}"
EMR_KEY_PAIR="${EMR_KEY_PAIR:-}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
COST_CENTER="${COST_CENTER:-data-platform}"
OPENLINEAGE_URL="${OPENLINEAGE_URL:-}"
OPENLINEAGE_SPARK_VERSION="${OPENLINEAGE_SPARK_VERSION:-1.53.0}"

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

# EMR bootstrap (Python 3.11 for the 3.10-baseline jobs). Always uploaded;
# set EMR_BOOTSTRAP=false to deploy the cluster without it.
EMR_BOOTSTRAP="${EMR_BOOTSTRAP:-true}"
EMR_BOOTSTRAP_KEY=""
if [ "${EMR_BOOTSTRAP}" = "true" ]; then
    EMR_BOOTSTRAP_KEY="${DEPLOY_PREFIX}/bootstrap_python311.sh"
    aws s3 cp "${PROJECT_DIR}/emr/bootstrap_python311.sh" \
        "s3://${DEPLOYMENT_BUCKET}/${EMR_BOOTSTRAP_KEY}"
fi

# Glue cannot resolve Maven coordinates; it needs the OpenLineage jar on S3
# (--extra-jars). Fetch it from Maven Central only when lineage is enabled.
if [ -n "${OPENLINEAGE_URL}" ]; then
    echo "=== Uploading OpenLineage Spark jar ${OPENLINEAGE_SPARK_VERSION} ==="
    OL_JAR="openlineage-spark_2.12-${OPENLINEAGE_SPARK_VERSION}.jar"
    OL_KEY="${DEPLOY_PREFIX}/jars/${OL_JAR}"
    if ! aws s3 ls "s3://${DEPLOYMENT_BUCKET}/${OL_KEY}" > /dev/null 2>&1; then
        curl -fsSL -o "${TEMP_DIR}/${OL_JAR}" \
            "https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OPENLINEAGE_SPARK_VERSION}/${OL_JAR}"
        aws s3 cp "${TEMP_DIR}/${OL_JAR}" "s3://${DEPLOYMENT_BUCKET}/${OL_KEY}"
    else
        echo "Jar already present at s3://${DEPLOYMENT_BUCKET}/${OL_KEY}"
    fi
fi

TEMPLATE_URL="https://${DEPLOYMENT_BUCKET}.s3.amazonaws.com/${DEPLOY_PREFIX}/main.yaml"

echo "=== Deploying CloudFormation stack ==="
cd "${PROJECT_DIR}"
uv run python scripts/deploy.py \
    --template-url "${TEMPLATE_URL}" \
    --deployment-bucket "${DEPLOYMENT_BUCKET}" \
    --batch-job-image "${BATCH_JOB_IMAGE}" \
    --vpc-id "${VPC_ID}" \
    --subnet-ids "${SUBNET_IDS}" \
    --emr-key-pair "${EMR_KEY_PAIR}" \
    --environment "${ENVIRONMENT}" \
    --cost-center "${COST_CENTER}" \
    --openlineage-url "${OPENLINEAGE_URL}" \
    --openlineage-spark-version "${OPENLINEAGE_SPARK_VERSION}" \
    --emr-bootstrap-key "${EMR_BOOTSTRAP_KEY}"

echo "=== Deployment complete ==="
