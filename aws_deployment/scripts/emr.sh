#!/usr/bin/env bash
# Manage the EMR stack (cloudformation/emr.yaml) separately from the core
# pipeline stack. EMR bills by the instance-hour whether or not it works, so
# the cluster is created for a job and deleted afterwards; the core stack
# (scripts/deploy.sh) stays up at near-zero idle cost.
#
#   ./scripts/emr.sh up       create/update the cluster stack, print the cluster id
#   ./scripts/emr.sh status   stack status + cluster state
#   ./scripts/emr.sh down     delete the stack (terminates the cluster)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${PROJECT_DIR}/.." && pwd)"

if [ -f "${REPO_ROOT}/.env" ]; then
    set -a
    source "${REPO_ROOT}/.env"
    set +a
fi
if [[ "${AWS_ACCESS_KEY_ID:-}" == your_* || "${AWS_SECRET_ACCESS_KEY:-}" == your_* ]]; then
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
fi

: "${DEPLOYMENT_BUCKET:?Set DEPLOYMENT_BUCKET in .env}"
: "${SUBNET_IDS:?Set SUBNET_IDS in .env}"
PROJECT_NAME="${PROJECT_NAME:-data-processing}"
STACK_NAME="${PROJECT_NAME}-emr"
DEPLOY_PREFIX="deployment"
TEMPLATE_URL="https://${DEPLOYMENT_BUCKET}.s3.amazonaws.com/${DEPLOY_PREFIX}/emr.yaml"
EMR_BOOTSTRAP="${EMR_BOOTSTRAP:-true}"

usage() {
    echo "Usage: $0 {up|status|down}"
    exit 1
}

case "${1:-}" in
    up)
        echo "=== Uploading EMR template and bootstrap ==="
        aws s3 cp "${PROJECT_DIR}/cloudformation/emr.yaml" \
            "s3://${DEPLOYMENT_BUCKET}/${DEPLOY_PREFIX}/emr.yaml" --only-show-errors
        EMR_BOOTSTRAP_KEY=""
        if [ "${EMR_BOOTSTRAP}" = "true" ]; then
            EMR_BOOTSTRAP_KEY="${DEPLOY_PREFIX}/bootstrap_python311.sh"
            aws s3 cp "${PROJECT_DIR}/emr/bootstrap_python311.sh" \
                "s3://${DEPLOYMENT_BUCKET}/${EMR_BOOTSTRAP_KEY}" --only-show-errors
        fi
        echo "=== Deploying ${STACK_NAME} ==="
        cd "${PROJECT_DIR}"
        # deploy.py drops parameters emr.yaml does not declare (vpc, batch
        # image, ...), so the same argument set serves both stacks.
        uv run python scripts/deploy.py \
            --stack-name "${STACK_NAME}" \
            --template-url "${TEMPLATE_URL}" \
            --deployment-bucket "${DEPLOYMENT_BUCKET}" \
            --batch-job-image "${BATCH_JOB_IMAGE:-unused}" \
            --vpc-id "${VPC_ID:-unused}" \
            --subnet-ids "${SUBNET_IDS}" \
            --emr-key-pair "${EMR_KEY_PAIR:-}" \
            --environment "${ENVIRONMENT:-dev}" \
            --cost-center "${COST_CENTER:-data-platform}" \
            --openlineage-url "${OPENLINEAGE_URL:-}" \
            --openlineage-spark-version "${OPENLINEAGE_SPARK_VERSION:-1.53.0}" \
            --emr-bootstrap-key "${EMR_BOOTSTRAP_KEY}"
        echo "Remember: ./scripts/emr.sh down when the job is finished."
        ;;
    status)
        aws cloudformation describe-stacks --stack-name "${STACK_NAME}" \
            --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "no ${STACK_NAME} stack"
        CLUSTER_ID=$(aws cloudformation describe-stacks --stack-name "${STACK_NAME}" \
            --query "Stacks[0].Outputs[?OutputKey=='EMRClusterId'].OutputValue" --output text 2>/dev/null || true)
        if [ -n "${CLUSTER_ID}" ] && [ "${CLUSTER_ID}" != "None" ]; then
            aws emr describe-cluster --cluster-id "${CLUSTER_ID}" \
                --query 'Cluster.[Id,Status.State,Status.StateChangeReason.Message]' --output text
        fi
        ;;
    down)
        echo "Deleting ${STACK_NAME} (terminates the cluster)..."
        aws cloudformation delete-stack --stack-name "${STACK_NAME}"
        aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}"
        echo "Deleted."
        ;;
    *)
        usage
        ;;
esac
