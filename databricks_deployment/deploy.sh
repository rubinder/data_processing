#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load environment variables
if [ -f "${REPO_ROOT}/.env" ]; then
    set -a
    source "${REPO_ROOT}/.env"
    set +a
fi

# Required environment variables
: "${DATABRICKS_HOST:?Set DATABRICKS_HOST in .env}"
: "${DATABRICKS_TOKEN:?Set DATABRICKS_TOKEN in .env}"

WORKFLOW_FILE="${SCRIPT_DIR}/workflow.json"
SPARK_APP_DIR="${REPO_ROOT}/spark_applications/spark_applications"
DBFS_DEST="dbfs:/spark_applications/spark_applications"

usage() {
    echo "Usage: $0 {deploy|upload-code|create-job|update-job|status|delete-job}"
    echo ""
    echo "Commands:"
    echo "  deploy       Upload code and create/update the workflow job"
    echo "  upload-code  Upload spark application files to DBFS"
    echo "  create-job   Create the workflow job from workflow.json"
    echo "  update-job   Update an existing workflow job"
    echo "  status       Show the current workflow job status"
    echo "  delete-job   Delete the workflow job"
    echo ""
    echo "Required environment variables (set in .env):"
    echo "  DATABRICKS_HOST   Databricks workspace URL"
    echo "  DATABRICKS_TOKEN  Databricks personal access token"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

upload_code() {
    echo "=== Uploading spark applications to DBFS ==="
    for py_file in "${SPARK_APP_DIR}"/*.py; do
        filename=$(basename "${py_file}")
        echo "  Uploading ${filename}..."
        databricks fs cp --overwrite "${py_file}" "${DBFS_DEST}/${filename}"
    done

    # Upload utils package
    echo "  Uploading utils package..."
    for py_file in "${SPARK_APP_DIR}"/utils/*.py; do
        filename=$(basename "${py_file}")
        databricks fs cp --overwrite "${py_file}" "${DBFS_DEST}/utils/${filename}"
    done

    echo "Code upload complete."
}

get_job_id() {
    databricks jobs list --output json \
        | python3 -c "
import json, sys
jobs = json.load(sys.stdin).get('jobs', [])
for job in jobs:
    if job['settings']['name'] == 'data_processing_workflow':
        print(job['job_id'])
        sys.exit(0)
sys.exit(1)
" 2>/dev/null
}

create_job() {
    echo "=== Creating Databricks workflow job ==="
    databricks jobs create --json-file "${WORKFLOW_FILE}"
    echo "Job created successfully."
}

update_job() {
    echo "=== Updating Databricks workflow job ==="
    JOB_ID=$(get_job_id) || {
        echo "Error: No existing job named 'data_processing_workflow' found."
        echo "Run '$0 create-job' first."
        exit 1
    }
    echo "Found job ID: ${JOB_ID}"
    databricks jobs reset --job-id "${JOB_ID}" --json-file "${WORKFLOW_FILE}"
    echo "Job updated successfully."
}

show_status() {
    JOB_ID=$(get_job_id) || {
        echo "No job named 'data_processing_workflow' found."
        exit 1
    }
    echo "Job ID: ${JOB_ID}"
    databricks jobs get --job-id "${JOB_ID}"
}

delete_job() {
    JOB_ID=$(get_job_id) || {
        echo "No job named 'data_processing_workflow' found."
        exit 1
    }
    echo "Deleting job ${JOB_ID}..."
    databricks jobs delete --job-id "${JOB_ID}"
    echo "Job deleted."
}

case "$1" in
    deploy)
        upload_code
        echo ""
        # Create or update
        if JOB_ID=$(get_job_id) 2>/dev/null; then
            echo "Existing job found (ID: ${JOB_ID}), updating..."
            update_job
        else
            echo "No existing job found, creating..."
            create_job
        fi
        echo ""
        echo "=== Deployment complete ==="
        ;;
    upload-code)
        upload_code
        ;;
    create-job)
        create_job
        ;;
    update-job)
        update_job
        ;;
    status)
        show_status
        ;;
    delete-job)
        delete_job
        ;;
    *)
        usage
        ;;
esac
