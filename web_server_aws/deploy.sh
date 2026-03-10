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
: "${AWS_ACCOUNT_ID:?Set AWS_ACCOUNT_ID in .env}"
: "${AWS_REGION:?Set AWS_REGION in .env}"
: "${VPC_ID:?Set VPC_ID in .env}"
: "${SUBNET_IDS:?Set SUBNET_IDS in .env}"

PROJECT_NAME="${PROJECT_NAME:-data-processing}"
STACK_NAME="${PROJECT_NAME}-web-server"
ECR_REPO="${PROJECT_NAME}-web-server"
IMAGE_TAG="${IMAGE_TAG:-latest}"
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"

TEMPLATE_FILE="${SCRIPT_DIR}/cloudformation/web_server.yaml"
WEB_SERVER_CODE_DIR="${REPO_ROOT}/web_server_code"
DOCKERFILE="${REPO_ROOT}/web_server_local/Dockerfile"

usage() {
    echo "Usage: $0 {deploy|build|push|create-stack|update-stack|status|delete|logs}"
    echo ""
    echo "Commands:"
    echo "  deploy        Build image, push to ECR, and create/update CloudFormation stack"
    echo "  build         Build the Docker image locally"
    echo "  push          Push the Docker image to ECR"
    echo "  create-stack  Create the CloudFormation stack"
    echo "  update-stack  Update an existing CloudFormation stack"
    echo "  status        Show stack status and service URL"
    echo "  delete        Delete the CloudFormation stack"
    echo "  logs          Tail ECS service logs"
    echo ""
    echo "Required environment variables (set in .env):"
    echo "  AWS_ACCOUNT_ID  AWS account ID"
    echo "  AWS_REGION      AWS region"
    echo "  VPC_ID          VPC ID for ECS and ALB"
    echo "  SUBNET_IDS      Comma-separated public subnet IDs"
    exit 1
}

if [ $# -eq 0 ]; then
    usage
fi

ecr_login() {
    echo "=== Logging in to ECR ==="
    aws ecr get-login-password --region "${AWS_REGION}" \
        | docker login --username AWS --password-stdin \
          "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
}

build_image() {
    echo "=== Building Docker image ==="
    docker build \
        -t "${ECR_REPO}:${IMAGE_TAG}" \
        -f "${DOCKERFILE}" \
        "${WEB_SERVER_CODE_DIR}"
    echo "Image built: ${ECR_REPO}:${IMAGE_TAG}"
}

push_image() {
    ecr_login

    echo "=== Pushing image to ECR ==="
    docker tag "${ECR_REPO}:${IMAGE_TAG}" "${ECR_URI}:${IMAGE_TAG}"
    docker push "${ECR_URI}:${IMAGE_TAG}"
    echo "Image pushed: ${ECR_URI}:${IMAGE_TAG}"
}

create_stack() {
    echo "=== Creating CloudFormation stack ==="
    aws cloudformation create-stack \
        --stack-name "${STACK_NAME}" \
        --template-body "file://${TEMPLATE_FILE}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameters \
            ParameterKey=ProjectName,ParameterValue="${PROJECT_NAME}" \
            ParameterKey=VpcId,ParameterValue="${VPC_ID}" \
            ParameterKey=SubnetIds,ParameterValue="\"${SUBNET_IDS}\"" \
            ParameterKey=ContainerImage,ParameterValue="${ECR_URI}:${IMAGE_TAG}"

    echo "Waiting for stack creation..."
    aws cloudformation wait stack-create-complete --stack-name "${STACK_NAME}"
    echo "Stack created successfully."
    show_status
}

update_stack() {
    echo "=== Updating CloudFormation stack ==="
    aws cloudformation update-stack \
        --stack-name "${STACK_NAME}" \
        --template-body "file://${TEMPLATE_FILE}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameters \
            ParameterKey=ProjectName,ParameterValue="${PROJECT_NAME}" \
            ParameterKey=VpcId,ParameterValue="${VPC_ID}" \
            ParameterKey=SubnetIds,ParameterValue="\"${SUBNET_IDS}\"" \
            ParameterKey=ContainerImage,ParameterValue="${ECR_URI}:${IMAGE_TAG}"

    echo "Waiting for stack update..."
    aws cloudformation wait stack-update-complete --stack-name "${STACK_NAME}"
    echo "Stack updated successfully."
    show_status
}

show_status() {
    echo "=== Stack Status ==="
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --query "Stacks[0].{Status:StackStatus,Outputs:Outputs}" \
        --output table 2>/dev/null || echo "Stack '${STACK_NAME}' not found."
}

delete_stack() {
    echo "=== Deleting CloudFormation stack ==="
    aws cloudformation delete-stack --stack-name "${STACK_NAME}"
    echo "Waiting for stack deletion..."
    aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}"
    echo "Stack deleted."
}

tail_logs() {
    LOG_GROUP="/ecs/${PROJECT_NAME}-web-server"
    echo "=== Tailing logs from ${LOG_GROUP} ==="
    aws logs tail "${LOG_GROUP}" --follow
}

case "$1" in
    deploy)
        build_image
        echo ""
        push_image
        echo ""
        # Create or update
        if aws cloudformation describe-stacks --stack-name "${STACK_NAME}" >/dev/null 2>&1; then
            echo "Existing stack found, updating..."
            update_stack
        else
            echo "No existing stack found, creating..."
            create_stack
        fi
        echo ""
        echo "=== Deployment complete ==="
        ;;
    build)
        build_image
        ;;
    push)
        push_image
        ;;
    create-stack)
        create_stack
        ;;
    update-stack)
        update_stack
        ;;
    status)
        show_status
        ;;
    delete)
        delete_stack
        ;;
    logs)
        tail_logs
        ;;
    *)
        usage
        ;;
esac
