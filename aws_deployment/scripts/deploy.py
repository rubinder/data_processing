"""Deploy CloudFormation stack using boto3."""

import argparse
import sys
import time

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

cf_client = boto3.client("cloudformation")


def stack_exists(stack_name: str) -> bool:
    """Check if a CloudFormation stack exists."""
    try:
        cf_client.describe_stacks(StackName=stack_name)
        return True
    except ClientError as e:
        if "does not exist" in str(e):
            return False
        raise


def deploy_stack(
    stack_name: str,
    template_url: str,
    parameters: list[dict],
) -> None:
    """Create or update a CloudFormation stack and wait for completion.

    Args:
        stack_name: Name of the CloudFormation stack.
        template_url: S3 URL of the template.
        parameters: List of parameter dicts with ParameterKey/ParameterValue.
    """
    common_args = {
        "StackName": stack_name,
        "TemplateURL": template_url,
        "Parameters": parameters,
        "Capabilities": ["CAPABILITY_NAMED_IAM"],
    }

    if stack_exists(stack_name):
        print(f"Updating existing stack: {stack_name}")
        try:
            cf_client.update_stack(**common_args)
            waiter = cf_client.get_waiter("stack_update_complete")
        except ClientError as e:
            if "No updates are to be performed" in str(e):
                print("No updates needed.")
                return
            raise
    else:
        print(f"Creating new stack: {stack_name}")
        cf_client.create_stack(**common_args)
        waiter = cf_client.get_waiter("stack_create_complete")

    print("Waiting for stack operation to complete...")
    start_time = time.time()

    waiter.wait(
        StackName=stack_name,
        WaiterConfig={"Delay": 15, "MaxAttempts": 120},
    )

    elapsed = time.time() - start_time
    print(f"Stack operation completed in {elapsed:.0f}s")

    response = cf_client.describe_stacks(StackName=stack_name)
    stack = response["Stacks"][0]
    print(f"Stack status: {stack['StackStatus']}")

    if stack.get("Outputs"):
        print("\nOutputs:")
        for output in stack["Outputs"]:
            print(f"  {output['OutputKey']}: {output['OutputValue']}")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Deploy data processing CloudFormation stack"
    )
    parser.add_argument(
        "--stack-name",
        default="data-processing-pipeline",
        help="CloudFormation stack name",
    )
    parser.add_argument(
        "--template-url",
        required=True,
        help="S3 URL of the CloudFormation template",
    )
    parser.add_argument(
        "--deployment-bucket",
        required=True,
        help="S3 bucket containing deployment artifacts",
    )
    parser.add_argument(
        "--batch-job-image",
        required=True,
        help="ECR image URI for encoding check job",
    )
    parser.add_argument(
        "--vpc-id",
        required=True,
        help="VPC ID for Batch and EMR",
    )
    parser.add_argument(
        "--subnet-ids",
        required=True,
        help="Comma-separated subnet IDs",
    )
    parser.add_argument(
        "--emr-key-pair",
        default="",
        help="EC2 key pair for EMR instances",
    )
    parser.add_argument(
        "--environment",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment name (names and cost-allocation tag)",
    )
    parser.add_argument(
        "--cost-center",
        default="data-platform",
        help="CostCenter tag value",
    )
    parser.add_argument(
        "--openlineage-url",
        default="",
        help="OpenLineage HTTP endpoint; empty disables lineage",
    )
    parser.add_argument(
        "--openlineage-spark-version",
        default="1.53.0",
        help="io.openlineage:openlineage-spark_2.12 version",
    )
    _add_emr_bootstrap_arg(parser)
    return parser.parse_args()


def _add_emr_bootstrap_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--emr-bootstrap-key",
        default="",
        help="S3 key of the EMR bootstrap script (empty disables it)",
    )


def main():
    """Entry point for deployment."""
    args = parse_args()

    parameters = [
        {"ParameterKey": "DeploymentBucket", "ParameterValue": args.deployment_bucket},
        {"ParameterKey": "BatchJobImage", "ParameterValue": args.batch_job_image},
        {"ParameterKey": "VpcId", "ParameterValue": args.vpc_id},
        {"ParameterKey": "SubnetIds", "ParameterValue": args.subnet_ids},
        {"ParameterKey": "EmrKeyPair", "ParameterValue": args.emr_key_pair},
        {"ParameterKey": "Environment", "ParameterValue": args.environment},
        {"ParameterKey": "CostCenter", "ParameterValue": args.cost_center},
        {"ParameterKey": "OpenLineageUrl", "ParameterValue": args.openlineage_url},
        {
            "ParameterKey": "EmrBootstrapScriptKey",
            "ParameterValue": args.emr_bootstrap_key,
        },
        {
            "ParameterKey": "OpenLineageSparkVersion",
            "ParameterValue": args.openlineage_spark_version,
        },
    ]

    try:
        deploy_stack(args.stack_name, args.template_url, parameters)
    except ClientError as e:
        print(f"Deployment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
