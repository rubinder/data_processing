"""Lambda function triggered by S3 event to start Step Function execution."""

import json
import os
import urllib.parse

import boto3

sfn_client = boto3.client("stepfunctions")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def lambda_handler(event, context):
    """Handle S3 put event and start Step Function execution.

    Args:
        event: S3 event notification payload.
        context: Lambda context object.

    Returns:
        dict with Step Function execution ARN.
    """
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"], encoding="utf-8"
        )
        size = record["s3"]["object"].get("size", 0)

        execution_input = json.dumps({
            "bucket": bucket,
            "key": key,
            "size": size,
        })

        # Execution names must be unique per state machine for 90 days. The
        # key alone is not: re-landing the same object (a retry, a backfill,
        # a corrected file) raised ExecutionAlreadyExists and the pipeline
        # silently never ran for it (seen on a live stack, 2026-09-06). The
        # S3 event sequencer is unique per object change, so suffix with it.
        sequencer = record["s3"]["object"].get("sequencer", "")[-16:]
        base_name = key.replace("/", "_").replace(".", "_")[:60]
        execution_name = f"{base_name}_{sequencer}" if sequencer else base_name

        response = sfn_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_name,
            input=execution_input,
        )

        print(
            f"Started execution {response['executionArn']} "
            f"for s3://{bucket}/{key}"
        )

    return {"statusCode": 200, "body": "Step Function execution started"}
