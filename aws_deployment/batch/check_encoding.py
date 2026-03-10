"""AWS Batch job to validate file encoding is UTF-8."""

import os
import sys

import boto3
import chardet

s3_client = boto3.client("s3")

SAMPLE_SIZE = 1024 * 1024  # 1 MB sample for detection


def check_encoding(bucket: str, key: str) -> dict:
    """Download file from S3 and validate encoding.

    Args:
        bucket: S3 bucket name.
        key: S3 object key.

    Returns:
        dict with encoding, confidence, and is_valid fields.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read(SAMPLE_SIZE)

    result = chardet.detect(body)
    encoding = result.get("encoding", "unknown")
    confidence = result.get("confidence", 0.0)

    is_valid = encoding and encoding.lower() in (
        "utf-8", "ascii", "utf-8-sig"
    )

    return {
        "encoding": encoding,
        "confidence": confidence,
        "is_valid": is_valid,
        "bucket": bucket,
        "key": key,
    }


def main():
    """Entry point for Batch job. Reads bucket/key from env vars."""
    bucket = os.environ.get("S3_BUCKET")
    key = os.environ.get("S3_KEY")

    if not bucket or not key:
        print("ERROR: S3_BUCKET and S3_KEY environment variables required")
        sys.exit(1)

    print(f"Checking encoding for s3://{bucket}/{key}")
    result = check_encoding(bucket, key)

    print(f"Encoding: {result['encoding']}")
    print(f"Confidence: {result['confidence']}")
    print(f"Valid UTF-8: {result['is_valid']}")

    if not result["is_valid"]:
        print("ERROR: File is not valid UTF-8 encoding")
        sys.exit(1)

    print("Encoding check passed")


if __name__ == "__main__":
    main()
