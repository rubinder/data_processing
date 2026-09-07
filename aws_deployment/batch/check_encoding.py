"""AWS Batch job to validate file encoding is UTF-8."""

import gzip
import io
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

    # The web server serves gzip CSV and api_pull lands it as-is. Detecting
    # the encoding of the *compressed* bytes is meaningless (chardet returns
    # None and the check fails every real file), so inspect the decompressed
    # text. Decompress only a sample: the file may be large and a 1MB sample
    # of text is plenty for detection. A truncated gzip stream raises
    # EOFError/OSError at the end of the sample; the bytes before that are
    # still what we want.
    if key.endswith(".gz"):
        decompressor = gzip.GzipFile(fileobj=io.BytesIO(body))
        try:
            body = decompressor.read(SAMPLE_SIZE)
        except (EOFError, OSError):
            body = decompressor.read()

    result = chardet.detect(body)
    encoding = result.get("encoding") or "unknown"
    confidence = result.get("confidence") or 0.0

    is_valid = encoding.lower() in ("utf-8", "ascii", "utf-8-sig")

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
