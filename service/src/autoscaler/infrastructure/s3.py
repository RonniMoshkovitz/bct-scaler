"""`ReadObject` over S3. Returns raw bytes; knows nothing about what the objects contain."""
from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError

from autoscaler.application.exceptions import ObjectUnavailableError

# Errors that repeat on every attempt: S3 is strongly consistent, and seed.py uploads every
# object before enqueueing its message, so a missing object stays missing.
_PERMANENT_ERROR_CODES = {"NoSuchKey", "NoSuchBucket", "AccessDenied"}


def read_object(s3_client: Any, bucket: str, key: str) -> bytes:
    try:
        return s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in _PERMANENT_ERROR_CODES:
            raise ObjectUnavailableError(f"s3://{bucket}/{key}: {exc}") from exc
        raise
