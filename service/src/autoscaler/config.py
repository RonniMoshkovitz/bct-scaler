"""Service configuration, read once from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    aws_endpoint_url: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_region: str
    s3_bucket: str
    sqs_queue_name: str
    ddb_table_name: str
    manifest_s3_key: str
    rules_s3_key: str
    sqs_wait_time_seconds: int
    sqs_max_messages: int
    max_receive_attempts: int
    log_level: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            aws_endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY") or None,
            aws_region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            s3_bucket=os.environ.get("S3_BUCKET", "spark-driver-logs"),
            sqs_queue_name=os.environ.get("SQS_QUEUE_NAME", "driver-log-files"),
            ddb_table_name=os.environ.get("DDB_TABLE_NAME", "scaling_decisions"),
            manifest_s3_key=os.environ.get("MANIFEST_S3_KEY", "clusters.json"),
            rules_s3_key=os.environ.get("RULES_S3_KEY", "config/rules.yaml"),
            sqs_wait_time_seconds=_int_env("SQS_WAIT_TIME_SECONDS", 10),
            sqs_max_messages=_int_env("SQS_MAX_MESSAGES", 10),
            # Deliveries of a failing log file before it's skipped (production: dead-letter queue).
            max_receive_attempts=_int_env("MAX_RECEIVE_ATTEMPTS", 5),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name}={raw!r} must be an integer") from exc
