"""Connection and acess to AWS services using boto3."""

from __future__ import annotations

from functools import cached_property
from typing import Any

import boto3
from botocore.config import Config

from autoscaler.config import Settings

_RETRY_CONFIG = Config(retries={"max_attempts": 5, "mode": "standard"})


class AwsClients:
    """One boto3 session for the process, with each client built on first access and reused
    after that, so every call goes through the same client and its pooled connections."""

    def __init__(self, settings: Settings) -> None:
        self._session = boto3.session.Session(
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )
        self._options = {"endpoint_url": settings.aws_endpoint_url, "config": _RETRY_CONFIG}

    @cached_property
    def s3(self) -> Any:
        """A plain S3 client."""
        return self._session.client("s3", **self._options)

    @cached_property
    def sqs(self) -> Any:
        """The SQS resource."""
        return self._session.resource("sqs", **self._options)

    @cached_property
    def dynamodb(self) -> Any:
        """The DynamoDB resource."""
        return self._session.resource("dynamodb", **self._options)
