"""Receives messages over SQS. Waits for the queue to exist, then long-polls batches of messages.
The service handles each message sequentially and acks it individually.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from botocore.exceptions import ClientError

from autoscaler.application.ports import QueueMessage

logger = logging.getLogger(__name__)

_QUEUE_MISSING_CODES = {"AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"}
_QUEUE_POLL_INTERVAL_SECONDS = 1.0
_WAITING_LOG_INTERVAL_SECONDS = 60.0
_RECEIVE_COUNT_ATTRIBUTE = "ApproximateReceiveCount"


def wait_for_queue(sqs_resource: Any, queue_name: str) -> Any:
    """Waits with no time limit until the queue exists, logging once a minute.
    Any error other than "queue does not exist" is raised immediately.
    """
    started = time.monotonic()
    last_log: float | None = None
    while True:
        try:
            return sqs_resource.get_queue_by_name(QueueName=queue_name)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code", "") not in _QUEUE_MISSING_CODES:
                raise
        now = time.monotonic()
        if last_log is None or now - last_log >= _WAITING_LOG_INTERVAL_SECONDS:
            logger.info(
                "queue %r does not exist yet; waiting for seed.py (%.0fs so far)",
                queue_name, now - started,
            )
            last_log = now
        time.sleep(_QUEUE_POLL_INTERVAL_SECONDS)


def receive_messages(queue: Any, max_messages: int, wait_time_seconds: int) -> list[QueueMessage]:
    messages = queue.receive_messages(
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=wait_time_seconds,
        MessageSystemAttributeNames=[_RECEIVE_COUNT_ATTRIBUTE],
    )
    return [_to_queue_message(message) for message in messages]


def _to_queue_message(message: Any) -> QueueMessage:
    return QueueMessage(
        body=message.body,
        # SQS's own delivery counter (the one a redrive policy uses), so it survives restarts.
        receive_count=int((message.attributes or {}).get(_RECEIVE_COUNT_ATTRIBUTE, 1)),
        ack=message.delete,
    )
