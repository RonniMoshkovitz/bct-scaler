"""Plain function signatures and the basic QueueMessage type."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from autoscaler.domain.models import Decision


@dataclass(frozen=True)
class QueueMessage:
    """One delivery of a queue message, independent of the queue technology."""

    body: str
    receive_count: int
    ack: Callable[[], None]


# (bucket, key) -> the object's raw bytes.
ReadObject = Callable[[str, str], bytes]

# Waits for and returns the next batch of messages. An empty batch means nothing arrived.
ReceiveMessages = Callable[[], Sequence[QueueMessage]]

# Persists one cluster's decisions.
WriteDecisions = Callable[[Sequence[Decision]], None]
