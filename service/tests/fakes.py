"""In-memory stand-ins for the ports in `autoscaler.application.ports`, for service-level tests.

Each fake is a small class only so the tests can inspect what happened; the service is given
its bound methods (`store.read`, `queue.receive`, `sink.write`), which match the port
signatures.
"""
from __future__ import annotations

import gzip
import json
from collections.abc import Callable, Sequence

from autoscaler.application.ports import QueueMessage
from autoscaler.domain.models import Decision


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.reads: list[tuple[str, str]] = []

    def put_json(self, bucket: str, key: str, payload: object) -> None:
        self.objects[(bucket, key)] = json.dumps(payload).encode("utf-8")

    def put_text(self, bucket: str, key: str, text: str) -> None:
        self.objects[(bucket, key)] = text.encode("utf-8")

    def put_log(self, bucket: str, key: str, text: str) -> None:
        self.objects[(bucket, key)] = gzip.compress(text.encode("utf-8"))

    def read(self, bucket: str, key: str) -> bytes:
        self.reads.append((bucket, key))
        try:
            return self.objects[(bucket, key)]
        except KeyError:
            raise FileNotFoundError(f"{bucket}/{key}") from None


class FakeDelivery:
    """Test-side record of one delivery: what was handed out, and whether it was acked."""

    def __init__(self, body: str, receive_count: int = 1) -> None:
        self.body = body
        self.receive_count = receive_count
        self.acked = False
        self.redelivered = False

    def ack(self) -> None:
        self.acked = True

    def as_message(self) -> QueueMessage:
        return QueueMessage(body=self.body, receive_count=self.receive_count, ack=self.ack)


class FakeWorkQueue:
    """Returns scripted batches in order. Once the script runs out, every message that is
    still un-acked is redelivered on each receive with its receive_count incremented (like SQS
    after the visibility timeout); with nothing left to redeliver, batches are empty.

    A script step may be a callable instead of a list of bodies: it runs when that batch is
    requested (e.g. data arriving while the service waits) and yields an empty batch.
    """

    def __init__(self, script: Sequence[Sequence[str] | Callable[[], None]]) -> None:
        self._script = list(script)
        self.delivered: list[FakeDelivery] = []

    @property
    def acked_bodies(self) -> list[str]:
        return [delivery.body for delivery in self.delivered if delivery.acked]

    def receive(self) -> list[QueueMessage]:
        if self._script:
            step = self._script.pop(0)
            if callable(step):
                step()
                return []
            batch = [FakeDelivery(body) for body in step]
        else:
            batch = []
            for delivery in self.delivered:
                if not delivery.acked and not delivery.redelivered:
                    delivery.redelivered = True
                    batch.append(FakeDelivery(delivery.body, delivery.receive_count + 1))
        self.delivered.extend(batch)
        return [delivery.as_message() for delivery in batch]


class FakeDecisionSink:
    def __init__(self) -> None:
        self.writes: list[list[Decision]] = []

    def write(self, decisions: Sequence[Decision]) -> None:
        self.writes.append(list(decisions))

    @property
    def decisions(self) -> list[Decision]:
        return [decision for batch in self.writes for decision in batch]
