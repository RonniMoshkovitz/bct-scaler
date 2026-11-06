"""The outcomes of processing a single delivery: what gets acked, read and recorded."""
import json

import pytest
from fakes import FakeDelivery, FakeObjectStore

from autoscaler.application.ingestion import ingest_message
from autoscaler.domain.cluster_tracking import ClusterProgress, start_tracking_clusters
from autoscaler.domain.models import ClusterSpec

BUCKET = "b"
KEY = "logs/c/a.log.gz"
LOG = (
    "25/11/03 09:00:00 INFO X: Streaming query made progress: "
    '{"id": "q", "batchId": 1, "timestamp": "2025-11-03T09:00:00.000Z"}'
)


def _body(cluster_id: str = "c", key: str = KEY) -> str:
    return json.dumps({"bucket": BUCKET, "key": key, "cluster_id": cluster_id})


@pytest.fixture
def clusters() -> dict[str, ClusterProgress]:
    return start_tracking_clusters([ClusterSpec("c", initial_workers=4, num_log_files=2)])


@pytest.fixture
def store() -> FakeObjectStore:
    store = FakeObjectStore()
    store.put_log(BUCKET, KEY, LOG)
    return store


def _ingest(delivery, clusters, store, max_receive_attempts=3):
    return ingest_message(delivery.as_message(), clusters, store.read, max_receive_attempts)


@pytest.mark.parametrize(
    "body", ["not json", _body(cluster_id="unknown")], ids=["malformed", "unknown-cluster"]
)
def test_unusable_message_is_acked_dropped_and_never_read(body, clusters, store):
    delivery = FakeDelivery(body)

    assert _ingest(delivery, clusters, store) is None
    assert delivery.acked
    assert store.reads == []


def test_duplicate_delivery_is_acked_without_reading_again(clusters, store):
    _ingest(FakeDelivery(_body()), clusters, store)
    duplicate = FakeDelivery(_body())

    assert _ingest(duplicate, clusters, store) is None
    assert duplicate.acked
    assert store.reads == [(BUCKET, KEY)]
    assert clusters["c"].files_received == 1


def test_failure_below_the_limit_is_left_unacked_for_redelivery(clusters, store):
    delivery = FakeDelivery(_body(key="logs/c/missing.log.gz"), receive_count=2)

    assert _ingest(delivery, clusters, store, max_receive_attempts=3) is None
    assert not delivery.acked
    assert clusters["c"].files_received == 0


def test_corrupt_file_is_skipped_and_acked_on_the_first_delivery(clusters, store):
    corrupt = "logs/c/corrupt.log.gz"
    store.objects[(BUCKET, corrupt)] = b"not gzip"  # same bytes on every delivery
    delivery = FakeDelivery(_body(key=corrupt), receive_count=1)

    progress = _ingest(delivery, clusters, store, max_receive_attempts=3)

    assert progress is clusters["c"]
    assert delivery.acked
    assert progress.skipped_files == [corrupt]


def test_failure_at_the_limit_skips_the_file_and_acks(clusters, store):
    missing = "logs/c/missing.log.gz"
    delivery = FakeDelivery(_body(key=missing), receive_count=3)

    progress = _ingest(delivery, clusters, store, max_receive_attempts=3)

    assert progress is clusters["c"]
    assert delivery.acked
    assert progress.files_received == 1  # counts toward completion...
    assert progress.skipped_files == [missing]  # but is reported as skipped
    assert progress.final_records() == []
