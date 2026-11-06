"""The whole run against in-memory ports (no AWS, no moto): first-message gating, files arriving
over several batches, clusters with no files, and finalizing each cluster exactly once.
Per-message outcomes are covered in test_ingestion.py.
"""
import json

from fakes import FakeDecisionSink, FakeObjectStore, FakeWorkQueue

from autoscaler.application import InputLocations, RunSummary, run_autoscaler

BUCKET = "spark-driver-logs"
INPUTS = InputLocations(bucket=BUCKET, manifest_key="clusters.json", rules_key="config/rules.yaml")

RULES_YAML = """
rules:
  - name: scale_up_slow_batches
    metric: batchDuration
    aggregation: avg
    operator: gt
    threshold: 300000
    window_minutes: 15
    min_batches: 3
    target_workers: 8
    cooldown_minutes: 20
"""


def _progress_line(minute: int, batch_id: int) -> str:
    payload = {
        "id": "q1",
        "timestamp": f"2025-11-03T09:{minute:02d}:00.000Z",
        "batchId": batch_id,
        "batchDuration": 400000,
    }
    return f"25/11/03 09:{minute:02d}:00 INFO X: Streaming query made progress: {json.dumps(payload)}"


def _log(*batches: tuple[int, int]) -> str:
    return "\n".join(_progress_line(minute, batch_id) for minute, batch_id in batches)


def _key(cluster_id: str, name: str) -> str:
    return f"logs/{cluster_id}/{name}.log.gz"


def _notification(cluster_id: str, name: str) -> str:
    return json.dumps({"bucket": BUCKET, "key": _key(cluster_id, name), "cluster_id": cluster_id})


def _seed(store: FakeObjectStore, clusters: dict[str, int], logs: dict[tuple[str, str], str]) -> None:
    store.put_json(
        BUCKET,
        INPUTS.manifest_key,
        {
            "clusters": [
                {"cluster_id": cid, "initial_workers": 4, "num_log_files": n}
                for cid, n in clusters.items()
            ]
        },
    )
    store.put_text(BUCKET, INPUTS.rules_key, RULES_YAML)
    for (cluster_id, name), text in logs.items():
        store.put_log(BUCKET, _key(cluster_id, name), text)


# Cluster "c": three slow batches split over two files -> one 4->8 decision at 09:04.
C_LOGS = {("c", "a"): _log((0, 1), (2, 2)), ("c", "b"): _log((4, 3))}


def _run(store, queue, sink) -> RunSummary:
    return run_autoscaler(
        read_object=store.read,
        receive_messages=queue.receive,
        write_decisions=sink.write,
        inputs=INPUTS,
        max_receive_attempts=5,
    )


def _decision_summary(sink: FakeDecisionSink) -> list[tuple]:
    return [(d.cluster_id, d.decision_ts, d.from_workers, d.to_workers) for d in sink.decisions]


def test_all_files_arrived_over_several_batches_produces_the_decision():
    store, sink = FakeObjectStore(), FakeDecisionSink()
    _seed(store, {"c": 2}, C_LOGS)
    queue = FakeWorkQueue([[_notification("c", "b")], [_notification("c", "a")]])

    summary = _run(store, queue, sink)

    assert (summary.clusters, summary.decisions) == (1, 1)
    assert _decision_summary(sink) == [("c", "2025-11-03T09:04:00.000Z", 4, 8)]
    assert all(message.acked for message in queue.delivered)


def test_manifest_and_rules_are_read_only_after_the_first_message():
    store, sink = FakeObjectStore(), FakeDecisionSink()
    # The store is empty until the first (empty) receive, as when seed.py runs after startup.
    queue = FakeWorkQueue(
        [lambda: _seed(store, {"c": 2}, C_LOGS), [_notification("c", "a"), _notification("c", "b")]]
    )

    _run(store, queue, sink)

    assert len(sink.decisions) == 1


def test_cluster_with_no_log_files_is_finalized_without_waiting_for_a_message():
    store, sink = FakeObjectStore(), FakeDecisionSink()
    _seed(store, {"c": 2, "empty": 0}, C_LOGS)
    queue = FakeWorkQueue([[_notification("c", "a"), _notification("c", "b")]])

    summary = _run(store, queue, sink)  # would poll forever if "empty" were never finalized

    assert (summary.clusters, summary.decisions) == (2, 1)


def test_each_cluster_is_finalized_exactly_once():
    store, sink = FakeObjectStore(), FakeDecisionSink()
    _seed(store, {"c": 2, "d": 1}, {**C_LOGS, ("d", "a"): _log((0, 1), (2, 2), (4, 3))})
    queue = FakeWorkQueue(
        [
            [_notification("c", "a"), _notification("c", "b")],
            [_notification("c", "b")],  # late redelivery after "c" is already finalized
            [_notification("d", "a")],
        ]
    )

    summary = _run(store, queue, sink)

    assert [batch[0].cluster_id for batch in sink.writes] == ["c", "d"]
    assert summary.decisions == 2
