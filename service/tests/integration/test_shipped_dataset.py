"""The whole use case: given the assignment's own original data/ and rules.yaml, exactly as
seed.py uploads them, the run must produce exactly these decisions.
Depends on the original data/ and rules.yaml (if either changes, this test will need updating).

Runs against the in-memory port fakes (no AWS).
"""
import json
from pathlib import Path

import pytest
from fakes import FakeDecisionSink, FakeObjectStore, FakeWorkQueue

from autoscaler.application import InputLocations, run_autoscaler

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data"
BUCKET = "spark-driver-logs"
INPUTS = InputLocations(bucket=BUCKET, manifest_key="clusters.json", rules_key="config/rules.yaml")

EXPECTED = [
    ("cluster-aurora", "2025-11-03T09:38:30.000Z", "scale_up_slow_batches", 4, 8),
    ("cluster-aurora", "2025-11-03T09:58:30.000Z", "scale_up_backlog_surge", 8, 16),
    ("cluster-aurora", "2025-11-03T11:28:30.000Z", "scale_down_idle", 16, 2),
    ("cluster-borealis", "2025-11-03T09:24:30.000Z", "scale_up_slow_batches", 4, 8),
    ("cluster-ember", "2025-11-03T08:08:30.000Z", "scale_down_idle", 8, 2),
    ("cluster-ember", "2025-11-03T09:28:30.000Z", "scale_up_backlog_surge", 2, 16),
    ("cluster-ember", "2025-11-03T11:08:30.000Z", "scale_down_idle", 16, 2),
]

pytestmark = pytest.mark.skipif(not DATA_DIR.is_dir(), reason="assignment data/ not present")


def test_shipped_dataset_produces_exactly_the_expected_decisions():
    store = FakeObjectStore()
    store.objects[(BUCKET, INPUTS.manifest_key)] = (DATA_DIR / "clusters.json").read_bytes()
    store.objects[(BUCKET, INPUTS.rules_key)] = (REPO_ROOT / "rules.yaml").read_bytes()
    notifications = []
    for path in sorted((DATA_DIR / "logs").rglob("*.log.gz")):
        cluster_id, key = path.parent.name, f"logs/{path.parent.name}/{path.name}"
        store.objects[(BUCKET, key)] = path.read_bytes()
        notifications.append(json.dumps({"bucket": BUCKET, "key": key, "cluster_id": cluster_id}))
    queue, sink = FakeWorkQueue([notifications]), FakeDecisionSink()

    summary = run_autoscaler(
        read_object=store.read,
        receive_messages=queue.receive,
        write_decisions=sink.write,
        inputs=INPUTS,
        max_receive_attempts=5,
    )

    produced = sorted(
        (d.cluster_id, d.decision_ts, d.rule_name, d.from_workers, d.to_workers)
        for d in sink.decisions
    )
    assert produced == EXPECTED
    assert (summary.clusters, summary.decisions, summary.skipped_files) == (5, 7, ())
