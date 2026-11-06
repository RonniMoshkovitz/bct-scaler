from datetime import UTC, datetime, timedelta

from autoscaler.domain.cluster_tracking import (
    ClusterProgress,
    dedupe,
)
from autoscaler.domain.models import ClusterSpec, ProgressRecord
from autoscaler.parsing.driver_log import parse_log_text

_BASE_TS = datetime(2025, 11, 3, 9, 0, 0, tzinfo=UTC)


def _record(batch_id: int, minute_offset: float, query_id: str = "q1") -> ProgressRecord:
    ts = _BASE_TS + timedelta(minutes=minute_offset)
    ts_raw = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return ProgressRecord(id=query_id, batch_id=batch_id, timestamp=ts, timestamp_raw=ts_raw, raw={})


def test_dedupe_by_id_and_batch_id_keeps_one_copy(sample_log_text):
    unique = dedupe(parse_log_text(sample_log_text).records)
    assert sorted(r.batch_id for r in unique) == [100, 101]


def test_final_records_are_deduped_across_files_and_sorted_by_timestamp_then_batch_id():
    progress = ClusterProgress(ClusterSpec("c", initial_workers=4, num_log_files=2))
    # Files arrive out of chronological order and overlap (rotation repeats batch 2).
    progress.add_file("late.log.gz", [_record(3, 4), _record(2, 2)])
    progress.add_file("early.log.gz", [_record(2, 2), _record(1, 0), _record(0, 0, query_id="q2")])

    records = progress.final_records()

    assert [(r.timestamp_raw[11:16], r.batch_id) for r in records] == [
        ("09:00", 0),
        ("09:00", 1),
        ("09:02", 2),
        ("09:04", 3),
    ]
