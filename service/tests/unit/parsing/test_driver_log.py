import gzip
from datetime import UTC, datetime

import pytest

from autoscaler.parsing.driver_log import parse_log_file, parse_log_text

_ENTRY = "25/11/03 09:14:00 INFO X: Streaming query made progress: "
_VALID = '{"id": "q", "batchId": 7, "timestamp": "2025-11-03T09:20:00.000Z"}'


def test_sample_file_keeps_valid_records_and_counts_the_malformed_ones(sample_log_text):
    parsed = parse_log_text(sample_log_text)

    # Compact and pretty-printed records parse alike: 100 (twice: a duplicate) and 101. The
    # truncated 102 and the id-less record are malformed; non-progress lines aren't counted.
    assert sorted(r.batch_id for r in parsed.records) == [100, 100, 101]
    assert parsed.malformed == 2


def test_near_miss_marker_with_a_valid_json_body_is_not_a_progress_record():
    near_miss = "25/11/03 09:14:00 INFO X: Streaming query has made progress: " + _VALID

    parsed = parse_log_text(near_miss)

    assert parsed.records == []
    assert parsed.malformed == 0  # not a progress entry at all, so not malformed either


def test_json_timestamp_is_used_never_the_log_line_header():
    parsed = parse_log_text(_ENTRY + _VALID)  # header says 09:14:00, the JSON says 09:20:00

    (record,) = parsed.records
    assert record.timestamp_raw == "2025-11-03T09:20:00.000Z"
    assert record.timestamp == datetime(2025, 11, 3, 9, 20, tzinfo=UTC)


def test_text_before_the_first_header_is_dropped():
    # A file can start mid-entry: the tail of a pretty-printed record from the previous file.
    tail = '  "batchId" : 6,\n  "timestamp" : "2025-11-03T09:10:00.000Z"\n}\n'

    parsed = parse_log_text(tail + _ENTRY + _VALID)

    assert [r.batch_id for r in parsed.records] == [7]
    assert parsed.malformed == 0


def test_malformed_input_never_raises():
    garbage = "not a log file at all\nStreaming query made progress: {not json"
    assert parse_log_text(garbage).records == []


@pytest.mark.parametrize(
    "body",
    [
        "[1, 2, 3]",
        '{"id": "q", "batchId": 1e999, "timestamp": "2025-11-03T09:14:00.000Z"}',
        '{"id": "q", "batchId": "x", "timestamp": "2025-11-03T09:14:00.000Z"}',
        '{"id": "q", "batchId": 1, "timestamp": 12345}',
        '{"id": null, "batchId": 1, "timestamp": "2025-11-03T09:14:00.000Z"}',
    ],
    ids=["no-json-object", "infinite-batch-id", "text-batch-id", "numeric-timestamp", "null-id"],
)
def test_invalid_record_values_are_skipped_and_counted(body):
    parsed = parse_log_text(_ENTRY + body)

    assert parsed.records == []
    assert parsed.malformed == 1


def test_text_after_the_json_object_is_ignored():
    parsed = parse_log_text(_ENTRY + _VALID + " trailing {junk")
    assert [r.batch_id for r in parsed.records] == [7]


def test_parse_log_file_gunzips_before_parsing(sample_log_text):
    data = gzip.compress(sample_log_text.encode("utf-8"))
    assert parse_log_file(data) == parse_log_text(sample_log_text)


@pytest.mark.parametrize(
    "data",
    [b"plain text, not gzip", gzip.compress(b"25/11/03 09:14:00 INFO X: hello")[:-8]],
    ids=["not-gzip", "truncated-gzip"],
)
def test_parse_log_file_raises_value_error_for_a_corrupt_file(data):
    # Not swallowed: the service skips the file (and reports it) instead of retrying it.
    with pytest.raises(ValueError):
        parse_log_file(data)


def test_timestamp_without_an_offset_is_read_as_utc():
    naive = _ENTRY + '{"id": "q", "batchId": 1, "timestamp": "2025-11-03T09:14:00.000"}'
    aware = _ENTRY + '{"id": "q", "batchId": 2, "timestamp": "2025-11-03T09:15:00.000Z"}'

    first, second = parse_log_text(naive + "\n" + aware).records

    assert first.timestamp == datetime(2025, 11, 3, 9, 14, tzinfo=UTC)
    assert first.timestamp_raw == "2025-11-03T09:14:00.000"  # kept verbatim for decision_ts
    assert first.timestamp < second.timestamp  # comparable with tz-aware records, no TypeError
