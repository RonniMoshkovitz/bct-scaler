from autoscaler.domain.metrics import get_metric_value


def test_top_level_field():
    assert get_metric_value({"numInputRows": 1234}, "numInputRows") == 1234.0


def test_source_metric_coerced_from_string():
    raw = {"sources": [{"metrics": {"numBytesOutstanding": "5000000000"}}]}
    assert get_metric_value(raw, "source.numBytesOutstanding") == 5000000000.0


def test_missing_sources_key():
    assert get_metric_value({}, "source.numBytesOutstanding") is None


def test_unparseable_string_is_none():
    raw = {"sources": [{"metrics": {"numBytesOutstanding": "not-a-number"}}]}
    assert get_metric_value(raw, "source.numBytesOutstanding") is None
