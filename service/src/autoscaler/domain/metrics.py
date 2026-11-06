"""Metric lookup on a progress record, as referenced by a rule's `metric` field."""
from __future__ import annotations

from typing import Any

_SOURCE_PREFIX = "source."


def get_metric_value(raw: dict[str, Any], metric_path: str) -> float | None:
    """Looks up a rule's metric in a record's raw JSON. Top-level fields are direct lookups. 
    `source.<name>` means `sources[0].metrics.<name>`. Any missing or malformed piece along the way degrades to None.
    """
    if (value := _get_raw_metric_value(raw, metric_path)) is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_raw_metric_value(raw: dict[str, Any], metric_path: str) -> object:
    if metric_path.startswith(_SOURCE_PREFIX):
        return _get_raw_source_metric(raw, metric_path.removeprefix(_SOURCE_PREFIX))
    return raw.get(metric_path)


def _get_raw_source_metric(raw: dict[str, Any], name: str) -> object:
    if (not isinstance(sources := raw.get("sources"), list) or
        not sources or
        not isinstance(first_source := sources[0], dict) or
        not isinstance(metrics := first_source.get("metrics"), dict)):
        return None
    return metrics.get(name)
