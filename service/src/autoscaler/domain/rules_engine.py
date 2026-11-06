"""Evaluates scaling rules. It expects the records to already be sorted and deduplicated."""

from __future__ import annotations

import bisect
import operator
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta

from autoscaler.domain.metrics import get_metric_value
from autoscaler.domain.models import Decision, ProgressRecord, Rule

AGGREGATIONS: dict[str, Callable[[list[float]], float]] = {
    "avg": lambda values: sum(values) / len(values),
    "max": max,
    "min": min,
    "last": lambda values: values[-1],
}
OPERATORS: dict[str, Callable[[float, float], bool]] = {"gt": operator.gt, "lt": operator.lt}


def evaluate_cluster(
    cluster_id: str,
    records: list[ProgressRecord],
    rules: Sequence[Rule],
    initial_workers: int,
) -> list[Decision]:
    """Walks one cluster's full, sorted record set and produces its scaling decisions."""
    timestamps = [record.timestamp for record in records]
    current_workers = initial_workers
    last_decision_ts: datetime | None = None
    decisions: list[Decision] = []

    for index, record in enumerate(records):
        matched_rule = None
        for rule in rules:
            if _condition_holds(rule, records, timestamps, index):
                matched_rule = rule
                break

        if (
            matched_rule is None
            or matched_rule.target_workers == current_workers  # idempotence gate
            or _in_cooldown(matched_rule, record.timestamp, last_decision_ts)  # cooldown gate
        ):
            continue

        decisions.append(
            Decision(
                cluster_id=cluster_id,
                decision_ts=record.timestamp_raw,
                rule_name=matched_rule.name,
                from_workers=current_workers,
                to_workers=matched_rule.target_workers,
            )
        )
        current_workers = matched_rule.target_workers
        last_decision_ts = record.timestamp

    return decisions


def _in_cooldown(rule: Rule, ts: datetime, last_decision_ts: datetime | None) -> bool:
    """Checks if it is in the cooldown period after the last decision."""
    return last_decision_ts is not None and ts < last_decision_ts + timedelta(
        minutes=rule.cooldown_minutes
    )


def _condition_holds(
    rule: Rule,
    records: list[ProgressRecord],
    timestamps: list[datetime],
    index: int,
) -> bool:
    """Checks if the condition holds at the current evaluation point."""
    values = _metric_values(rule.metric, _window_records(rule, records, timestamps, index))
    if not values or len(values) < rule.min_batches:
        return False

    aggregate = AGGREGATIONS[rule.aggregation](values)
    return OPERATORS[rule.operator](aggregate, rule.threshold)


def _window_records(
    rule: Rule,
    records: list[ProgressRecord],
    timestamps: list[datetime],
    index: int,
) -> list[ProgressRecord]:
    current_ts = timestamps[index]
    window_start = current_ts - timedelta(minutes=rule.window_minutes)

    lower = bisect.bisect_right(timestamps, window_start)
    upper = bisect.bisect_right(timestamps, current_ts)
    return records[lower:upper]


def _metric_values(metric: str, window: list[ProgressRecord]) -> list[float]:
    """Gets metric value for each record in the window, skipping records that lack it."""
    return [
        value
        for value in (get_metric_value(record.raw, metric) for record in window)
        if value is not None
    ]
