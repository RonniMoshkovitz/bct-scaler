"""Parser YAML file containing the scaling rules into validated rules, according to file order."""
from __future__ import annotations

from typing import Any

import yaml

from autoscaler.domain.models import Rule

AGGREGATIONS = ["avg", "max", "min", "last"]
OPERATORS =["gt", "lt"]


def parse_rules(data: bytes) -> list[Rule]:
    """Parses rules file data into a list of rules.
    It raises ValueError on a malformed rules file (the run can't start without it)."""
    payload = yaml.safe_load(data.decode("utf-8"))
    entries = payload.get("rules") if isinstance(payload, dict) else None
    if not isinstance(entries, list) or not entries:
        raise ValueError("rules file must contain a non-empty 'rules' list")
    return [_parse_rule(entry) for entry in entries]


def _parse_rule(entry: Any) -> Rule:
    """Parses a raw rule entry into a Rule"""
    name = entry.get("name") if isinstance(entry, dict) else None
    if not isinstance(name, str) or not name:
        raise ValueError(f"each rule must be a mapping with a name: {entry!r}")

    aggregation = entry.get("aggregation")
    operator = entry.get("operator")
    if aggregation not in AGGREGATIONS:
        raise ValueError(f"rule {name!r} has unsupported aggregation {aggregation!r}")
    if operator not in OPERATORS:
        raise ValueError(f"rule {name!r} has unsupported operator {operator!r}")

    try:
        return Rule(
            name=name,
            metric=str(entry["metric"]),
            aggregation=aggregation,
            operator=operator,
            threshold=float(entry["threshold"]),
            window_minutes=float(entry["window_minutes"]),
            min_batches=int(entry["min_batches"]),
            target_workers=int(entry["target_workers"]),
            cooldown_minutes=float(entry["cooldown_minutes"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"rule {name!r} is missing or has an invalid field: {exc!r}") from exc
