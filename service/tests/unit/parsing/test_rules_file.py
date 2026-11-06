from pathlib import Path

import pytest

from autoscaler.parsing.rules_file import parse_rules

FIXTURES_DIR = Path(__file__).parents[2] / "fixtures"


def test_preserves_file_order_and_fields():
    rules = parse_rules((FIXTURES_DIR / "rules_sample.yaml").read_bytes())
    assert [r.name for r in rules] == ["scale_up_slow_batches", "scale_down_idle"]
    assert rules[0].aggregation == "avg"
    assert rules[0].operator == "gt"
    assert rules[0].threshold == 300000
    assert rules[1].target_workers == 2


def _one_rule(aggregation: str, operator: str) -> bytes:
    return (
        f"rules:\n  - {{name: r, metric: m, aggregation: {aggregation}, operator: {operator},"
        " threshold: 1, window_minutes: 1, min_batches: 1, target_workers: 1, cooldown_minutes: 1}"
    ).encode()


@pytest.mark.parametrize(
    ("aggregation", "operator", "error"),
    [("median", "gt", "unsupported aggregation"), ("avg", "gte", "unsupported operator")],
    ids=["unknown-aggregation", "unknown-operator"],
)
def test_unsupported_aggregation_or_operator_fails_loudly(aggregation, operator, error):
    with pytest.raises(ValueError, match=error):
        parse_rules(_one_rule(aggregation, operator))
