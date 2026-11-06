from datetime import UTC, datetime, timedelta

import pytest

from autoscaler.domain.models import ProgressRecord, Rule
from autoscaler.domain.rules_engine import evaluate_cluster

_BASE_TS = datetime(2025, 11, 3, 9, 0, 0, tzinfo=UTC)


def _record(batch_id: int, minute_offset: float, **fields) -> ProgressRecord:
    ts = _BASE_TS + timedelta(minutes=minute_offset)
    ts_raw = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    raw = {"id": "q1", "batchId": batch_id, "timestamp": ts_raw, **fields}
    return ProgressRecord(id="q1", batch_id=batch_id, timestamp=ts, timestamp_raw=ts_raw, raw=raw)


def test_readme_worked_example():
    """This test runs a case shown in the README, at section 6.5 as an example under decision semantics."""
    rule_a = Rule(
        name="A", metric="batchDuration", aggregation="avg", operator="gt", threshold=300000,
        window_minutes=15, min_batches=3, target_workers=8, cooldown_minutes=20,
    )
    rule_b = Rule(
        name="B", metric="numInputRows", aggregation="max", operator="lt", threshold=1000,
        window_minutes=30, min_batches=5, target_workers=2, cooldown_minutes=60,
    )
    # 09:26 is 26 minutes after _BASE_TS (09:00), 09:40's batchDuration brings the avg to 344000.
    durations = {minute: 300000 for minute in range(26, 40, 2)} | {40: 652000}
    durations |= {minute: 652000 for minute in range(42, 61, 2)}
    records = [
        _record(batch_id, minute, batchDuration=duration, numInputRows=50000)
        for batch_id, (minute, duration) in enumerate(sorted(durations.items()))
    ]

    decisions = evaluate_cluster("c", records, [rule_a, rule_b], initial_workers=4)

    assert [(d.decision_ts, d.rule_name, d.from_workers, d.to_workers) for d in decisions] == [
        ("2025-11-03T09:40:00.000Z", "A", 4, 8),
    ]


@pytest.mark.parametrize("operator", ["gt", "lt"])
def test_aggregate_equal_to_the_threshold_does_not_fire(operator):
    rule = Rule(
        name="r", metric="batchDuration", aggregation="last", operator=operator, threshold=500,
        window_minutes=10, min_batches=1, target_workers=8, cooldown_minutes=0,
    )
    records = [_record(0, 0, batchDuration=500)]

    assert evaluate_cluster("c", records, [rule], initial_workers=4) == []


def test_last_skips_a_newest_record_that_lacks_the_metric():
    """Last is the newest record among those carrying the metric."""
    hold = Rule(
        name="hold", metric="spike", aggregation="last", operator="gt", threshold=0,
        window_minutes=1, min_batches=1, target_workers=4, cooldown_minutes=0,
    )
    rule = Rule(
        name="r", metric="batchDuration", aggregation="last", operator="gt", threshold=500,
        window_minutes=10, min_batches=1, target_workers=8, cooldown_minutes=0,
    )
    records = [
        _record(0, 0, spike=1, batchDuration=0),
        _record(1, 2, spike=1, batchDuration=1000),
        _record(2, 4),  # the newest record in the window, without batchDuration
    ]

    decisions = evaluate_cluster("c", records, [hold, rule], initial_workers=4)

    # At record 2, `last` is record 1's 1000 (> 500): the metric-less record 2 is passed over.
    assert [(d.decision_ts, d.rule_name) for d in decisions] == [(records[2].timestamp_raw, "r")]


def test_window_left_boundary_is_strictly_exclusive():
    """ts == T - window_minutes must be excluded (strictly greater on the left)."""
    rule = Rule(
        name="r", metric="batchDuration", aggregation="avg", operator="gt", threshold=500,
        window_minutes=10, min_batches=1, target_workers=8, cooldown_minutes=0,
    )
    early = _record(0, 0, batchDuration=0)      # would drag the average below threshold if included
    late = _record(1, 10, batchDuration=600)    # exactly window_minutes later than `early`

    decisions = evaluate_cluster("c", [early, late], [rule], initial_workers=4)

    # If `early` were wrongly included, avg((0+600)/2)=300 would not exceed 500 and nothing
    # would fire. it's correctly excluded, so the window is just [600] -> avg 600 > 500.
    assert len(decisions) == 1
    assert decisions[0].decision_ts == late.timestamp_raw


def test_window_right_edge_excludes_later_records():
    """Only ts <= T is in the window. the full record set is searched, but never past T."""
    rule = Rule(
        name="r", metric="batchDuration", aggregation="last", operator="gt", threshold=500,
        window_minutes=10, min_batches=1, target_workers=8, cooldown_minutes=0,
    )
    records = [_record(0, 0, batchDuration=1000), _record(1, 2, batchDuration=0)]

    decisions = evaluate_cluster("c", records, [rule], initial_workers=4)

    # Had the later record leaked into record 0's window, `last` would be 0 and nothing fires.
    assert [d.decision_ts for d in decisions] == [records[0].timestamp_raw]


def test_records_sharing_a_timestamp_window_together_and_last_takes_the_higher_batch_id():
    """Two records at the same T are both in each window (ts <= T), and `last` is the one with
    the greatest (timestamp, batchId), i.e. the higher batchId, at either evaluation point."""
    rule = Rule(
        name="r", metric="batchDuration", aggregation="last", operator="gt", threshold=500,
        window_minutes=10, min_batches=2, target_workers=8, cooldown_minutes=0,
    )
    records = [_record(1, 0, batchDuration=1000), _record(2, 0, batchDuration=0)]

    # Both points see [1000, 0] (so min_batches=2 holds even at the first).
    # Last is batch 2's 0, not batch 1's 1000, so the rule never fires.
    assert evaluate_cluster("c", records, [rule], initial_workers=4) == []


def test_min_batches_excludes_records_missing_the_metric():
    """A record without the rule's metric is still a valid evaluation point, but must not
    count toward min_batches for that rule.
    """
    rule = Rule(
        name="r", metric="batchDuration", aggregation="avg", operator="gt", threshold=500,
        window_minutes=60, min_batches=3, target_workers=8, cooldown_minutes=0,
    )
    records = [
        _record(0, 0, batchDuration=1000),
        _record(1, 2),                      # no batchDuration at all
        _record(2, 4, batchDuration=1000),
        _record(3, 6, batchDuration=1000),
    ]

    decisions = evaluate_cluster("c", records, [rule], initial_workers=4)

    # Carrying-the-metric count only reaches 3 at the 4th record (0, 2, 3 carry it, 1 doesn't).
    assert len(decisions) == 1
    assert decisions[0].decision_ts == records[3].timestamp_raw


def test_first_matching_rule_in_list_order_wins():
    record = _record(0, 0, batchDuration=1000, numInputRows=1000)
    rule_a = Rule(
        name="A", metric="batchDuration", aggregation="last", operator="gt", threshold=500,
        window_minutes=60, min_batches=1, target_workers=8, cooldown_minutes=0,
    )
    rule_b = Rule(
        name="B", metric="numInputRows", aggregation="last", operator="gt", threshold=500,
        window_minutes=60, min_batches=1, target_workers=5, cooldown_minutes=0,
    )
    # Both conditions hold. Whichever is listed first must be the one that matches.
    decisions_a_first = evaluate_cluster("c", [record], [rule_a, rule_b], initial_workers=4)
    decisions_b_first = evaluate_cluster("c", [record], [rule_b, rule_a], initial_workers=4)

    assert decisions_a_first[0].rule_name == "A"
    assert decisions_a_first[0].to_workers == 8
    assert decisions_b_first[0].rule_name == "B"
    assert decisions_b_first[0].to_workers == 5


def test_idempotence_gate_blocks_repeat_to_the_same_target():
    rule = Rule(
        name="A", metric="batchDuration", aggregation="avg", operator="gt", threshold=500,
        window_minutes=60, min_batches=1, target_workers=8, cooldown_minutes=0,
    )
    records = [_record(0, 0, batchDuration=1000), _record(1, 2, batchDuration=1000)]

    decisions = evaluate_cluster("c", records, [rule], initial_workers=4)

    assert len(decisions) == 1  # only the first record's match produces a decision
    assert decisions[0].to_workers == 8


def test_cooldown_gate_blocks_then_passes_at_the_exact_boundary():
    """Isolates the cooldown gate from idempotence: rule B's target (3) always differs from
    whatever rule A last set `current_workers` to, so idempotence can never be the reason a
    decision is skipped here — only cooldown can be.
    """
    rule_a = Rule(  # matches once, at record 0 only (its metric only exists on that record)
        name="A", metric="spike", aggregation="last", operator="gt", threshold=0,
        window_minutes=1, min_batches=1, target_workers=8, cooldown_minutes=0,
    )
    rule_b = Rule(
        name="B", metric="numInputRows", aggregation="last", operator="gt", threshold=500,
        window_minutes=60, min_batches=1, target_workers=3, cooldown_minutes=20,
    )
    before_boundary = _record(1, 2, numInputRows=1000)     # 2 min after record 0: inside cooldown
    at_boundary = _record(2, 20, numInputRows=1000)        # exactly 20 min after record 0's decision
    records = [
        _record(0, 0, spike=999, numInputRows=1000),
        before_boundary,
        at_boundary,
    ]

    decisions = evaluate_cluster("c", records, [rule_a, rule_b], initial_workers=4)

    assert len(decisions) == 2
    first, second = decisions
    assert (first.rule_name, first.from_workers, first.to_workers) == ("A", 4, 8)
    assert first.decision_ts == records[0].timestamp_raw
    # `before_boundary` (record 1) must have produced nothing: cooldown still active.
    assert (second.rule_name, second.from_workers, second.to_workers) == ("B", 8, 3)
    assert second.decision_ts == at_boundary.timestamp_raw
