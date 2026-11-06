"""Writes decisions into a pre-created DynamoDB table."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict
from typing import Any

from autoscaler.domain.models import Decision


def write_decisions(table: Any, decisions: Sequence[Decision]) -> None:
    with table.batch_writer() as batch:
        for decision in decisions:
            # Decision's fields *are* the table's item schema, so it maps 1:1.
            batch.put_item(Item=asdict(decision))
