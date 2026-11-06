"""Domain value objects (Pure data)."""
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ProgressRecord:
    """A single, validated "Streaming query made progress" record."""
    id: str
    batch_id: int
    timestamp: datetime # parsed, tz-aware datetime used for all comparisons
    timestamp_raw: str # exact JSON string, kept for verbatim decision output
    raw: dict[str, Any]


@dataclass(frozen=True)
class Rule:
    name: str
    metric: str
    aggregation: str  # "avg" | "max" | "min" | "last"
    operator: str  # "gt" | "lt"
    threshold: float
    window_minutes: float
    min_batches: int
    target_workers: int
    cooldown_minutes: float


@dataclass(frozen=True)
class Decision:
    cluster_id: str
    decision_ts: str  # verbatim JSON timestamp string of the triggering record
    rule_name: str
    from_workers: int
    to_workers: int


@dataclass(frozen=True)
class ClusterSpec:
    """One cluster as described by the manifest."""
    cluster_id: str
    initial_workers: int
    num_log_files: int


@dataclass(frozen=True)
class LogFileRef:
    """Location of a log file, as announced by a notification."""
    cluster_id: str
    bucket: str
    key: str
