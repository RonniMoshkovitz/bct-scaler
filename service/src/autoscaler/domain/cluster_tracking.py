"""Tracks each cluster's progress, collecting its related data like records."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from autoscaler.domain.models import ClusterSpec, ProgressRecord


@dataclass
class ClusterProgress:
    """Collects one cluster's records until every file listed in the manifest has arrived.
    Rrecords are only sorted once the set is complete.
    """

    spec: ClusterSpec
    _seen_keys: set[str] = field(default_factory=set)
    _skipped_keys: set[str] = field(default_factory=set)
    _records: list[ProgressRecord] = field(default_factory=list)
    _finalized: bool = False

    @property
    def finalized(self) -> bool:
        """Whether this cluster's decisions have been evaluated and written."""
        return self._finalized

    def mark_finalized(self) -> None:
        self._finalized = True

    @property
    def files_received(self) -> int:
        return len(self._seen_keys)

    @property
    def skipped_files(self) -> list[str]:
        return sorted(self._skipped_keys)

    @property
    def complete(self) -> bool:
        return len(self._seen_keys) >= self.spec.num_log_files

    def has_file(self, key: str) -> bool:
        return key in self._seen_keys

    def add_file(self, key: str, records: Iterable[ProgressRecord]) -> None:
        self._records.extend(records)
        self._seen_keys.add(key)

    def skip_file(self, key: str) -> None:
        """Gives up on a file. Marks as seen but contributes no records, instead of waiting forever."""
        self._seen_keys.add(key)
        self._skipped_keys.add(key)

    def final_records(self) -> list[ProgressRecord]:
        """Orders the cluster's records, with deduplicated records, sorted ascending by (timestamp, batch_id)."""
        return sorted(dedupe(self._records), key=lambda record: (record.timestamp, record.batch_id))


def start_tracking_clusters(specs: Iterable[ClusterSpec]) -> dict[str, ClusterProgress]:
    """Builds One ClusterProgress per manifest cluster, keyed by cluster id."""
    return {spec.cluster_id: ClusterProgress(spec) for spec in specs}


def all_finalized(clusters: dict[str, ClusterProgress]) -> bool:
    return all(progress.finalized for progress in clusters.values())


def pending_clusters(clusters: dict[str, ClusterProgress]) -> list[str]:
    return sorted(cid for cid, progress in clusters.items() if not progress.finalized)


def skipped_files(clusters: dict[str, ClusterProgress]) -> list[str]:
    return sorted(key for progress in clusters.values() for key in progress.skipped_files)


def dedupe(records: Iterable[ProgressRecord]) -> list[ProgressRecord]:
    """Deduplicates by (id, batchId). Keeps the first copy."""
    seen: dict[tuple[str, int], ProgressRecord] = {}
    for record in records:
        seen.setdefault((record.id, record.batch_id), record)
    return list(seen.values())
