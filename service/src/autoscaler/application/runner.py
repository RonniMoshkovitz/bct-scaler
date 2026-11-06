"""Autoscaler full run pipeline.
The run starts by waiting for the first message. That message is what proves the input data
exists, so the scope (manifest + rules) is loaded only after
that point, with no retries.
"""
from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from itertools import chain

from autoscaler.application.decisions import finalize_cluster
from autoscaler.application.ingestion import ingest_message
from autoscaler.application.ports import QueueMessage, ReadObject, ReceiveMessages, WriteDecisions
from autoscaler.application.scope import InputLocations, load_scope
from autoscaler.domain import cluster_tracking
from autoscaler.domain.cluster_tracking import ClusterProgress
from autoscaler.domain.models import Rule

logger = logging.getLogger(__name__)

_WAITING_LOG_INTERVAL_SECONDS = 60.0


@dataclass(frozen=True)
class RunSummary:
    clusters: int
    decisions: int
    skipped_files: tuple[str, ...]  # corrupt, or unreadable after max_receive_attempts deliveries


def run_autoscaler(
    *,
    read_object: ReadObject,
    receive_messages: ReceiveMessages,
    write_decisions: WriteDecisions,
    inputs: InputLocations,
    max_receive_attempts: int,
    clock: Callable[[], float] = time.monotonic,
) -> RunSummary:
    """Runs until every cluster is finalized (there is no time limit,
    the run ends only when it's done or an error is raised)."""
    if max_receive_attempts < 1:
        raise ValueError("max_receive_attempts must be at least 1")

    rules, clusters_tracker, batches = _set_up_run(read_object, receive_messages, inputs, clock)

    _finalize_clusters_without_files(clusters_tracker)

    decisions_written = 0
    for batch in batches:
        for message in batch:
            progress = ingest_message(message, clusters_tracker, read_object, max_receive_attempts)
            if progress is not None and progress.complete:
                decisions_written += len(finalize_cluster(progress, rules, write_decisions))
        if cluster_tracking.all_finalized(clusters_tracker):
            break

    return RunSummary(
        clusters=len(clusters_tracker),
        decisions=decisions_written,
        skipped_files=tuple(cluster_tracking.skipped_files(clusters_tracker)),
    )


def _set_up_run(
    read_object: ReadObject,
    receive_messages: ReceiveMessages,
    inputs: InputLocations,
    clock: Callable[[], float],
) -> tuple[tuple[Rule, ...], dict[str, ClusterProgress], Iterator[Sequence[QueueMessage]]]:
    """Waits for the first message, then loads the scope and starts tracking its clusters.
    Returns the rules, the clusters being tracked, and every batch still to process (starting
    with that first one).
    """
    clusters_tracker: dict[str, ClusterProgress] = {}
    batches = _non_empty_batches(receive_messages, clock, lambda: _waiting_for(clusters_tracker))

    first_batch = next(batches)  # blocks until the first message arrives.
    scope = load_scope(read_object, inputs)
    clusters_tracker.update(cluster_tracking.start_tracking_clusters(scope.clusters))
    logger.info("starting: %d clusters, %d rules", len(scope.clusters), len(scope.rules))

    return scope.rules, clusters_tracker, chain([first_batch], batches)


def _finalize_clusters_without_files(clusters_tracker: dict[str, ClusterProgress]) -> None:
    """Finalizes clusters whose manifest lists no log files (they have no records,
    so no decisions can be made)."""
    for progress in clusters_tracker.values():
        if progress.complete:
            logger.info("%s has no log files; nothing to evaluate", progress.spec.cluster_id)
            progress.mark_finalized()


def _non_empty_batches(
    receive_messages: ReceiveMessages,
    clock: Callable[[], float],
    waiting_for: Callable[[], str],
) -> Iterator[Sequence[QueueMessage]]:
    """Yields every non-empty batch, polling forever. While nothing arrives, logs what the run is
    waiting for at most once per interval, so a long wait is visible but not noisy."""
    started = last_log = clock()
    while True:
        if batch := receive_messages():
            yield batch
        elif (now := clock()) - last_log >= _WAITING_LOG_INTERVAL_SECONDS:
            logger.info("still waiting for %s (%.0fs so far)", waiting_for(), now - started)
            last_log = now


def _waiting_for(clusters: dict[str, ClusterProgress]) -> str:
    if not clusters:
        return "the first message"
    return f"files; pending clusters: {cluster_tracking.pending_clusters(clusters)}"
