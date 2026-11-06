"""Processes messages from a given queue that contain a reference to a log file into cluster records.
It owns the whole life of a delivery: decide whether the message is usable, fetch and parse its
file, and ack it (or leave it un-acked so the queue redelivers it).
"""
from __future__ import annotations

import logging

from autoscaler.application.exceptions import ObjectUnavailableError
from autoscaler.application.ports import QueueMessage, ReadObject
from autoscaler.domain.cluster_tracking import ClusterProgress
from autoscaler.domain.models import LogFileRef
from autoscaler.parsing.driver_log import parse_log_file
from autoscaler.parsing.notification import parse_notification

logger = logging.getLogger(__name__)


def ingest_message(
    message: QueueMessage,
    clusters: dict[str, ClusterProgress],
    read_object: ReadObject,
    max_receive_attempts: int,
) -> ClusterProgress | None:
    """Processes one delivery. Returns the cluster that gained a file (read, or skipped as
    corrupt or unreadable), or None if the message changed nothing."""
    if (ref := _try_parse_notification(message)) is None:
        return None

    progress = clusters.get(ref.cluster_id)
    if progress is None:
        logger.warning("message for unknown cluster %r, dropping", ref.cluster_id)
    elif progress.has_file(ref.key):
        logger.info("duplicate delivery of %s for %s, discarding", ref.key, ref.cluster_id)
    elif progress.finalized:
        logger.warning("%s is already finalized; ignoring extra file %s", ref.cluster_id, ref.key)
    else:
        _ingest_file(message, ref, progress, read_object, max_receive_attempts)
        return progress if progress.has_file(ref.key) else None

    _ack(message)
    return None


def _try_parse_notification(message: QueueMessage) -> LogFileRef | None:
    """Returns the message's file reference, or None if the body is malformed."""
    try:
        return parse_notification(message.body)
    except ValueError as exc:
        logger.error("dropping message: %s", exc)
        _ack(message)
        return None


def _ack(message: QueueMessage) -> None:
    """Deletes the message, without letting a failed delete stop the run (if redelivered, it's discarded as a duplicate)."""
    try:
        message.ack()
    except Exception:
        logger.exception("failed to ack a message; it will be redelivered and discarded")


def _ingest_file(
    message: QueueMessage,
    ref: LogFileRef,
    progress: ClusterProgress,
    read_object: ReadObject,
    max_receive_attempts: int,
) -> None:
    """Fetches and parses one log file into its cluster. A file that can never be read is skipped
    at once, any other failed read is left for redelivery. A file is skipped once it has
    failed max_receive_attempts deliveries."""
    try:
        data = read_object(ref.bucket, ref.key)
    except ObjectUnavailableError:
        logger.exception(
            "%s (%s) can't be read; skipping it — "
            "the cluster will be evaluated without its records",
            ref.key, ref.cluster_id,
        )
        progress.skip_file(ref.key)
    except Exception:
        attempt = message.receive_count
        logger.exception(
            "attempt %d/%d to read %s (%s) failed",
            attempt, max_receive_attempts, ref.key, ref.cluster_id,
        )
        if attempt < max_receive_attempts:
            return

        # In production a redrive policy would move it to a dead-letter queue,
        # but this service doesn't create resources, so it drops it and moves on.
        logger.error(
            "giving up on %s (%s); skipping it — the cluster will be evaluated without its records",
            ref.key, ref.cluster_id,
        )
        progress.skip_file(ref.key)
    else:
        _record_file(data, ref, progress)
    _ack(message)
    logger.info(
        "%s: %d/%d files received (%s)",
        ref.cluster_id, progress.files_received, progress.spec.num_log_files, ref.key,
    )


def _record_file(data: bytes, ref: LogFileRef, progress: ClusterProgress) -> None:
    """Parses a fetched file into its cluster. A corrupt file is skipped at once."""
    try:
        parsed = parse_log_file(data)
    except ValueError:
        logger.exception(
            "%s (%s) is not a valid log file; skipping it — "
            "the cluster will be evaluated without its records",
            ref.key, ref.cluster_id,
        )
        progress.skip_file(ref.key)
    else:
        logger.info(
            "%s (%s): %d progress records, %d malformed entries skipped",
            ref.key, ref.cluster_id, len(parsed.records), parsed.malformed,
        )
        progress.add_file(ref.key, parsed.records)
