"""Decision making for a cluster whose files have all arrived."""
from __future__ import annotations

import logging
from collections.abc import Sequence

from autoscaler.application.ports import WriteDecisions
from autoscaler.domain.cluster_tracking import ClusterProgress
from autoscaler.domain.models import Decision, Rule
from autoscaler.domain.rules_engine import evaluate_cluster

logger = logging.getLogger(__name__)


def finalize_cluster(
    progress: ClusterProgress, rules: Sequence[Rule], write_decisions: WriteDecisions
) -> list[Decision]:
    """Evaluates the cluster's full record set, writes the resulting decisions and
    marks the cluster finalized. Returns the decisions written (possibly none)."""
    cluster_id = progress.spec.cluster_id
    records = progress.final_records()
    decisions = evaluate_cluster(cluster_id, records, rules, progress.spec.initial_workers)
    if decisions:
        write_decisions(decisions)
    progress.mark_finalized()

    logger.info(
        "%s finalized: %d unique records, %d decisions",
        cluster_id, len(records), len(decisions),
    )
    if progress.skipped_files:
        logger.warning(
            "%s was evaluated without %d skipped file(s): %s",
            cluster_id, len(progress.skipped_files), progress.skipped_files,
        )
    return decisions
