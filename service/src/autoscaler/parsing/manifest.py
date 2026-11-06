"""Converts a JSON manifest for clusters into a list of cluster specs."""
from __future__ import annotations

import json

from autoscaler.domain.models import ClusterSpec


def parse_manifest(data: bytes) -> list[ClusterSpec]:
    """Parses manifest and raises ValueError on a malformed manifest (the run can't start without it)."""
    payload = json.loads(data.decode("utf-8"))
    clusters = payload.get("clusters") if isinstance(payload, dict) else None

    if not isinstance(clusters, list) or not clusters:
        raise ValueError("manifest must contain a non-empty 'clusters' list")
    try:
        return [
            ClusterSpec(
                cluster_id=str(entry["cluster_id"]),
                initial_workers=int(entry["initial_workers"]),
                num_log_files=int(entry["num_log_files"]),
            )
            for entry in clusters
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"malformed manifest cluster entry: {exc!r}") from exc
