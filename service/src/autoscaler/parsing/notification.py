"""Queue notification body (`{"bucket", "key", "cluster_id"}` JSON) -> log file reference."""
from __future__ import annotations

import json

from autoscaler.domain.models import LogFileRef


def parse_notification(body: str) -> LogFileRef:
    """Raises ValueError if the body isn't a JSON object with the three expected strings."""
    try:
        payload = json.loads(body)
        return LogFileRef(
            cluster_id=str(payload["cluster_id"]),
            bucket=str(payload["bucket"]),
            key=str(payload["key"]),
        )
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValueError(f"malformed notification body: {exc!r}") from exc
