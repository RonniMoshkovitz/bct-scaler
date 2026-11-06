"""Parses Spark driver log files (gzipped text) into progress records."""

from __future__ import annotations

import gzip
import json
import logging
import re
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise

from autoscaler.domain.models import ProgressRecord

logger = logging.getLogger(__name__)

_HEADER_RE = re.compile(r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ", re.MULTILINE)
_MARKER = "Streaming query made progress:"
_REQUIRED_FIELDS = ("id", "batchId", "timestamp")
_JSON_DECODER = json.JSONDecoder()


@dataclass(frozen=True)
class ParsedLog:
    records: list[ProgressRecord]
    malformed: int  # progress entries that didn't yield a valid record


def parse_log_file(data: bytes) -> ParsedLog:
    """Parses raw bytes of a `.log.gz` file into a single ParsedLog.
    Raises ValueError only if they aren't a complete, valid gzip stream."""
    try:
        text = gzip.decompress(data).decode("utf-8", errors="replace")
    except (OSError, EOFError, zlib.error) as exc:  # BadGzipFile is an OSError; EOF = truncated
        raise ValueError(f"not a valid gzip file: {exc}") from exc
    return parse_log_text(text)


def parse_log_text(text: str) -> ParsedLog:
    """Parses all valid progress records out of one decompressed log file's text.
    Malformed or incomplete entries are skipped (and counted) so a single bad
    record can't take down the run.
    """
    progress_entries = [entry for entry in _split_entries(text) if _MARKER in entry]
    records = [
        record
        for entry in progress_entries
        if (record := _parse_progress_entry(entry)) is not None
    ]
    return ParsedLog(records=records, malformed=len(progress_entries) - len(records))


def _split_entries(text: str) -> list[str]:
    """Splits log file text into separate log text entries."""
    starts = [match.start() for match in _HEADER_RE.finditer(text)]
    if not starts:
        return []

    return [text[i:j] for i, j in pairwise([*starts, len(text)])]


def _parse_progress_entry(entry: str) -> ProgressRecord | None:
    """Parses text of a progress record log entry into a ProgressRecord."""
    _, _, after_marker = entry.partition(_MARKER)
    if (brace_pos := after_marker.find("{")) == -1:
        logger.debug("progress marker with no JSON body, skipping entry")
        return None

    try:
        # Decodes the one JSON value starting at brace_pos and ignores whatever follows it, so
        # compact and pretty-printed records parse identically; truncation raises here too.
        payload, _ = _JSON_DECODER.raw_decode(after_marker, idx=brace_pos)
    except json.JSONDecodeError as err:
        logger.debug("malformed or truncated JSON progress record (%s), skipping entry", err)
        return None

    if any(payload.get(field) is None for field in _REQUIRED_FIELDS):
        logger.debug("progress record missing a required field, skipping entry")
        return None

    return _build_record(payload)


def _build_record(payload: dict) -> ProgressRecord | None:
    """Converts a decoded progress payload into a record, or None if its values don't convert."""
    try:
        timestamp = datetime.fromisoformat(payload["timestamp"])
        batch_id = int(payload["batchId"])
    except (TypeError, ValueError, OverflowError) as err:
        logger.debug("unparseable timestamp/batchId (%s), skipping entry", err)
        return None

    return ProgressRecord(
        id=str(payload["id"]),
        batch_id=batch_id,
        timestamp=timestamp if timestamp.tzinfo is not None else timestamp.replace(tzinfo=UTC),
        timestamp_raw=payload["timestamp"],
        raw=payload,
    )
