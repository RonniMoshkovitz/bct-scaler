"""Logging goes to stderr only, so stdout stays reserved for the grading harness's line."""
import logging
import sys


def configure_logging(level: str) -> None:
    logging.basicConfig(
        stream=sys.stderr,
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
