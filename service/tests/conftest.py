from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_log_text() -> str:
    return (FIXTURES_DIR / "sample_log_entries.log").read_text()
