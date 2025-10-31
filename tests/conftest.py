"""Pytest configuration and fixtures."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def sample_game_data() -> dict[str, str | int]:
    """Sample game data for testing."""
    return {
        "id": 1234,
        "name": "Pokemon Red",
        "platform": "Game Boy",
        "release_year": 1996,
    }
