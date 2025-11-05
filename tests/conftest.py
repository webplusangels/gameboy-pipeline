import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from src.pipeline.interfaces import AuthProvider

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def mock_game_data() -> list[dict]:
    """
    [Fixture]
    실제 IGDB game 응답 데이터 파일을
    유닛 테스트용 Mock 데이터로 로드합니다.
    """
    file_path = ROOT / "tests" / "test_data" / "igdb_games_mock.json"

    if not file_path.exists():
        pytest.skip("통합 테스트 응답 데이터 파일이 존재하지 않습니다.")

    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def mock_auth_provider(mocker) -> AsyncMock:
    """AuthProvider의 기본 Mock (토큰 반환)"""
    mock = mocker.AsyncMock(spec=AuthProvider)
    mock.get_valid_token.return_value = "mock-token"
    return mock


@pytest.fixture
def mock_client(mocker) -> AsyncMock:
    """httpx.AsyncClient의 기본 Mock"""
    mock = mocker.AsyncMock()
    # raise_for_status가 에러를 내지 않도록 기본 설정
    mock_response = mocker.Mock(raise_for_status=lambda: None)
    mock.post.return_value = mock_response
    return mock
