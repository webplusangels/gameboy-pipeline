import json
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from dotenv import load_dotenv

from src.pipeline.interfaces import AuthProvider, Extractor

# .env 파일을 먼저 로드하여 실제 환경 변수 설정
# 통합 테스트에서 실제 API 자격 증명을 사용할 수 있도록 함
load_dotenv(override=False)

# .env에 값이 없는 경우에만 테스트용 기본값 설정
os.environ.setdefault("IGDB_CLIENT_ID", "test-client-id")
os.environ.setdefault("IGDB_CLIENT_SECRET", "test-client-secret")
os.environ.setdefault("IGDB_STATIC_TOKEN", "test-static-token")

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


@pytest.fixture
def mock_s3_client(mocker) -> AsyncMock:
    """boto3 S3 클라이언트의 기본 Mock"""

    class NoSuchKeyError(Exception):
        pass

    mock = mocker.AsyncMock()
    mock.exceptions = mocker.MagicMock()
    mock.exceptions.NoSuchKey = NoSuchKeyError

    mock.get_paginator = mocker.MagicMock()
    return mock


@pytest.fixture
def mock_cloudfront_client(mocker) -> AsyncMock:
    """boto3 CloudFront 클라이언트의 기본 Mock"""
    mock = mocker.AsyncMock()
    return mock


@pytest.fixture
def mock_loader(mocker) -> AsyncMock:
    """Loader 인터페이스의 기본 Mock"""
    mock = mocker.AsyncMock()
    return mock


@pytest.fixture
def mock_extractor(mocker) -> AsyncMock:
    """Extractor 인터페이스의 기본 Mock"""
    mock = mocker.AsyncMock(spec=Extractor)
    mock.extract = mocker.MagicMock()
    return mock


@pytest.fixture
def mock_dependencies(mocker) -> dict[str, AsyncMock]:
    """Orchestrator에 주입할 기본 Mock 종속성들"""
    return {
        "s3_client": mocker.AsyncMock(),
        "cloudfront_client": mocker.AsyncMock(),
        "loader": mocker.AsyncMock(),
        "state_manager": mocker.AsyncMock(),
        "bucket_name": "test-bucket",
    }


@pytest.fixture
def mock_extractors(mocker) -> dict[str, AsyncMock]:
    """테스트용 엔티티 extractors"""
    return {
        "games": mocker.AsyncMock(spec=Extractor),
        "popscore": mocker.AsyncMock(spec=Extractor),
    }
