from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from src.pipeline.extractors import IgdbPopScoreExtractor
from src.pipeline.interfaces import AuthProvider, Extractor


@pytest.mark.asyncio
async def test_popscore_extractor_conforms_to_interface():
    """
    [GREEN]
    IgdbPopScoreExtractor가 Extractor 인터페이스를 준수하는지 테스트합니다.
    """

    assert issubclass(IgdbPopScoreExtractor, Extractor)


@pytest.mark.asyncio
async def test_popscore_extractor_extract_method(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbPopScoreExtractor의 extract 메서드가 예상대로 작동하는지 테스트합니다.

    Verifies:
        1. extract 메서드가 비동기 제너레이터를 반환하는지
        2. last_updated_at이 주어져도 이를 무시하고 전체 데이터를 추출하는지
    """
    target = datetime(2025, 1, 1)

    extractor = IgdbPopScoreExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client_id"
    )
    extractor._client.post.return_value.json.return_value = []
    async for _ in extractor.extract(last_updated_at=target):
        pass

    call_args = extractor._client.post.call_args
    assert call_args is not None
    request_content = call_args.kwargs["content"]

    assert "updated_at" not in request_content
    assert "popularity_type =" in request_content


@pytest.mark.asyncio
async def test_popscore_extractor_extract_concurrent_method(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbPopScoreExtractor의 extract_concurrent 메서드가 예상대로 작동하는지 테스트합니다.

    Verifies:
        1. extract_concurrent 메서드가 비동기 제너레이터를 반환하는지
        2. last_updated_at이 주어져도 이를 무시하고 전체 데이터를 추출하는지
    """
    target = datetime(2025, 1, 1)

    extractor = IgdbPopScoreExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client_id"
    )
    extractor._client.post.return_value.json.return_value = []
    async for _ in extractor.extract_concurrent(last_updated_at=target):
        pass

    call_args = extractor._client.post.call_args
    assert call_args is not None
    request_content = call_args.kwargs["content"]

    assert "updated_at" not in request_content
    assert "popularity_type =" in request_content


@pytest.mark.asyncio
async def test_popscore_extractor_query_format(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbPopScoreExtractor의 쿼리 포맷이 예상대로 생성되는지 테스트합니다.

    Verifies:
        1. 쿼리에 올바른 필드가 포함되는지
        2. 쿼리에 불필요한 조건이 포함되지 않는지
    """
    extractor = IgdbPopScoreExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client_id"
    )

    query = extractor.base_query

    # 11개 popularity type 모두 포함 확인
    assert "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 34" in query
    assert "fields game_id, popularity_type, value" in query
