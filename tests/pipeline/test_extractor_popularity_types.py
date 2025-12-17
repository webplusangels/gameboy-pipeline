from unittest.mock import AsyncMock

import pytest

from src.pipeline.extractors import IgdbPopularityTypesExtractor
from src.pipeline.interfaces import AuthProvider, Extractor


@pytest.mark.asyncio
async def test_popularity_types_extractor_conforms_to_interface():
    """
    [GREEN]
    IgdbPopularityTypesExtractor가 Extractor 인터페이스를 준수하는지 테스트합니다.
    """

    assert issubclass(IgdbPopularityTypesExtractor, Extractor)


@pytest.mark.asyncio
async def test_popularity_types_extractor_api_url(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbPopularityTypesExtractor가 올바른 API URL을 사용하는지 테스트합니다.

    Verifies:
        1. API URL이 popularity_types 엔드포인트를 가리키는지
    """
    extractor = IgdbPopularityTypesExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client_id"
    )

    assert extractor.api_url == "https://api.igdb.com/v4/popularity_types"
