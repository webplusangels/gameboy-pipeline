from unittest.mock import AsyncMock, Mock

import pytest

from src.pipeline.extractors import Extractor, IgdbPlatformExtractor
from src.pipeline.interfaces import AuthProvider

MOCK_PLATFORM_DATA = [
    {"id": 6, "name": "PC (Microsoft Windows)", "slug": "win"},
    {"id": 48, "name": "PlayStation 5", "slug": "ps5"},
]


@pytest.mark.asyncio
async def test_platform_extractor_conforms_to_interface():
    """
    [GREEN]
    IgdbPlatformExtractor가 Extractor 인터페이스를 준수하는지 테스트합니다.
    """
    assert issubclass(IgdbPlatformExtractor, Extractor)


@pytest.mark.asyncio
async def test_platform_extractor_fetches_and_pages_data(
    mocker, mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbPlatformExtractor가 'platforms' 엔드포인트에서
    올바르게 데이터를 페칭하고 페이지네이션하는지 테스트합니다.
    """
    mock_response_page_1 = Mock(
        status_code=200,
        json=lambda: MOCK_PLATFORM_DATA,
        raise_for_status=lambda: None,
    )
    mock_response_page_2 = Mock(
        status_code=200,
        json=lambda: [],
        raise_for_status=lambda: None,
    )
    mock_client.post.side_effect = [
        mock_response_page_1,
        mock_response_page_2,
    ]

    extractor = IgdbPlatformExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == len(MOCK_PLATFORM_DATA)
    assert results[0]["name"] == "PC (Microsoft Windows)"
    assert results[1]["slug"] == "ps5"

    assert mock_client.post.call_count == 2

    all_calls = mock_client.post.call_args_list

    api_url = extractor._API_URL
    base_query = extractor._BASE_QUERY
    limit = extractor._LIMIT

    query_page_1 = f"{base_query} limit {limit}; offset 0;"
    query_page_2 = f"{base_query} limit {limit}; offset {limit};"

    assert all_calls[0].kwargs["url"] == api_url
    assert all_calls[0].kwargs["content"] == query_page_1

    assert all_calls[1].kwargs["url"] == api_url
    assert all_calls[1].kwargs["content"] == query_page_2
