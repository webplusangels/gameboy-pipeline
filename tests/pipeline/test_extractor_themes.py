from unittest.mock import AsyncMock, Mock

import pytest

from src.pipeline.extractors import IgdbThemeExtractor
from src.pipeline.interfaces import AuthProvider, Extractor

MOCK_THEME_DATA = [
    {"id": 1, "name": "Action", "slug": "action"},
    {"id": 2, "name": "Adventure", "slug": "adventure"},
]


@pytest.mark.asyncio
async def test_theme_extractor_conforms_to_interface():
    """
    [GREEN]
    IgdbThemeExtractor가 Extractor 인터페이스를 준수하는지 테스트합니다.
    """
    assert issubclass(IgdbThemeExtractor, Extractor)


@pytest.mark.asyncio
async def test_theme_extractor_fetches_and_pages_data(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbThemeExtractor가 'themes' 엔드포인트에서
    올바르게 데이터를 페칭하고 페이지네이션하는지 테스트합니다.
    """
    mock_response_page_1 = Mock(
        status_code=200,
        json=lambda: MOCK_THEME_DATA,
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

    extractor = IgdbThemeExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client_id"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == 2
    assert results[0]["name"] == "Action"
    assert results[1]["slug"] == "adventure"

    assert mock_client.post.call_count == 2

    all_calls = mock_client.post.call_args_list

    api_url = extractor.api_url
    limit = extractor.limit
    base_query = extractor.base_query

    query_page_1 = f"{base_query} limit {limit}; offset 0;"
    query_page_2 = f"{base_query} limit {limit}; offset {limit};"

    assert all_calls[0].kwargs["url"] == api_url
    assert all_calls[0].kwargs["content"] == query_page_1

    assert all_calls[1].kwargs["url"] == api_url
    assert all_calls[1].kwargs["content"] == query_page_2
