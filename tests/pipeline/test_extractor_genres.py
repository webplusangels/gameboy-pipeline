from unittest.mock import AsyncMock, Mock

import pytest

from src.pipeline.extractors import IgdbGenreExtractor
from src.pipeline.interfaces import AuthProvider, Extractor

MOCK_GENRE_DATA = [
    {"id": 4, "name": "Fighting", "slug": "fighting"},
    {"id": 5, "name": "Shooter", "slug": "shooter"},
]


@pytest.mark.asyncio
async def test_genre_extractor_conforms_to_interface():
    """
    [GREEN]
    IgdbGenreExtractor가 Extractor 인터페이스를 준수하는지 테스트합니다.
    """
    assert issubclass(IgdbGenreExtractor, Extractor)


@pytest.mark.asyncio
async def test_genre_extractor_fetches_and_pages_data(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbGenreExtractor가 'genres' 엔드포인트에서
    올바르게 데이터를 페칭하고 페이지네이션하는지 테스트합니다.
    """
    mock_response_page_1 = Mock(
        status_code=200,
        json=lambda: MOCK_GENRE_DATA,
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

    extractor = IgdbGenreExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client_id"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == 2
    assert results[0]["name"] == "Fighting"
    assert results[1]["slug"] == "shooter"

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
