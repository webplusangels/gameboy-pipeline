from unittest.mock import AsyncMock, Mock

import pytest

from src.pipeline.extractors import IgdbGameModeExtractor
from src.pipeline.interfaces import AuthProvider, Extractor

MOCK_GAME_MODE_DATA = [
    {"id": 1, "name": "Single player", "slug": "single-player"},
    {"id": 2, "name": "Multiplayer", "slug": "multiplayer"},
]


@pytest.mark.asyncio
async def test_game_mode_extractor_conforms_to_interface():
    """
    [GREEN]
    IgdbGameModeExtractor가 Extractor 인터페이스를 준수하는지 테스트합니다.
    """
    assert issubclass(IgdbGameModeExtractor, Extractor)


@pytest.mark.asyncio
async def test_game_mode_extractor_fetches_and_pages_data(
    mocker, mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbGameModeExtractor가 'game_modes' 엔드포인트에서
    올바르게 데이터를 페칭하고 페이지네이션하는지 테스트합니다.
    """
    mock_response_page_1 = Mock(
        status_code=200,
        json=lambda: MOCK_GAME_MODE_DATA,
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

    extractor = IgdbGameModeExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client_id"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == 2
    assert results[0]["name"] == "Single player"
    assert results[1]["slug"] == "multiplayer"

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
