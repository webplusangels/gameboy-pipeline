from unittest.mock import AsyncMock, Mock

import pytest

from src.pipeline.extractors import BaseIgdbExtractor, IgdbExtractor
from src.pipeline.interfaces import AuthProvider, Extractor


@pytest.mark.asyncio
async def test_base_igdb_extractor_is_abstract(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    BaseIgdbExtractor가 추상 속성이나 메서드를 구현하지 않으면
    TypeError를 발생시키는지 테스트합니다.
    """

    class IncompleteExtractor(BaseIgdbExtractor):
        # _API_URL, _BASE_QUERY, _LIMIT 모두 구현 안 함
        pass

    with pytest.raises(TypeError):
        IncompleteExtractor(
            client=mock_client,
            auth_provider=mock_auth_provider,
            client_id="test-client-id",
        )


@pytest.mark.asyncio
async def test_igdb_extractor_conforms_to_interface():
    """
    [GREEN]
    IgdbExtractor가 Extractor 인터페이스를 준수하는지 테스트합니다.
    """
    # 클래스가 Extractor를 상속하는지 확인
    assert issubclass(IgdbExtractor, Extractor)


@pytest.mark.asyncio
async def test_igdb_extractor_returns_mock_data(
    mocker,
    mock_client: AsyncMock,
    mock_auth_provider: AuthProvider,
    mock_game_data: list[dict],
):
    """
    [GREEN]
    IgdbExtractor가 모킹된 API로부터 데이터를 반환하는지 테스트합니다.

    - 실제 IGDB 응답 데이터를 사용하여 추출된 게임 데이터의 정확성을 검증합니다.
    - 페이징을 통해 모든 데이터를 수집합니다.
    - Offset이 올바르게 증가하는지 확인합니다.
    """
    mock_response = Mock(
        status_code=200,
        json=lambda: mock_game_data,
        raise_for_status=lambda: None,
    )
    mock_response_empty = Mock(
        status_code=200, json=lambda: [], raise_for_status=lambda: None
    )

    mock_client.post.side_effect = [mock_response, mock_response_empty]

    extractor = IgdbExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client-id"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == 4
    assert results[0]["name"] == "Rival Species"
    assert results[3]["name"] == "Ace wo Nerae!"

    assert mock_client.post.call_count == 2

    all_calls = mock_client.post.call_args_list

    base_query = extractor._BASE_QUERY
    limit = extractor._LIMIT

    query_page_1 = f"{base_query} limit {limit}; offset 0;"
    query_page_2 = f"{base_query} limit {limit}; offset {limit};"

    assert all_calls[0].kwargs["content"] == query_page_1
    assert all_calls[1].kwargs["content"] == query_page_2


@pytest.mark.asyncio
async def test_igdb_extractor_handles_http_error(
    mocker, mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    HTTP 에러(4xx, 5xx)가 발생할 때 IgdbExtractor가 예외를 발생시키는지 테스트합니다.
    """
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = Exception("HTTP 500 Error")
    mock_client.post.return_value = mock_response

    extractor = IgdbExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client-id"
    )

    with pytest.raises(Exception, match="HTTP 500 Error"):
        async for _ in extractor.extract():
            pass

    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_igdb_extractor_handles_pagination_empty_first_page(
    mocker, mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbExtractor가 첫 페이지가 빈 응답일 때 페이지네이션을 올바르게 처리하는지 테스트합니다.

    Scenario:
        - Page 1: 빈 응답 (종료)

    Verifies:
        1. 결과 데이터가 빈 리스트인지 확인
        2. API 호출 횟수 (1번)
    """
    mock_response_page_1 = Mock(
        status_code=200,
        json=lambda: [],  # 종료 조건
        raise_for_status=lambda: None,
    )

    mock_client.post.side_effect = [
        mock_response_page_1,
    ]

    extractor = IgdbExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client-id"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    # 1. 결과 데이터 검증
    assert len(results) == 0

    # 2. API 호출 횟수 검증
    assert mock_client.post.call_count == 1
