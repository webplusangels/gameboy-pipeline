from unittest.mock import AsyncMock, Mock

import pytest

from src.pipeline.extractors import IgdbExtractor
from src.pipeline.interfaces import AuthProvider, Extractor


def test_igdb_extractor_conforms_to_interface():
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
    assert results[0]["id"] == 350392
    assert results[0]["name"] == "Rival Species"
    assert results[3]["id"] == 63844
    assert results[3]["name"] == "Ace wo Nerae!"

    assert mock_client.post.call_count == 2


@pytest.mark.asyncio
async def test_igdb_extractor_returns_empty_list(
    mocker, mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    API가 빈 응답을 반환할 때 IgdbExtractor가 처리하는지 테스트합니다.
    """
    mock_response = Mock(
        status_code=200, json=lambda: [], raise_for_status=lambda: None
    )
    mock_client.post.return_value = mock_response

    extractor = IgdbExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client-id"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == 0
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_igdb_extractor_returns_multiple_items(
    mocker,
    mock_client: AsyncMock,
    mock_auth_provider: AuthProvider,
    mock_game_data: list[dict],
):
    """
    [GREEN]
    API가 여러 게임을 반환할 때 IgdbExtractor가 처리하는지 테스트합니다.
    """
    mock_response = Mock(
        status_code=200,
        json=lambda: mock_game_data,
        raise_for_status=lambda: None,
    )
    mock_response_empty = mocker.Mock(
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
    assert results[3]["id"] == 63844
    assert results[3]["name"] == "Ace wo Nerae!"
    assert mock_client.post.call_count == 2


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
async def test_igdb_extractor_handles_pagination(
    mocker, mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    [GREEN]
    IgdbExtractor가 페이지네이션을 올바르게 처리하는지 테스트합니다.

    Scenario:
        - Page 1: 2개 아이템 (id: 1, 2)
        - Page 2: 1개 아이템 (id: 3)
        - Page 3: 빈 응답 (종료)

    Verifies:
        1. 모든 페이지의 데이터를 순차적으로 수집
        2. API 호출 횟수 (3번)
        3. Offset 증가 (0 → 500 → 1000)
    """
    mock_response_page_1 = Mock(
        status_code=200,
        json=lambda: [{"id": 1, "name": "Game 1"}, {"id": 2, "name": "Game 2"}],
        raise_for_status=lambda: None,
    )
    mock_response_page_2 = Mock(
        status_code=200,
        json=lambda: [{"id": 3, "name": "Game 3"}],
        raise_for_status=lambda: None,
    )
    mock_response_page_3 = Mock(
        status_code=200,
        json=lambda: [],  # 종료 조건
        raise_for_status=lambda: None,
    )

    mock_client.post.side_effect = [
        mock_response_page_1,
        mock_response_page_2,
        mock_response_page_3,
    ]

    extractor = IgdbExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client-id"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    # 1. 결과 데이터 검증
    assert len(results) == 3
    assert results[0]["id"] == 1
    assert results[2]["id"] == 3

    # 2. API 호출 횟수 검증
    assert mock_client.post.call_count == 3

    # 3. (핵심) Offset 증가 검증
    all_calls = mock_client.post.call_args_list

    # extractor._BASE_QUERY = "fields *;"
    # extractor._LIMIT = 500

    # 호출 1: offset 0
    query_page_1 = f"{extractor._BASE_QUERY} limit {extractor._LIMIT}; offset 0;"
    assert all_calls[0].kwargs["content"] == query_page_1

    # 호출 2: offset 500
    query_page_2 = (
        f"{extractor._BASE_QUERY} limit {extractor._LIMIT}; offset {extractor._LIMIT};"
    )
    assert all_calls[1].kwargs["content"] == query_page_2

    # 호출 3: offset 1000
    query_page_3 = f"{extractor._BASE_QUERY} limit {extractor._LIMIT}; offset {extractor._LIMIT * 2};"
    assert all_calls[2].kwargs["content"] == query_page_3


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
