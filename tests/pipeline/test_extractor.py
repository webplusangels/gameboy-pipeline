from unittest.mock import AsyncMock, Mock

import pytest
from datetime import UTC, datetime, timedelta

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
        # api_url, base_query, limit 모두 구현 안 함
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

    base_query = extractor.base_query
    limit = extractor.limit

    query_page_1 = f"{base_query} limit {limit}; offset 0;"
    query_page_2 = f"{base_query} limit {limit}; offset {limit};"

    assert all_calls[0].kwargs["content"] == query_page_1
    assert all_calls[1].kwargs["content"] == query_page_2


@pytest.mark.asyncio
async def test_igdb_extractor_handles_http_error(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
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
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
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

@pytest.mark.asyncio
async def test_igdb_extractor_query_configuration(
    mock_client: AsyncMock, mock_auth_provider: AuthProvider
):
    """
    IgdbExtractor의 쿼리 구성 속성들이 올바른지 테스트합니다.

    Verifies:
        1. base_query 속성 값
        2. incremental_query 속성 값
        3. limit 속성 값
        4. safety_margin_minutes 속성 값
    """
    extractor = IgdbExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client-id"
    )

    # 1. base_query 검증
    assert extractor.base_query == "fields *; sort id asc;"

    # 2. incremental_query 검증
    assert extractor.incremental_query == "fields *;"

    # 3. limit 검증
    assert extractor.limit == 500

    # 4. safety_margin_minutes 검증
    assert extractor.safety_margin_minutes == 5


@pytest.mark.asyncio
async def test_incremental_extract_applies_safety_margin_to_query(
    mock_client: AsyncMock,
    mock_auth_provider: AuthProvider,
) -> None:
    """증분 추출 시 안전 마진이 적용된 쿼리가 생성된다."""
    # Arrange: IgdbExtractor의 기본 safety_margin_minutes는 5분
    extractor = IgdbExtractor(
        client=mock_client,
        auth_provider=mock_auth_provider,
        client_id="test-client-id",
    )

    last_updated_at = datetime(2025, 11, 28, 12, 0, 0, tzinfo=UTC)
    expected_safe_timestamp = last_updated_at - timedelta(minutes=5)
    expected_unix_timestamp = int(expected_safe_timestamp.timestamp())

    # Mock 응답: 빈 결과 (쿼리 검증이 목적)
    mock_response = Mock(
        status_code=200,
        json=lambda: [],
        raise_for_status=lambda: None,
    )
    mock_client.post.return_value = mock_response

    # Act
    _ = [item async for item in extractor.extract(last_updated_at=last_updated_at)]

    # Assert: HTTP 요청의 content 파라미터 검증
    mock_client.post.assert_called()
    call_args = mock_client.post.call_args
    query_data = call_args.kwargs.get("content", "")

    # 쿼리에 올바른 timestamp가 포함되어 있는지 확인
    assert f"where updated_at > {expected_unix_timestamp}" in query_data
    assert "sort id asc" in query_data


@pytest.mark.asyncio
async def test_incremental_extract_calculates_correct_timestamp(
    mock_client: AsyncMock,
    mock_auth_provider: AuthProvider,
) -> None:
    """다양한 시점에서 안전 마진이 적용된 Unix timestamp가 정확히 계산된다."""
    # Arrange: IgdbExtractor의 기본 safety_margin_minutes는 5분
    extractor = IgdbExtractor(
        client=mock_client,
        auth_provider=mock_auth_provider,
        client_id="test-client-id",
    )
    safety_margin = extractor.safety_margin_minutes  # 5분

    # 특정 시점 설정
    last_updated_at = datetime(2025, 1, 15, 10, 30, 0, tzinfo=UTC)

    # 예상 계산
    expected_safe_time = last_updated_at - timedelta(minutes=safety_margin)
    expected_timestamp = int(expected_safe_time.timestamp())

    mock_response = Mock(
        status_code=200,
        json=lambda: [],
        raise_for_status=lambda: None,
    )
    mock_client.post.return_value = mock_response

    # Act
    _ = [item async for item in extractor.extract(last_updated_at=last_updated_at)]

    # Assert
    call_args = mock_client.post.call_args
    query_data = call_args.kwargs.get("content", "")

    # timestamp 값이 정확히 일치하는지 검증
    assert str(expected_timestamp) in query_data