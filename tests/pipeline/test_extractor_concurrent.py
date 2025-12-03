"""extract_concurrent 메서드 테스트."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from src.pipeline.extractors import IgdbExtractor
from src.pipeline.interfaces import AuthProvider


@pytest.mark.asyncio
async def test_extract_concurrent_returns_all_data(
    mock_client: AsyncMock,
    mock_auth_provider: AuthProvider,
):
    """
    extract_concurrent 메서드가 모든 데이터를 올바르게 반환하는지 테스트합니다.

    Verifies:
        - extract_concurrent가 모든 레코드를 반환하는지 확인합니다.
        - 총 5개의 레코드가 반환되어야 합니다.
    """
    page_0 = [{"id": 1}, {"id": 2}, {"id": 3}]
    page_1 = [{"id": 4}, {"id": 5}]
    page_2 = []

    def mock_response(data):
        return Mock(raise_for_status=lambda: None, json=lambda: data)

    # batch_size=16이므로 첫 배치에서 16개 요청 생성
    responses = [
        mock_response(page_0),
        mock_response(page_1),
    ]
    responses += [mock_response(page_2) for _ in range(14)]  # 나머지는 빈 응답
    mock_client.post.side_effect = responses

    # 실제 IgdbExtractor 인스턴스 사용
    extractor = IgdbExtractor(
        client=mock_client,
        auth_provider=mock_auth_provider,
        client_id="test-client-id",
    )

    results = [item async for item in extractor.extract_concurrent(batch_size=16)]

    assert len(results) == 5
    assert results[0]["id"] == 1
    assert results[-1]["id"] == 5


@pytest.mark.asyncio
async def test_extract_concurrent_handles_no_data(
    mock_client: AsyncMock,
    mock_auth_provider: AuthProvider,
):
    """
    extract_concurrent 메서드가 데이터가 없을 때 올바르게 처리하는지 테스트합니다.

    Verifies:
        - 데이터가 없을 때 빈 리스트를 반환하는지 확인합니다.
        - API 호출 횟수 (1번)
    """
    mock_response = Mock(
        status_code=200,
        json=lambda: [],  # 종료 조건
        raise_for_status=lambda: None,
    )

    mock_client.post.return_value = mock_response

    extractor = IgdbExtractor(
        client=mock_client,
        auth_provider=mock_auth_provider,
        client_id="test-client-id",
    )

    results = [item async for item in extractor.extract_concurrent(batch_size=16)]

    assert len(results) == 0
    assert mock_client.post.call_count == 16


@pytest.mark.asyncio
async def test_extract_concurrent_handles_http_error(
    mock_client: AsyncMock,
    mock_auth_provider: AuthProvider,
):
    """
    extract_concurrent 메서드가 HTTP 에러를 올바르게 처리하는지 테스트합니다.

    Verifies:
        - HTTP 500 에러 발생 시 ExceptionGroup으로 래핑된 예외가 발생하는지 확인
        - ExceptionGroup 내부에 HTTPStatusError가 포함되어 있는지 확인
    """
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "HTTP 500 Error", request=Mock(), response=Mock(status_code=500)
    )
    mock_client.post.return_value = mock_response

    extractor = IgdbExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="mock-client-id"
    )

    with pytest.raises(ExceptionGroup) as exc_info:
        async for _ in extractor.extract_concurrent(batch_size=2):
            pass

    exception_group = exc_info.value
    http_errors = [
        exc
        for exc in exception_group.exceptions
        if isinstance(exc, httpx.HTTPStatusError)
    ]
    assert len(http_errors) > 0
    assert "HTTP 500 Error" in str(http_errors[0])


@pytest.mark.asyncio
async def test_extract_concurrent_with_last_updated_at(
    mock_client: AsyncMock,
    mock_auth_provider: AuthProvider,
):
    """
    extract_concurrent 메서드가 last_updated_at 파라미터를 올바르게 처리하는지 테스트합니다.

    Verifies:
        - last_updated_at이 지정된 경우 safety_margin이 적용된 timestamp가 쿼리에 포함되는지 확인합니다.
    """

    def mock_response(data):
        return Mock(raise_for_status=lambda: None, json=lambda: data)

    # 첫 페이지만 데이터, 나머지는 빈 응답 (종료 조건)
    responses = [mock_response([{"id": 1}, {"id": 2}])]
    responses += [mock_response([]) for _ in range(15)]
    mock_client.post.side_effect = responses

    extractor = IgdbExtractor(
        client=mock_client,
        auth_provider=mock_auth_provider,
        client_id="test-client-id",
    )

    last_updated_at = datetime(2021, 6, 1, 0, 0, 0, tzinfo=UTC)

    safety_margin_minutes = extractor.safety_margin_minutes
    expected_safe_timestamp = last_updated_at - timedelta(minutes=safety_margin_minutes)
    expected_unix_timestamp = int(expected_safe_timestamp.timestamp())

    results = [
        item
        async for item in extractor.extract_concurrent(
            batch_size=16, last_updated_at=last_updated_at
        )
    ]

    assert len(results) == 2

    call_args = mock_client.post.call_args_list[0]
    called_query = call_args.kwargs.get("content", "")

    assert f"where updated_at > {expected_unix_timestamp}" in called_query
    assert "sort id asc" in called_query
