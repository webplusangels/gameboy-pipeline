import pytest

from src.pipeline.auth import AuthProvider, StaticAuthProvider
from src.pipeline.extractors import IgdbExtractor


@pytest.mark.asyncio
async def test_static_auth_provider_returns_configured_token():
    """
    [RED]
    StaticAuthProvider가 올바른 토큰을 반환하는지 테스트합니다.
    """
    token_value = "test-token"
    auth_provider = StaticAuthProvider(token=token_value)

    token = await auth_provider.get_valid_token()
    assert token == "test-token"


@pytest.mark.asyncio
async def test_igdb_extractor_uses_auth_provider(mocker):
    """
    [RED]
    IgdbExtractor가 StaticAuthProvider로부터 토큰을 받아 사용하는지 테스트합니다.
    """
    mock_client = mocker.AsyncMock()
    mock_auth_provider = mocker.AsyncMock(spec=AuthProvider)
    mock_auth_provider.get_valid_token.return_value = "test-bearer-token"
    mock_auth_provider.client_id = "test-client-id"  # ← 속성 추가

    mock_client.post.return_value = mocker.Mock(
        status_code=200,
        json=lambda: [{"id": 1, "name": "Mock Game"}],
        raise_for_status=lambda: None,  # raise_for_status()가 에러를 내지 않도록 모킹
    )

    extractor = IgdbExtractor(client=mock_client, auth_provider=mock_auth_provider)

    results = []
    async for item in extractor.extract():
        results.append(item)

    mock_auth_provider.get_valid_token.assert_called_once()

    mock_client.post.assert_called_once()

    call_args = mock_client.post.call_args
    assert "headers" in call_args.kwargs
    assert call_args.kwargs["headers"]["Authorization"] == "Bearer test-bearer-token"
