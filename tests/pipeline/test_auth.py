import pytest

from src.pipeline.auth import AuthProvider, StaticAuthProvider
from src.pipeline.extractors import IgdbExtractor


@pytest.mark.asyncio
async def test_static_auth_provider_returns_configured_token():
    """
    [GREEN]
    StaticAuthProvider가 올바른 토큰을 반환하는지 테스트합니다.
    """
    token_value = "test-token"
    auth_provider = StaticAuthProvider(token=token_value)

    token = await auth_provider.get_valid_token()
    assert token == "test-token"


@pytest.mark.asyncio
async def test_igdb_extractor_uses_auth_provider(mocker):
    """
    [GREEN]
    IgdbExtractor가 StaticAuthProvider로부터 토큰을 받아 사용하는지 테스트합니다.
    """
    mock_client = mocker.AsyncMock()
    mock_auth_provider = mocker.AsyncMock(spec=AuthProvider)
    mock_auth_provider.get_valid_token.return_value = "test-bearer-token"
    mock_response = mocker.Mock(
        status_code=200,
        json=lambda: [{"id": 1, "name": "Mock Game"}],
        raise_for_status=lambda: None,
    )
    mock_response_empty = mocker.Mock(
        status_code=200, json=lambda: [], raise_for_status=lambda: None
    )

    mock_client.post.side_effect = [
        mock_response,
        mock_response_empty,
    ]

    extractor = IgdbExtractor(
        client=mock_client, auth_provider=mock_auth_provider, client_id="test-client-id"
    )

    results = []
    async for item in extractor.extract():
        results.append(item)

    mock_auth_provider.get_valid_token.assert_called_once()

    assert mock_client.post.call_count == 2

    call_args = mock_client.post.call_args
    assert "headers" in call_args.kwargs
    assert call_args.kwargs["headers"]["Authorization"] == "Bearer test-bearer-token"
