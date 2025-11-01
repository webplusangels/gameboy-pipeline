import pytest
from src.pipeline.extractors import IgdbExtractor

from src.pipeline.interfaces import Extractor


def test_igdb_extractor_conforms_to_interface():
    """
    [RED]
    IgdbExtractor가 Extractor 인터페이스를 준수하는지 테스트합니다.
    """
    # 클래스가 Extractor를 상속하는지 확인
    assert issubclass(IgdbExtractor, Extractor)


@pytest.mark.asyncio
async def test_igdb_extractor_returns_mock_data(mocker):
    """
    [RED]
    IgdbExtractor가 모킹된 API로부터 데이터를 반환하는지 테스트합니다.
    """
    mock_client = mocker.AsyncMock()
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": 1, "name": "Mock Game"}]
    mock_client.post.return_value = mock_response

    extractor = IgdbExtractor(client=mock_client, api_key="mock-key")

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == 1
    assert results[0]["name"] == "Mock Game"
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_igdb_extractor_returns_empty_list(mocker):
    """
    [RED]
    API가 빈 응답을 반환할 때 IgdbExtractor가 처리하는지 테스트합니다.
    """
    mock_client = mocker.AsyncMock()
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = []  # 빈 응답
    mock_client.post.return_value = mock_response

    extractor = IgdbExtractor(client=mock_client, api_key="mock-key")

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == 0
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_igdb_extractor_returns_multiple_items(mocker):
    """
    [RED]
    API가 여러 게임을 반환할 때 IgdbExtractor가 처리하는지 테스트합니다.
    """
    mock_client = mocker.AsyncMock()
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"id": 1, "name": "Game 1", "summary": "First game"},
        {"id": 2, "name": "Game 2", "summary": "Second game"},
        {"id": 3, "name": "Game 3", "summary": "Third game"},
    ]
    mock_client.post.return_value = mock_response

    extractor = IgdbExtractor(client=mock_client, api_key="mock-key")

    results = []
    async for item in extractor.extract():
        results.append(item)

    assert len(results) == 3
    assert results[0]["name"] == "Game 1"
    assert results[1]["name"] == "Game 2"
    assert results[2]["name"] == "Game 3"
    mock_client.post.assert_called_once()


@pytest.mark.asyncio
async def test_igdb_extractor_handles_http_error(mocker):
    """
    [RED]
    HTTP 에러(4xx, 5xx)가 발생할 때 IgdbExtractor가 예외를 발생시키는지 테스트합니다.
    """
    mock_client = mocker.AsyncMock()
    mock_response = mocker.Mock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = Exception("HTTP 500 Error")
    mock_client.post.return_value = mock_response

    extractor = IgdbExtractor(client=mock_client, api_key="mock-key")

    with pytest.raises(Exception, match="HTTP 500 Error"):
        async for _ in extractor.extract():
            pass

    mock_client.post.assert_called_once()
