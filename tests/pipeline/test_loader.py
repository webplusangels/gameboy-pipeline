import pytest

from src.pipeline.interfaces import Loader
from src.pipeline.loaders import S3Loader


@pytest.mark.asyncio
async def test_s3_loader_conforms_to_interface():
    """
    [GREEN]
    S3Loader 구현체가 Loader 인터페이스를 준수하는지 테스트합니다.
    """
    assert issubclass(S3Loader, Loader)


@pytest.mark.asyncio
async def test_s3_loader_calls_put_object_correctly(mocker):
    """
    [GREEN]
    S3Loader가 S3 클라이언트의 put_object 메서드를 올바르게 호출하는지 테스트합니다.
    """
    mock_s3_client = mocker.AsyncMock()

    test_data = [{"id": 1, "name": "Test Game"}, {"id": 2, "name": "Another Game"}]
    bucket_name = "test-bucket"
    key = "raw/games/test_games.jsonl"

    loader = S3Loader(client=mock_s3_client, bucket_name=bucket_name)
    await loader.load(data=test_data, key=key)

    assert mock_s3_client.put_object.call_count == 1
    _, kwargs = mock_s3_client.put_object.call_args

    assert kwargs["Bucket"] == "test-bucket"
    assert kwargs["Key"] == key
    assert kwargs["Body"] == (
        '{"id": 1, "name": "Test Game"}\n{"id": 2, "name": "Another Game"}'
    )
    assert kwargs["ContentType"] == "application/x-jsonlines"


@pytest.mark.asyncio
async def test_s3_loader_handles_empty_data(mocker):
    """
    [GREEN]
    S3Loader가 빈 데이터 리스트를 처리하는지 테스트합니다.

    처리하지 않고 바로 반환되어야 합니다.
    """
    mock_s3_client = mocker.AsyncMock()

    test_data = []
    bucket_name = "test-bucket"
    key = "raw/games/empty_games.jsonl"

    loader = S3Loader(client=mock_s3_client, bucket_name=bucket_name)
    await loader.load(data=test_data, key=key)

    # 빈 데이터는 호출되지 않아야 함
    mock_s3_client.put_object.assert_not_called()


@pytest.mark.asyncio
async def test_s3_loader_handles_s3_error(mocker):
    """
    [GREEN]
    S3Loader가 S3 클라이언트의 예외를 적절히 처리하는지 테스트합니다.

    예외는 상위로 전파되어야 합니다.
    """
    mock_s3_client = mocker.AsyncMock()
    mock_s3_client.put_object.side_effect = Exception("S3 Access Denied")

    test_data = [{"id": 1, "name": "Test Game"}]
    bucket_name = "test-bucket"
    key = "raw/games/error_games.jsonl"

    loader = S3Loader(client=mock_s3_client, bucket_name=bucket_name)

    with pytest.raises(Exception) as exc_info:
        await loader.load(data=test_data, key=key)

    assert str(exc_info.value) == "S3 Access Denied"
