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
