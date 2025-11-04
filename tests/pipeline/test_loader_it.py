import os

import aioboto3
import pytest
from dotenv import load_dotenv

from src.pipeline.loaders import S3Loader

load_dotenv()

pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
async def s3_client():
    """실제 aioboto3 S3 클라이언트 세션을 생성합니다."""
    region = os.getenv("AWS_DEFAULT_REGION")

    session = aioboto3.Session(region_name=region)
    async with session.client("s3", region_name=region) as client:
        yield client


@pytest.mark.asyncio
async def test_s3_loader_it_uploads_data(s3_client):
    """
    [INTEGRATION]
    - S3Loader가 실제 S3 버킷에 데이터를 업로드하는지 테스트합니다.
    - .env에 AWS 자격 증명이 올바르게 설정되어 있어야 합니다.
    - 실제 S3 버킷에 업로드된 객체를 확인합니다.
    - 테스트 완료 후 생성된 파일을 삭제합니다.
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if (
        not bucket_name
        or not os.getenv("AWS_ACCESS_KEY_ID")
        or not os.getenv("AWS_SECRET_ACCESS_KEY")
    ):
        pytest.skip(
            "S3_BUCKET_NAME, AWS_ACCESS_KEY_ID 또는 AWS_SECRET_ACCESS_KEY 환경 변수가 설정되지 않았습니다."
        )

    test_key = "raw/games/integration_test_file.jsonl"
    test_data = [
        {"id": 1, "name": "Integration Test Game 1"},
        {"id": 2, "name": "Integration Test Game 2"},
    ]
    expected_body = (
        '{"id": 1, "name": "Integration Test Game 1"}\n'
        '{"id": 2, "name": "Integration Test Game 2"}'
    )

    loader = S3Loader(client=s3_client, bucket_name=bucket_name)

    try:
        await loader.load(data=test_data, key=test_key)

        response = await s3_client.get_object(Bucket=bucket_name, Key=test_key)

        body = await response["Body"].read()
        body_str = body.decode("utf-8")

        assert body_str == expected_body

    finally:
        # 테스트 후 업로드된 파일 삭제
        await s3_client.delete_object(Bucket=bucket_name, Key=test_key)
