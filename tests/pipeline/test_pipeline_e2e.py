import json
import uuid

import aioboto3
import httpx
import pytest

from src.config import settings
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.extractors import IgdbExtractor
from src.pipeline.loaders import S3Loader

pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
async def s3_client():
    """실제 aioboto3 S3 클라이언트 세션을 생성합니다."""
    region = settings.aws_default_region

    session = aioboto3.Session(region_name=region)
    async with session.client("s3", region_name=region) as client:
        yield client


@pytest.mark.asyncio
async def test_e2e_pipeline_extractor_to_loader(s3_client):
    """
    [E2E]
    Extractor와 Loader를 실제로 연결하여 E -> (Batch) -> L 파이프라인이 정상적으로 동작하는지 테스트합니다.
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id
    bucket_name = settings.s3_bucket_name

    if not token or not client_id or not bucket_name:
        pytest.skip(
            "IGDB_STATIC_TOKEN, IGDB_CLIENT_ID 또는 S3_BUCKET_NAME 환경 변수가 설정되지 않았습니다."
        )

    test_key = f"raw/games/e2e_test_{uuid.uuid4()}.jsonl"
    test_item_count = 4

    async with httpx.AsyncClient() as client:
        auth_provider = StaticAuthProvider(token=token)
        extractor = IgdbExtractor(
            client=client, auth_provider=auth_provider, client_id=client_id
        )
        loader = S3Loader(client=s3_client, bucket_name=bucket_name)

        # === 배치 로직 ===
        batch = []
        try:
            async for item in extractor.extract():
                batch.append(item)
                if len(batch) >= test_item_count:
                    break

            # 남은 데이터 로드
            if batch:
                await loader.load(data=batch, key=test_key)

            response = await s3_client.get_object(Bucket=bucket_name, Key=test_key)
            body_bytes = await response["Body"].read()
            body_str = body_bytes.decode("utf-8")

            lines = body_str.strip().split("\n")
            assert len(lines) == test_item_count

            for i, line in enumerate(lines):
                assert json.loads(line) == batch[i]

        finally:
            # 테스트 후 업로드된 파일 삭제
            try:
                await s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            except Exception as e:
                print(f"Error deleting S3 object {test_key}: {e}")
