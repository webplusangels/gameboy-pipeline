import json
import os

import httpx
import pytest
from loguru import logger

from src.config import settings
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.extractors import IgdbPlatformExtractor


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_igdb_platform_extractor_it_fetches_real_data(mocker):
    """
    [INTEGRATION]
    - IgdbPlatformExtractor가 실제 IGDB API로부터 플랫폼 데이터를 성공적으로 가져오는지 테스트합니다.
    - .env에 IGDB API 자격 증명이 올바르게 설정되어 있어야 합니다.
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id

    if not token or not client_id:
        pytest.skip(
            "IGDB_STATIC_TOKEN 또는 IGDB_CLIENT_ID 환경 변수가 설정되지 않았습니다."
        )

    auth_provider = StaticAuthProvider(token=token)

    async with httpx.AsyncClient() as client:
        extractor = IgdbPlatformExtractor(
            client=client, auth_provider=auth_provider, client_id=client_id
        )

        results = []
        async for item in extractor.extract():
            results.append(item)
            if len(results) >= 4:  # 4개 항목만 수집
                break

        assert len(results) == 4
        assert "id" in results[0]
        assert "name" in results[0]
        assert "slug" in results[0]

        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        # (새로운 파일 이름으로 저장)
        file_path = os.path.join(log_dir, "it_platforms_response.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        logger.debug(f"Platform 통합 테스트 응답을 {file_path} 에 저장했습니다.")
