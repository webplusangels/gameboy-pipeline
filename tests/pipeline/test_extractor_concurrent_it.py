import json
import os

import httpx
import pytest

from src.config import settings
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.extractors import IgdbGenreExtractor
from src.pipeline.rate_limiter import IgdbRateLimiter

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_igdb_extractor_it_concurrent_fetches_real_data_static_auth():
    """
    [INTEGRATION]
    - IgdbGenreExtractor의 extract_concurrent 메서드가 실제 IGDB API로부터
      데이터를 성공적으로 가져오는지 테스트합니다.
    - genres 엔드포인트 사용 (데이터 수가 적어 테스트에 적합)
    - 실제 API 호출이므로 네트워크 상태에 따라 테스트 결과가 달라질 수 있습니다.
    - .env에 IGDB API 자격 증명이 올바르게 설정되어 있어야 합니다.
    - 실제 응답을 'logs/it_extractor_concurrent_response.json'에 저장합니다.
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id

    if not token or not client_id:
        pytest.skip(
            "IGDB_STATIC_TOKEN 또는 IGDB_CLIENT_ID 환경 변수가 설정되지 않았습니다."
        )

    auth_provider = StaticAuthProvider(token=token)
    rate_limiter = IgdbRateLimiter(max_concurrency=4, requests_per_second=4)

    async with httpx.AsyncClient(timeout=30.0) as client:
        extractor = IgdbGenreExtractor(
            client=client,
            auth_provider=auth_provider,
            client_id=client_id,
            rate_limiter=rate_limiter,
        )

        # genres는 전체 ~24개로 적음 (Rate limit 우려 없음)
        results = []
        async for item in extractor.extract_concurrent(batch_size=4):
            results.append(item)

    # 검증
    assert len(results) > 0
    assert all("id" in item for item in results)

    # 중복 검증
    ids = [item["id"] for item in results]
    assert len(ids) == len(set(ids))

    # 응답을 로그 파일에 저장
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_path = os.path.join(log_dir, "it_extractor_concurrent_response.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
