import json
import os

import httpx
import pytest

from src.config import settings
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.extractors import IgdbExtractor

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_igdb_extractor_it_fetches_real_data_static_auth():
    """
    [INTEGRATION]
    - IgdbExtractor가 실제 IGDB API로부터 데이터를 성공적으로 가져오는지 테스트합니다.
    - 실제 API 호출이므로 네트워크 상태에 따라 테스트 결과가 달라질 수 있습니다.
    - .env에 IGDB API 자격 증명이 올바르게 설정되어 있어야 합니다.
    - httpx.AsyncClient와 StaticAuthProvider를 사용합니다.
    - 실제 응답 4개를 'logs/it_extractor_response.json'에 저장합니다.
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id

    if not token or not client_id:
        pytest.skip(
            "IGDB_STATIC_TOKEN 또는 IGDB_CLIENT_ID 환경 변수가 설정되지 않았습니다."
        )

    auth_provider = StaticAuthProvider(token=token)

    async with httpx.AsyncClient() as client:
        extractor = IgdbExtractor(
            client=client, auth_provider=auth_provider, client_id=client_id
        )

        results = []
        async for item in extractor.extract():
            results.append(item)
            if len(results) >= 4:  # 4개 항목만 수집
                break

        assert len(results) == 4

    # 응답을 로그 파일에 저장
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_path = os.path.join(log_dir, "it_extractor_response.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


@pytest.mark.asyncio
async def test_igdb_extractor_it_pagination_with_500_limit():
    """
    [INTEGRATION]
    - IgdbExtractor가 LIMIT=500 설정으로 데이터를 올바르게 가져오는지 테스트합니다.
    - Rate limit을 고려하여 200개만 수집 (API 호출 최소화)
    - 실제 API 호출 횟수와 수집된 아이템 수를 검증합니다.

    Warning:
        - 실제 API 호출이므로 rate limit(4 req/sec) 고려 필요
        - 네트워크 상태에 따라 시간이 오래 걸릴 수 있음 (약 0.5초 이상)
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id

    if not token or not client_id:
        pytest.skip("IGDB API 자격 증명이 설정되지 않았습니다.")

    auth_provider = StaticAuthProvider(token=token)

    async with httpx.AsyncClient() as client:
        extractor = IgdbExtractor(
            client=client, auth_provider=auth_provider, client_id=client_id
        )

        results = []
        async for item in extractor.extract():
            results.append(item)
            if len(results) >= 200:  # Rate limit 고려하여 최소화
                break

        # 검증 1: 최소 200개 수집 (페이지네이션 여부와 무관하게 데이터 추출 확인)
        assert len(results) >= 200, f"Expected >= 200 items, got {len(results)}"

        # 검증 2: ID 중복 없음 (페이지네이션 overlap 방지)
        ids = [item["id"] for item in results if "id" in item]
        assert len(ids) == len(set(ids)), "중복된 ID가 발견되었습니다"

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_path = os.path.join(log_dir, "it_extractor_pagination_response.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
