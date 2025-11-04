import json
import os

import httpx
import pytest
from dotenv import load_dotenv

from src.pipeline.auth import StaticAuthProvider
from src.pipeline.extractors import IgdbExtractor

load_dotenv()

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
    token = os.getenv("IGDB_STATIC_TOKEN")
    client_id = os.getenv("IGDB_CLIENT_ID")

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
    - IgdbExtractor가 LIMIT=500 설정으로 페이지네이션을 올바르게 수행하는지 테스트합니다.
    - 최소 2번의 API 호출(1000개)을 수행하여 offset 증가를 확인합니다.
    - 실제 API 호출 횟수와 수집된 아이템 수를 검증합니다.

    Warning:
        - 실제 API 호출이므로 rate limit(4 req/sec) 고려 필요
        - 네트워크 상태에 따라 시간이 오래 걸릴 수 있음 (약 0.5초 이상)
    """
    token = os.getenv("IGDB_STATIC_TOKEN")
    client_id = os.getenv("IGDB_CLIENT_ID")

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
            if len(results) >= 1000:  # 2페이지 분량 (500 * 2)
                break

        # 검증 1: 최소 1000개 이상 수집 (페이지네이션 발생)
        assert len(results) >= 1000, f"Expected >= 1000 items, got {len(results)}"

        # 검증 2: ID 중복 없음 (페이지네이션 overlap 방지)
        ids = [item["id"] for item in results if "id" in item]
        assert len(ids) == len(set(ids)), "중복된 ID가 발견되었습니다"

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_path = os.path.join(log_dir, "it_extractor_pagination_response.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
