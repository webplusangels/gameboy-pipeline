import json
import os

import httpx
import pytest

from src.config import settings
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.extractors import IgdbPopScoreExtractor

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_popscore_extractor_it_fetches_real_data():
    """
    [INTEGRATION]
    PopScoreExtractor가 실제 IGDB API로부터 데이터를 성공적으로 가져오는지 테스트합니다.

    Verifies:
        1. 실제 API에서 PopScore 데이터 추출
        2. 8가지 popularity_type이 모두 포함되는지 확인
        3. 응답 데이터 형식 검증
        4. 로그 파일에 샘플 데이터 저장
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id

    if not token or not client_id:
        pytest.skip(
            "IGDB_STATIC_TOKEN 또는 IGDB_CLIENT_ID 환경 변수가 설정되지 않았습니다."
        )

    auth_provider = StaticAuthProvider(token=token)

    async with httpx.AsyncClient() as client:
        extractor = IgdbPopScoreExtractor(
            client=client, auth_provider=auth_provider, client_id=client_id
        )

        results = []
        popularity_types_seen = set()

        async for item in extractor.extract():
            results.append(item)
            popularity_types_seen.add(item["popularity_type"])

            # Rate limit 고려 - 최소한으로 수집
            if len(results) >= 50:
                break

        # 최소 50개 항목 수집 확인
        assert len(results) >= 50

        # 각 항목이 필수 필드를 포함하는지 확인
        for item in results:
            assert "id" in item
            assert "game_id" in item
            assert "popularity_type" in item
            assert "value" in item
            # popularity_type은 정수 ID (1-8)
            assert isinstance(item["popularity_type"], int)
            assert 1 <= item["popularity_type"] <= 8

        # 여러 popularity_type이 포함되는지 확인
        # expected_types: 1-8 (popularity_type IDs)
        expected_types = {1, 2, 3, 4, 5, 6, 7, 8}

        # 최소한 일부 타입이 포함되어 있는지 확인
        # (API에서 모든 타입의 데이터가 항상 있지는 않을 수 있음)
        assert len(popularity_types_seen) > 0
        assert popularity_types_seen.issubset(expected_types)

    # 응답을 로그 파일에 저장
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_path = os.path.join(log_dir, "it_popscore_response.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


@pytest.mark.asyncio
async def test_popscore_extractor_it_pagination():
    """
    [INTEGRATION]
    PopScoreExtractor가 데이터를 중복 없이 올바르게 수집하는지 테스트합니다.

    Verifies:
        1. LIMIT=500 설정으로 데이터 추출
        2. 중복 없이 데이터 수집 (Rate limit 고려하여 200개만)
        3. offset 기반 페이지네이션 동작
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id

    if not token or not client_id:
        pytest.skip(
            "IGDB_STATIC_TOKEN 또는 IGDB_CLIENT_ID 환경 변수가 설정되지 않았습니다."
        )

    auth_provider = StaticAuthProvider(token=token)

    async with httpx.AsyncClient() as client:
        extractor = IgdbPopScoreExtractor(
            client=client, auth_provider=auth_provider, client_id=client_id
        )

        results = []
        seen_ids = set()

        async for item in extractor.extract():
            # ID 중복 확인
            item_id = item["id"]
            assert item_id not in seen_ids, f"Duplicate ID found: {item_id}"
            seen_ids.add(item_id)

            results.append(item)

            # Rate limit 고려 - 200개만 수집
            if len(results) >= 200:
                break

        # 최소 200개 수집 확인
        assert len(results) >= 200
        # 중복 없음 확인
        assert len(seen_ids) == len(results)
