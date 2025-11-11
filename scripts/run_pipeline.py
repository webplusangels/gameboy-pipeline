import argparse
import asyncio
import os
import sys
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any
from datetime import datetime, timezone
import aioboto3
import httpx
from dotenv import load_dotenv
from loguru import logger

from src.pipeline.auth import StaticAuthProvider
from src.pipeline.extractors import (
    IgdbExtractor,
    IgdbGameModeExtractor,
    IgdbGenreExtractor,
    IgdbPlatformExtractor,
    IgdbPlayerPerspectiveExtractor,
    IgdbThemeExtractor,
)
from src.pipeline.interfaces import Extractor, Loader, StateManager
from src.pipeline.loaders import S3Loader
from src.pipeline.state import S3StateManager

load_dotenv()
BATCH_SIZE = 50000

# 실행 순서 정의 (차원 데이터 먼저, 팩트 데이터 나중에)
EXECUTION_ORDER = [
    "platforms",
    "genres",
    "game_modes",
    "themes",
    "player_perspectives",
    "games",  # 마지막
]

ALL_ENTITIES = {
    "games": IgdbExtractor,
    "platforms": IgdbPlatformExtractor,
    "genres": IgdbGenreExtractor,
    "game_modes": IgdbGameModeExtractor,
    "player_perspectives": IgdbPlayerPerspectiveExtractor,
    "themes": IgdbThemeExtractor,
}

@asynccontextmanager
async def create_clients() -> AsyncGenerator[tuple[httpx.AsyncClient, Any], None]:
    """
    EL 파이프라인에 필요한 비동기 클라이언트를 생성하고 세션을 관리합니다.
    Yields:
        tuple[httpx.AsyncClient, Any]: HTTP 클라이언트와 기타 필요한 클라이언트 객체
    """
    logger.info("HTTPX AsyncClient 세션 생성...")
    region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    session = aioboto3.Session(region_name=region)

    async with (
        httpx.AsyncClient() as http_client,
        session.client("s3", region_name=region) as s3_client,
    ):
        try:
            yield http_client, s3_client
        finally:
            logger.info("클라이언트 세션 종료...")


async def run_entity_pipeline(
    extractor: Extractor,
    loader: Loader,
    state_manager: StateManager,
    entity_name: str,
    full_refresh: bool = False,
) -> None:
    """
    하나의 엔티티에 대한 EL 파이프라인을 실행합니다.

    Args:
        extractor (Extractor): 데이터 추출기 인스턴스
        loader (Loader): 데이터 적재기 인스턴스
        entity_name (str): 처리할 엔티티 이름 (예: "games", "platforms")
        full_refresh (bool): 전체 로드 여부
    """
    logger.info(f"{entity_name} 엔티티에 대한 EL 파이프라인 시작...")
    start_time = time.perf_counter()
    
    # 추출 시작 시간 기록 (State 저장 기준점)
    extraction_start = datetime.now(timezone.utc)

    last_run_time: datetime | None = None
    if not full_refresh:
        last_run_time = await state_manager.get_last_run_time(entity_name)
        if last_run_time:
            logger.info(
                f"'{entity_name}' 증분 업데이트 실행 (마지막 실행: {last_run_time.isoformat()})"
            )
        else:
            logger.info(f"'{entity_name}' 전체 로드 실행")

    batch = []
    total_count = 0
    batch_count = 0
    
    # 날짜 파티션을 파이프라인 시작 시점에 한 번만 설정
    dt_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        async for item in extractor.extract(last_updated_at=last_run_time):
            batch.append(item)
            total_count += 1

            if len(batch) >= BATCH_SIZE:
                key = f"raw/{entity_name}/dt={dt_partition}/batch-{batch_count}-{uuid.uuid4()}.jsonl"
                await loader.load(batch, key)
                logger.info(
                    f"S3에 {entity_name} 배치 {batch_count} 적재 완료: {len(batch)}개 항목"
                )
                batch.clear()
                batch_count += 1

        if batch:
            key = f"raw/{entity_name}/dt={dt_partition}/batch-{batch_count}-{uuid.uuid4()}.jsonl"
            await loader.load(batch, key)
            logger.info(
                f"S3에 {entity_name} 마지막 배치 {batch_count} 적재 완료: {len(batch)}개 항목"
            )

        # 성공 시에만 State 저장 (추출 시작 시간 기준)
        await state_manager.save_last_run_time(entity_name, extraction_start)

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        logger.success(
            f"{entity_name} 엔티티 EL 파이프라인 완료 - "
            f"총 {total_count}개 항목 처리 "
            f"({'증분' if last_run_time else '전체'} 모드), "
            f"소요 시간: {elapsed_time:.2f}초 "
            f"({'평균 ' + str(int(total_count / elapsed_time)) + '개/초)' if elapsed_time > 0 and total_count > 0 else ''}"
        )

    except Exception as e:
        logger.error(f"{entity_name} 엔티티 EL 파이프라인 실패: {e}")
        logger.warning(
            f"'{entity_name}' State 저장 안 함 - "
            f"다음 실행 시 {'전체 로드' if not last_run_time else f'{last_run_time.isoformat()} 이후부터 재시도'}"
        )
        raise


async def main(full_refresh: bool = False) -> None:
    """
    EL 파이프라인의 실행 진입점입니다.
    """
    logger.remove()
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.add(sys.stderr, level=log_level)

    logger.add(
        "logs/pipeline_{time:YYYY-MM-DD-HH-mm-ss}.log",
        rotation="10 MB",
        compression="zip",
        level=log_level,
    )

    logger.info("=== 데이터 레이크 적재 파이프라인 시작 ===")
    pipeline_start_time = time.perf_counter()

    client_id = os.getenv("IGDB_CLIENT_ID")
    static_token = os.getenv("IGDB_STATIC_TOKEN")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not all([client_id, static_token, bucket_name]):
        logger.error("필수 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return

    auth_provider = StaticAuthProvider(token=static_token)

    async with create_clients() as (http_client, s3_client):
        state_manager = S3StateManager(client=s3_client, bucket_name=bucket_name)
        loader = S3Loader(client=s3_client, bucket_name=bucket_name)

        extractors = {
            entity_name: entity_extractor(
                client=http_client, 
                auth_provider=auth_provider, 
                client_id=client_id
            ) 
            for entity_name, entity_extractor in ALL_ENTITIES.items()
        }

        # 정의된 순서대로 실행 (차원 → 팩트)
        for entity_name in EXECUTION_ORDER:
            extractor = extractors[entity_name]
            await run_entity_pipeline(
                extractor=extractor,
                loader=loader,
                state_manager=state_manager,
                entity_name=entity_name,
                full_refresh=full_refresh
            )

    pipeline_end_time = time.perf_counter()
    total_elapsed_time = pipeline_end_time - pipeline_start_time

    logger.success("=== 데이터 레이크 적재 파이프라인 완료 ===")
    logger.success(f"총 소요 시간: {total_elapsed_time:.2f}초")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="적재 파이프라인 실행")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="전체 로드를 수행합니다. 지정하지 않으면 증분 업데이트가 수행됩니다.",
    )
    args = parser.parse_args()
    asyncio.run(main(full_refresh=args.full_refresh))