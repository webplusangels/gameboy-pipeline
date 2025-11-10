import asyncio
import os
import sys
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

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
from src.pipeline.interfaces import Extractor, Loader
from src.pipeline.loaders import S3Loader

load_dotenv()
BATCH_SIZE = 50000


@asynccontextmanager
async def create_clients() -> AsyncGenerator[tuple[httpx.AsyncClient, Any], None]:
    """
    EL 파이프라인에 필요한 비동기 클라이언트를 생성하고 세션을 관리합니다.
    Yields:
        tuple[httpx.AsyncClient, Any]: HTTP 클라이언트와 기타 필요한 클라이언트 객체
    """
    logger.info("HTTPX AsyncClient 세션 생성...")
    region = os.getenv("AWS_REGION", "ap-northeast-2")
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
    entity_name: str,
) -> None:
    """
    하나의 엔티티에 대한 EL 파이프라인을 실행합니다.

    Args:
        extractor (Extractor): 데이터 추출기 인스턴스
        loader (Loader): 데이터 적재기 인스턴스
        entity_name (str): 처리할 엔티티 이름 (예: "games", "platforms")
    """
    logger.info(f"{entity_name} 엔티티에 대한 EL 파이프라인 시작...")

    start_time = time.perf_counter()

    batch = []
    total_count = 0
    batch_count = 0

    try:
        async for item in extractor.extract():
            batch.append(item)
            total_count += 1

            if len(batch) >= BATCH_SIZE:
                key = f"raw/{entity_name}/batch-{batch_count}-{uuid.uuid4()}.jsonl"
                await loader.load(batch, key)
                logger.info(
                    f"S3에 {entity_name} 배치 {batch_count} 적재 완료: {len(batch)}개 항목"
                )
                batch.clear()
                batch_count += 1

        if batch:
            key = f"raw/{entity_name}/batch-{batch_count}-{uuid.uuid4()}.jsonl"
            await loader.load(batch, key)
            logger.info(
                f"S3에 {entity_name} 마지막 배치 {batch_count} 적재 완료: {len(batch)}개 항목"
            )

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        logger.success(
            f"{entity_name} 엔티티 EL 파이프라인 완료 "
            f"총 {total_count}개 항목 처리, 소요 시간: {elapsed_time:.2f}초"
        )

    except Exception as e:
        logger.error(f"{entity_name} 엔티티 EL 파이프라인 실패: {e}")
        raise


async def main() -> None:
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
        loader = S3Loader(client=s3_client, bucket_name=bucket_name)

        extractors = {
            "games": IgdbExtractor(
                client=http_client,
                auth_provider=auth_provider,
                client_id=client_id,
            ),
            "platforms": IgdbPlatformExtractor(
                client=http_client,
                auth_provider=auth_provider,
                client_id=client_id,
            ),
            "genres": IgdbGenreExtractor(
                client=http_client,
                auth_provider=auth_provider,
                client_id=client_id,
            ),
            "game_modes": IgdbGameModeExtractor(
                client=http_client,
                auth_provider=auth_provider,
                client_id=client_id,
            ),
            "player_perspectives": IgdbPlayerPerspectiveExtractor(
                client=http_client,
                auth_provider=auth_provider,
                client_id=client_id,
            ),
            "themes": IgdbThemeExtractor(
                client=http_client,
                auth_provider=auth_provider,
                client_id=client_id,
            ),
        }

        for entity_name, extractor in extractors.items():
            await run_entity_pipeline(
                extractor=extractor,
                loader=loader,
                entity_name=entity_name,
            )

    pipeline_end_time = time.perf_counter()
    total_elapsed_time = pipeline_end_time - pipeline_start_time

    logger.success("=== 데이터 레이크 적재 파이프라인 완료 ===")
    logger.success(f"총 소요 시간: {total_elapsed_time:.2f}초")


if __name__ == "__main__":
    asyncio.run(main())
