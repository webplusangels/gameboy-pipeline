import argparse
import asyncio
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from loguru import logger

from src.config import settings
from src.pipeline.utils import get_s3_path
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.interfaces import Extractor, Loader, StateManager
from src.pipeline.loaders import S3Loader
from src.pipeline.state import S3StateManager

from src.pipeline.constants import EXECUTION_ORDER
from src.pipeline.registry import ALL_ENTITIES
from src.pipeline.manifest import update_manifest
from src.pipeline.s3_ops import create_clients, invalidate_cloudfront_cache, mark_old_files_as_outdated, tag_files_as_final


async def _extract_and_load_batches(
    extractor: Extractor,
    loader: Loader,
    entity_name: str,
    dt_partition: str,
    last_run_time: datetime | None,
) -> tuple[list[str], int]:
    """
    주어진 Extractor를 사용하여 데이터를 추출하고, 배치 단위로 적재합니다.

    Args:
        extractor (Extractor): 데이터 추출기 인스턴스
        loader (Loader): 데이터 적재기 인스턴스
        entity_name (str): 처리할 엔티티 이름
        dt_partition (str): 날짜 파티션 문자열 (예: "2024-06-15")
        last_run_time (datetime | None): 마지막 실행 시간 (증분 업데이트용)
    Returns:
        tuple[list[str], int]: 업로드된 파일 키 목록과 총 항목 수
    """
    batch = []
    total_count = 0
    batch_count = 0
    uploaded_files = []  # 업로드된 파일 목록 추적

    s3_path_prefix = get_s3_path(entity_name, dt_partition)

    async for item in extractor.extract(last_updated_at=last_run_time):
        batch.append(item)
        total_count += 1

        if len(batch) >= settings.batch_size:
            key = f"{s3_path_prefix}/batch-{batch_count}-{uuid.uuid4()}.jsonl"
            await loader.load(batch, key)
            uploaded_files.append(key)  # 파일 목록에 추가
            logger.info(
                f"S3에 {entity_name} 배치 {batch_count} 적재 완료: {len(batch)}개 항목"
            )
            batch.clear()
            batch_count += 1

    if batch:
        key = f"{s3_path_prefix}/batch-{batch_count}-{uuid.uuid4()}.jsonl"
        await loader.load(batch, key)
        uploaded_files.append(key)  # 파일 목록에 추가
        logger.info(
            f"S3에 {entity_name} 마지막 배치 {batch_count} 적재 완료: {len(batch)}개 항목"
        )

    return uploaded_files, total_count

async def _get_execution_context(
    state_manager: StateManager,
    entity_name: str,
    full_refresh: bool,
) -> datetime | None:
    """
    실행 컨텍스트를 결정합니다. 전체 로드인지 증분 업데이트인지를 판단하고,
    증분 업데이트인 경우 마지막 실행 시간을 반환합니다.

    Args:
        state_manager (StateManager): 상태 관리 인스턴스
        entity_name (str): 처리할 엔티티 이름
        full_refresh (bool): 전체 로드 여부

    Returns:
        datetime | None: 마지막 실행 시간 (증분 업데이트인 경우), None (전체 로드인 경우)
    """
    if full_refresh:
        return None

    last_run_time = await state_manager.get_last_run_time(entity_name)
    if last_run_time:
        logger.info(
            f"'{entity_name}' 증분 업데이트 모드로 실행합니다. 마지막 실행 시간: {last_run_time.isoformat()}"
        )
    else:
        logger.info(f"'{entity_name}' 전체 로드 모드로 실행합니다. 이전 실행 기록이 없습니다.")

    return last_run_time

async def run_entity_pipeline(
    extractor: Extractor,
    loader: Loader,
    state_manager: StateManager,
    s3_client: Any,
    bucket_name: str,
    entity_name: str,
    dt_partition: str,
    full_refresh: bool = False,
) -> None:
    """
    하나의 엔티티에 대한 EL 파이프라인을 실행합니다.

    Args:
        extractor (Extractor): 데이터 추출기 인스턴스
        loader (Loader): 데이터 적재기 인스턴스
        state_manager (StateManager): 상태 관리 인스턴스
        s3_client (Any): S3 클라이언트 인스턴스
        bucket_name (str): S3 버킷 이름
        entity_name (str): 처리할 엔티티 이름 (예: "games", "platforms")
        full_refresh (bool): 전체 로드 여부
    """
    logger.info(f"{entity_name} 엔티티에 대한 EL 파이프라인 시작...")
    start_time = time.perf_counter()

    # 추출 시작 시간 기록 (State 저장 기준점)
    extraction_start = datetime.now(UTC)

    if full_refresh:
        logger.info(f"'{entity_name}' Full Refresh 모드로 실행 중...")
        await mark_old_files_as_outdated(
            s3_client=s3_client,
            bucket_name=bucket_name,
            entity_name=entity_name,
        )

    last_run_time: datetime | None = await _get_execution_context(
        state_manager=state_manager,
        entity_name=entity_name,
        full_refresh=full_refresh,
    )

    if not full_refresh and last_run_time:
        logger.info(
                f"'{entity_name}' 증분 업데이트 실행 (마지막 실행: {last_run_time.isoformat()})"
            )
    else:
        logger.info(f"'{entity_name}' 전체 로드 실행")

    try:
        new_files, new_count = await _extract_and_load_batches(
            extractor=extractor,
            loader=loader,
            entity_name=entity_name,
            dt_partition=dt_partition,
            last_run_time=last_run_time,
        )

        if new_count == 0:
            logger.info(f"{entity_name} 엔티티에 새로운 데이터가 없습니다. 파이프라인 종료.")
            await state_manager.save_last_run_time(entity_name, extraction_start)
            return

        await update_manifest(
            s3_client=s3_client,
            bucket_name=bucket_name,
            entity_name=entity_name,
            dt_partition=dt_partition,
            new_files=new_files,
            new_count=new_count,
            extraction_start=extraction_start,
            full_refresh=full_refresh,
        )

        await tag_files_as_final(
            s3_client=s3_client,
            bucket_name=bucket_name,
            entity_name=entity_name,
            file_keys=new_files,
        )

        await state_manager.save_last_run_time(entity_name, extraction_start)

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        logger.success(
            f"{entity_name} 엔티티 EL 파이프라인 완료 - "
            f"총 {new_count}개 항목 처리 "
            f"({'증분' if last_run_time else '전체'} 모드), "
            f"소요 시간: {elapsed_time:.2f}초 "
            f"({'평균 ' + str(int(new_count / elapsed_time)) + '개/초)' if elapsed_time > 0 and new_count > 0 else ''}"
        )

    except Exception as e:
        logger.error(f"{entity_name} 엔티티 EL 파이프라인 실패: {e}")
        logger.warning(
            f"'{entity_name}' State 저장 안 함 - "
            f"다음 실행 시 {'전체 로드' if not last_run_time else f'{last_run_time.isoformat()} 이후부터 재시도'}"
        )
        raise

async def main(full_refresh: bool = False, target_date: str | None = None) -> None:
    """
    EL 파이프라인의 실행 진입점입니다.
    """
    logger.remove()
    log_level = settings.log_level.upper()
    logger.add(sys.stderr, level=log_level)

    logger.add(
        "logs/pipeline_{time:YYYY-MM-DD-HH-mm-ss}.log",
        rotation="10 MB",
        compression="zip",
        level=log_level,
    )

    logger.info("=== 데이터 레이크 적재 파이프라인 시작 ===")
    pipeline_start_time = time.perf_counter()

    client_id = settings.igdb_client_id
    static_token = settings.igdb_client_secret
    bucket_name = settings.s3_bucket_name

    if not all([client_id, static_token, bucket_name]):
        logger.error("필수 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return

    assert client_id is not None
    assert static_token is not None
    assert bucket_name is not None

    auth_provider = StaticAuthProvider(token=static_token)

    if target_date:
        dt_partition = target_date
        logger.info(f"지정된 날짜 파티션 사용: {dt_partition}")
    else:
        dt_partition = datetime.now(UTC).strftime("%Y-%m-%d")
        logger.info(f"현재 날짜 파티션 사용: {dt_partition}")

    async with create_clients() as (http_client, s3_client, cloudfront_client):
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
                s3_client=s3_client,
                bucket_name=bucket_name,
                entity_name=entity_name,
                dt_partition=dt_partition,
                full_refresh=full_refresh
            )

        dist_id = settings.cloudfront_distribution_id
        
        await invalidate_cloudfront_cache(
            cloudfront_client=cloudfront_client,
            cloudfront_distribution_id=dist_id,
            dt_partition=dt_partition
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
    parser.add_argument(
        "--date",
        type=str,
        help="데이터를 적재할 날짜 파티션을 지정합니다 (형식: YYYY-MM-DD). 지정하지 않으면 현재 날짜가 사용됩니다.",
    )
    args = parser.parse_args()
    asyncio.run(main(full_refresh=args.full_refresh, target_date=args.date))
