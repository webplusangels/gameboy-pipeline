from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

from loguru import logger

from src.pipeline.batch_processor import BatchProcessor
from src.pipeline.constants import EXECUTION_ORDER
from src.pipeline.extractors import BaseIgdbExtractor
from src.pipeline.interfaces import Extractor, Loader, StateManager
from src.pipeline.manifest import update_manifest
from src.pipeline.s3_ops import (
    invalidate_cloudfront_cache,
    mark_old_files_as_outdated,
    tag_files_as_final,
)


@dataclass
class PipelineResult:
    """
    파이프라인 실행 결과를 나타내는 데이터 클래스입니다.
    """

    entity_name: str
    record_count: int
    file_count: int
    elapsed_seconds: float
    mode: str  # 'full' 또는 'incremental'


class PipelineOrchestrator:
    """
    데이터 파이프라인의 오케스트레이션을 담당하는 클래스입니다.
    """

    def __init__(
        self,
        s3_client: Any,
        cloudfront_client: Any,
        extractors: dict[str, BaseIgdbExtractor],
        loader: Loader,
        state_manager: StateManager,
        bucket_name: str,
        cloudfront_distribution_id: str | None = None,
    ) -> None:
        self._s3_client = s3_client
        self._cloudfront_client = cloudfront_client
        self._extractors = extractors
        self._loader = loader
        self._state_manager = state_manager
        self._bucket_name = bucket_name
        self._cloudfront_distribution_id = cloudfront_distribution_id
        self._batch_processor = BatchProcessor(loader=loader)

    async def run(
        self,
        full_refresh: bool = False,
        target_date: str | None = None,
    ) -> list[PipelineResult]:
        """
        모든 엔티티에 대해 파이프라인을 실행합니다.

        Args:
            full_refresh (bool): 전체 로드 여부
            target_date (str | None): 대상 날짜 (예: '2023-10-01')

        Returns:
            list[PipelineResult]: 각 엔티티에 대한 파이프라인 실행 결과 목록
        """
        dt_partition = target_date or datetime.now(UTC).strftime("%Y-%m-%d")
        logger.info(
            f"파이프라인 실행 시작 - 날짜 파티션: {dt_partition}, 전체 갱신: {full_refresh}"
        )

        results: list[PipelineResult] = []

        for entity_name in EXECUTION_ORDER:
            extractor = self._extractors[entity_name]
            result = await self._run_entity(
                extractor=extractor,
                entity_name=entity_name,
                dt_partition=dt_partition,
                full_refresh=full_refresh,
            )
            results.append(result)

        # 전체 엔티티 처리 후 CloudFront 캐시 무효화
        await invalidate_cloudfront_cache(
            cloudfront_client=self._cloudfront_client,
            cloudfront_distribution_id=self._cloudfront_distribution_id,
            dt_partition=dt_partition,
        )

        logger.info("파이프라인 실행 완료")
        return results

    async def _run_entity(
        self,
        extractor: Extractor,
        entity_name: str,
        dt_partition: str,
        full_refresh: bool,
    ) -> PipelineResult:
        """
        단일 엔티티에 대해 파이프라인을 실행합니다.

        Args:
            extractor (Extractor): 데이터 추출기 인스턴스
            entity_name (str): 엔티티 이름
            dt_partition (str): 날짜 파티션 문자열
            full_refresh (bool): 전체 로드 여부

        Returns:
            PipelineResult: 엔티티에 대한 파이프라인 실행 결과
        """
        logger.info(f"엔티티 '{entity_name}' 파이프라인 실행 시작...")
        start_time = perf_counter()
        extraction_start = datetime.now(UTC)

        # Full Refresh시 기존 파일 outdated 태그 처리
        if full_refresh:
            await mark_old_files_as_outdated(
                s3_client=self._s3_client,
                bucket_name=self._bucket_name,
                entity_name=entity_name,
            )

        # 실행 컨텍스트 결정
        last_run_time = await self._determine_execution_context(
            entity_name=entity_name,
            full_refresh=full_refresh,
        )

        # 추출 및 적재
        batch_result = await self._batch_processor.process(
            extractor=extractor,
            entity_name=entity_name,
            dt_partition=dt_partition,
            last_run_time=last_run_time,
        )
        new_files = batch_result.uploaded_files
        new_count = batch_result.total_count

        # 데이터가 있는 경우
        if new_count > 0:
            # 매니페스트 업데이트
            await update_manifest(
                s3_client=self._s3_client,
                bucket_name=self._bucket_name,
                entity_name=entity_name,
                dt_partition=dt_partition,
                new_files=new_files,
                new_count=new_count,
                extraction_start=extraction_start,
                full_refresh=full_refresh,
            )

            # 새 파일 final 태그 처리
            await tag_files_as_final(
                s3_client=self._s3_client,
                bucket_name=self._bucket_name,
                entity_name=entity_name,
                file_keys=new_files,
            )

        # 마지막 실행 시간 저장
        await self._state_manager.save_last_run_time(
            entity=entity_name,
            run_time=extraction_start,
        )

        elapsed = perf_counter() - start_time
        mode = "incremental" if last_run_time else "full"

        logger.success(
            f"엔티티 '{entity_name}' 파이프라인 실행 완료 - 모드: {mode}, "
            f"신규 레코드 수: {new_count}, 신규 파일 수: {len(new_files)}, "
            f"소요 시간: {elapsed:.2f}초"
        )

        return PipelineResult(
            entity_name=entity_name,
            record_count=new_count,
            file_count=len(new_files),
            elapsed_seconds=elapsed,
            mode=mode,
        )

    async def _determine_execution_context(
        self,
        entity_name: str,
        full_refresh: bool,
    ) -> datetime | None:
        """
        실행 컨텍스트(전체 로드 또는 증분 업데이트)를 결정합니다.

        Args:
            entity_name (str): 엔티티 이름
            full_refresh (bool): 전체 로드 여부

        Returns:
            datetime | None: 마지막 실행 시간 또는 None(전체 로드 시)
        """
        if full_refresh:
            logger.info(f"엔티티 '{entity_name}' 전체 로드 모드로 실행")
            return None

        last_run_time = await self._state_manager.get_last_run_time(entity_name)
        if last_run_time:
            logger.info(
                f"엔티티 '{entity_name}' 증분 업데이트 모드로 실행 - "
                f"마지막 실행 시간: {last_run_time.isoformat()}"
            )
        else:
            logger.info(
                f"엔티티 '{entity_name}' 전체 로드 모드로 실행 (이전 실행 기록 없음)"
            )

        return last_run_time
