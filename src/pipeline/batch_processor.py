"""배치 단위 데이터 추출 및 적재를 담당하는 모듈."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from loguru import logger

from src.config import settings
from src.pipeline.interfaces import Extractor, Loader
from src.pipeline.utils import get_s3_path


@dataclass
class BatchResult:
    """배치 처리 결과를 나타내는 데이터 클래스."""

    uploaded_files: list[str]
    total_count: int
    batch_count: int


class BatchProcessor:
    """
    Extractor에서 데이터를 추출하고 배치 단위로 Loader에 적재합니다.

    책임:
    - 배치 크기 관리
    - S3 키 생성
    - 추출/적재 조율

    Example:
        ```python
        processor = BatchProcessor(loader=s3_loader, batch_size=50000)
        result = await processor.process(
            extractor=game_extractor,
            entity_name="games",
            dt_partition="2025-01-15",
            last_run_time=None,
        )
        print(f"처리 완료: {result.total_count}개 레코드")
        ```
    """

    def __init__(
        self,
        loader: Loader,
        batch_size: int | None = None,
    ) -> None:
        """
        Args:
            loader: 데이터 적재기 인스턴스
            batch_size: 배치 크기 (기본값: settings.batch_size)
        """
        self._loader = loader
        self._batch_size = batch_size or settings.batch_size

    async def process(
        self,
        extractor: Extractor,
        entity_name: str,
        dt_partition: str,
        last_run_time: datetime | None = None,
    ) -> BatchResult:
        """
        데이터를 배치 단위로 추출하고 적재합니다.

        Args:
            extractor: 데이터 추출기 인스턴스
            entity_name: 엔티티 이름 (예: "games", "platforms")
            dt_partition: 날짜 파티션 문자열 (예: "2025-01-15")
            last_run_time: 마지막 실행 시간 (증분 추출용, None이면 전체 추출)

        Returns:
            BatchResult: 적재된 파일 목록, 총 레코드 수, 배치 수
        """
        uploaded_files: list[str] = []
        total_count = 0
        batch: list[dict[str, Any]] = []
        batch_count = 0

        s3_path_prefix = get_s3_path(entity_name, dt_partition)

        async for item in extractor.extract(last_updated_at=last_run_time):
            batch.append(item)
            total_count += 1

            if len(batch) >= self._batch_size:
                key = self._generate_batch_key(s3_path_prefix, batch_count)
                await self._loader.load(batch, key)
                uploaded_files.append(key)
                logger.debug(
                    f"S3에 '{entity_name}' 배치 {batch_count} 적재 완료: "
                    f"{len(batch)}개 항목"
                )
                batch.clear()
                batch_count += 1

        # 남은 배치 처리
        if batch:
            key = self._generate_batch_key(s3_path_prefix, batch_count)
            await self._loader.load(batch, key)
            uploaded_files.append(key)
            batch_count += 1

        return BatchResult(
            uploaded_files=uploaded_files,
            total_count=total_count,
            batch_count=batch_count,
        )

    @staticmethod
    def _generate_batch_key(s3_path_prefix: str, batch_count: int) -> str:
        """배치 파일의 S3 키를 생성합니다."""
        return f"{s3_path_prefix}/batch-{batch_count}-{uuid.uuid4()}.jsonl"
