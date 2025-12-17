"""PopScore 멱등성 및 원자적 교체 테스트"""

from unittest.mock import AsyncMock, patch

import pytest

from src.pipeline.batch_processor import BatchResult
from src.pipeline.orchestrator import PipelineOrchestrator


@pytest.mark.asyncio
async def test_popscore_uses_fixed_filename_without_uuid(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    PopScore는 UUID 없이 고정 파일명을 사용하는지 테스트합니다.

    Verifies:
        1. batch-0.jsonl, batch-1.jsonl 형식 (UUID 없음)
        2. 같은 날짜 재실행 시 덮어쓰기 가능
    """
    from src.pipeline.batch_processor import BatchProcessor

    # UUID 없는 파일명 생성 확인
    key = BatchProcessor._generate_batch_key(
        s3_path_prefix="raw/popscore/dt=2025-01-15",
        batch_count=0,
        entity_name="popscore",
    )

    assert key == "raw/popscore/dt=2025-01-15/batch-0.jsonl"
    assert "uuid" not in key.lower()  # UUID가 포함되지 않음


@pytest.mark.asyncio
async def test_general_entity_uses_uuid_filename(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    일반 엔티티(games)는 UUID를 사용하는지 테스트합니다.

    Verifies:
        1. batch-0-{uuid}.jsonl 형식
        2. 충돌 방지를 위한 UUID 사용
    """
    from src.pipeline.batch_processor import BatchProcessor

    key = BatchProcessor._generate_batch_key(
        s3_path_prefix="raw/games/dt=2025-01-15",
        batch_count=0,
        entity_name="games",
    )

    assert key.startswith("raw/games/dt=2025-01-15/batch-0-")
    assert key.endswith(".jsonl")
    assert len(key) > len("raw/games/dt=2025-01-15/batch-0-.jsonl")  # UUID 존재


@pytest.mark.asyncio
async def test_popscore_atomic_replacement_with_temp_directory(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    PopScore가 temp 디렉토리를 사용하여 원자적 교체를 수행하는지 테스트합니다.

    Verifies:
        1. temp 디렉토리에 먼저 적재
        2. 기존 파일 삭제
        3. temp → 본 디렉토리로 이동
        4. 원자적 교체 보장
    """
    target_date = "2025-01-15"

    mock_batch_results = BatchResult(
        uploaded_files=[
            "raw/popscore/dt=2025-01-15/_temp_abc123/batch-0.jsonl",
            "raw/popscore/dt=2025-01-15/_temp_abc123/batch-1.jsonl",
        ],
        total_count=2000,
        batch_count=2,
    )

    with (
        patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor,
        patch(
            "src.pipeline.orchestrator.delete_files_in_partition",
            new_callable=AsyncMock,
        ) as mock_delete,
        patch(
            "src.pipeline.orchestrator.move_files_atomically",
            new_callable=AsyncMock,
            return_value=2,  # 2개 파일 이동
        ) as mock_move,
        patch(
            "src.pipeline.orchestrator.update_manifest", new_callable=AsyncMock
        ) as mock_update_manifest,
        patch("src.pipeline.orchestrator.tag_files_as_final", new_callable=AsyncMock),
        patch(
            "src.pipeline.orchestrator.invalidate_cloudfront_cache",
            new_callable=AsyncMock,
        ),
        patch("src.pipeline.orchestrator.EXECUTION_ORDER", ["popscore"]),
    ):
        mock_bp_instance = mock_batch_processor.return_value
        mock_bp_instance.process = AsyncMock(return_value=mock_batch_results)

        orchestrator = PipelineOrchestrator(
            **mock_dependencies,
            extractors=mock_extractors,
        )

        results = await orchestrator.run(full_refresh=True, target_date=target_date)

        # 1. 기존 파일 삭제 확인
        mock_delete.assert_awaited_once_with(
            s3_client=mock_dependencies["s3_client"],
            bucket_name=mock_dependencies["bucket_name"],
            prefix=f"raw/popscore/dt={target_date}/",
        )

        # 2. temp → 본 디렉토리 이동 확인
        mock_move.assert_awaited_once()
        move_call_args = mock_move.call_args.kwargs
        assert move_call_args["dest_prefix"] == f"raw/popscore/dt={target_date}/"
        assert "_temp_" in move_call_args["source_prefix"]

        # 3. 매니페스트는 원본 파티션으로 업데이트
        manifest_call_args = mock_update_manifest.call_args.kwargs
        assert manifest_call_args["dt_partition"] == target_date  # temp 아님

        assert results[0].record_count == 2000


@pytest.mark.asyncio
async def test_popscore_idempotency_same_date_rerun(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    PopScore를 같은 날짜에 여러 번 실행해도 멱등성이 보장되는지 테스트합니다.

    Verifies:
        1. 첫 실행: batch-0.jsonl, batch-1.jsonl 생성
        2. 재실행: 기존 파일 삭제 → 새 파일 생성
        3. 최종 파일 수 동일 (중복 없음)
    """
    target_date = "2025-01-15"

    mock_batch_results = BatchResult(
        uploaded_files=[
            "raw/popscore/dt=2025-01-15/_temp_xyz/batch-0.jsonl",
            "raw/popscore/dt=2025-01-15/_temp_xyz/batch-1.jsonl",
        ],
        total_count=2000,
        batch_count=2,
    )

    with (
        patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor,
        patch(
            "src.pipeline.orchestrator.delete_files_in_partition",
            new_callable=AsyncMock,
            return_value=2,  # 기존 2개 파일 삭제
        ) as mock_delete,
        patch(
            "src.pipeline.orchestrator.move_files_atomically",
            new_callable=AsyncMock,
            return_value=2,  # 2개 파일 이동
        ),
        patch("src.pipeline.orchestrator.update_manifest", new_callable=AsyncMock),
        patch("src.pipeline.orchestrator.tag_files_as_final", new_callable=AsyncMock),
        patch(
            "src.pipeline.orchestrator.invalidate_cloudfront_cache",
            new_callable=AsyncMock,
        ),
        patch("src.pipeline.orchestrator.EXECUTION_ORDER", ["popscore"]),
    ):
        mock_bp_instance = mock_batch_processor.return_value
        mock_bp_instance.process = AsyncMock(return_value=mock_batch_results)

        orchestrator = PipelineOrchestrator(
            **mock_dependencies,
            extractors=mock_extractors,
        )

        # 첫 실행
        await orchestrator.run(full_refresh=True, target_date=target_date)

        # 재실행 (같은 날짜)
        results = await orchestrator.run(full_refresh=True, target_date=target_date)

        # 기존 파일 삭제가 호출됨 (멱등성 보장)
        assert mock_delete.call_count == 2  # 첫 실행 + 재실행

        # 최종 결과는 동일
        assert results[0].record_count == 2000


@pytest.mark.asyncio
async def test_general_entity_no_temp_directory(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    일반 엔티티(games)는 temp 디렉토리를 사용하지 않는지 테스트합니다.

    Verifies:
        1. temp 디렉토리 미사용
        2. delete_files_in_partition 호출 안 됨
        3. move_files_atomically 호출 안 됨
    """
    target_date = "2025-01-15"

    mock_batch_results = BatchResult(
        uploaded_files=["raw/games/dt=2025-01-15/batch-0-uuid.jsonl"],
        total_count=100,
        batch_count=1,
    )

    with (
        patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor,
        patch(
            "src.pipeline.orchestrator.delete_files_in_partition",
            new_callable=AsyncMock,
        ) as mock_delete,
        patch(
            "src.pipeline.orchestrator.move_files_atomically",
            new_callable=AsyncMock,
        ) as mock_move,
        patch(
            "src.pipeline.orchestrator.list_files_with_tag",
            new_callable=AsyncMock,
            return_value=[],
        ),
        patch("src.pipeline.orchestrator.update_manifest", new_callable=AsyncMock),
        patch("src.pipeline.orchestrator.tag_files_as_final", new_callable=AsyncMock),
        patch(
            "src.pipeline.orchestrator.invalidate_cloudfront_cache",
            new_callable=AsyncMock,
        ),
        patch("src.pipeline.orchestrator.EXECUTION_ORDER", ["games"]),
    ):
        mock_bp_instance = mock_batch_processor.return_value
        mock_bp_instance.process = AsyncMock(return_value=mock_batch_results)

        orchestrator = PipelineOrchestrator(
            **mock_dependencies,
            extractors=mock_extractors,
        )

        await orchestrator.run(full_refresh=True, target_date=target_date)

        # 일반 엔티티는 temp 처리 안 함
        mock_delete.assert_not_called()
        mock_move.assert_not_called()
