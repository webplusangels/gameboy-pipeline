from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from src.pipeline.batch_processor import BatchResult
from src.pipeline.orchestrator import PipelineOrchestrator


@pytest.mark.asyncio
async def test_orchestrator_run_full_refresh(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    Full Refresh 모드일 때의 흐름을 테스트합니다.

    Verifies:
        1. mark_old_files_as_outdated가 호출되는지
        2. state_manager에서 last_run_time이 None으로 반환되는지
        3. manifest, tag_files_as_final가 호출되는지
        4. invalidate_cloudfront_cache가 호출되는지
    """
    target_date = "2025-01-01"

    mock_batch_results = BatchResult(
        uploaded_files=["file1.json", "file2.json"],
        total_count=200,
        batch_count=2,
    )

    with (
        patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor,
        patch(
            "src.pipeline.orchestrator.list_files_with_tag",
            new_callable=AsyncMock,
            return_value=["raw/games/old-file.jsonl"],
        ) as mock_list_files,
        patch(
            "src.pipeline.orchestrator.mark_old_files_as_outdated",
            new_callable=AsyncMock,
        ) as mock_mark_old_files,
        patch(
            "src.pipeline.orchestrator.update_manifest", new_callable=AsyncMock
        ) as mock_update_manifest,
        patch(
            "src.pipeline.orchestrator.tag_files_as_final", new_callable=AsyncMock
        ) as mock_tag_files,
        patch(
            "src.pipeline.orchestrator.invalidate_cloudfront_cache",
            new_callable=AsyncMock,
        ) as mock_invalidate_cache,
        patch("src.pipeline.orchestrator.EXECUTION_ORDER", ["games"]),
    ):
        mock_bp_instance = mock_batch_processor.return_value
        mock_bp_instance.process = AsyncMock(return_value=mock_batch_results)

        orchestrator = PipelineOrchestrator(
            **mock_dependencies,
            extractors=mock_extractors,
        )

        results = await orchestrator.run(full_refresh=True, target_date=target_date)

        mock_list_files.assert_awaited_once()

        mock_mark_old_files.assert_awaited_once_with(
            s3_client=mock_dependencies["s3_client"],
            bucket_name=mock_dependencies["bucket_name"],
            file_keys=["raw/games/old-file.jsonl"],
        )

        mock_bp_instance.process.assert_called_once_with(
            extractor=mock_extractors["games"],
            entity_name="games",
            dt_partition=target_date,
            last_run_time=None,
            concurrent=True,
        )

        mock_update_manifest.assert_awaited_once()
        mock_tag_files.assert_awaited_once()

        mock_dependencies["state_manager"].save_last_run_time.assert_called_once()
        mock_invalidate_cache.assert_called_once()

        assert len(results) == 1
        assert results[0].record_count == 200
        assert results[0].mode == "full"


async def test_orchestrator_run_incremental_no_data(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    증분 모드에서 데이터가 없을 때의 흐름을 테스트합니다.

    Verifies:
        1. mark_old_files_as_outdated가 호출되지 않는지
        2. manifest, tag_files_as_final가 호출되지 않는지
        3. invalidate_cloudfront_cache가 호출되지 않는지
    """
    mock_batch_results = BatchResult(
        uploaded_files=[],
        total_count=0,
        batch_count=0,
    )

    last_run_time = datetime(2025, 1, 1)
    mock_dependencies["state_manager"].get_last_run_time = AsyncMock(
        return_value=last_run_time
    )

    with (
        patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor,
        patch(
            "src.pipeline.orchestrator.mark_old_files_as_outdated",
            new_callable=AsyncMock,
        ) as mock_mark_old_files,
        patch(
            "src.pipeline.orchestrator.update_manifest", new_callable=AsyncMock
        ) as mock_update_manifest,
        patch(
            "src.pipeline.orchestrator.tag_files_as_final", new_callable=AsyncMock
        ) as mock_tag_files,
        patch("src.pipeline.orchestrator.EXECUTION_ORDER", ["games"]),
    ):
        mock_bp_instance = mock_batch_processor.return_value
        mock_bp_instance.process = AsyncMock(return_value=mock_batch_results)

        orchestrator = PipelineOrchestrator(
            **mock_dependencies,
            extractors=mock_extractors,
        )

        results = await orchestrator.run(full_refresh=False)

        mock_mark_old_files.assert_not_called()
        mock_bp_instance.process.assert_called_once()
        call_kwargs = mock_bp_instance.process.call_args.kwargs
        assert call_kwargs["last_run_time"] == last_run_time

        mock_update_manifest.assert_not_called()
        mock_tag_files.assert_not_called()

        mock_dependencies["state_manager"].save_last_run_time.assert_called_once()

        assert results[0].record_count == 0
        assert results[0].mode == "incremental"


@pytest.mark.asyncio
async def test_orchestrator_full_refresh_extraction_failure_no_outdated(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    Full Refresh 모드에서 추출기가 실패하고 기존 파일이 outdated로 변경되지 않는 경우의 흐름을 테스트합니다.

    Verifies:
        1. mark_old_files_as_outdated가 호출되지 않는지
        2. 예외가 상위로 전파되는지
        3. 기존 데이터가 안전하게 유지되는지
    """
    with (
        patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor,
        patch(
            "src.pipeline.orchestrator.list_files_with_tag",
            new_callable=AsyncMock,
            return_value=["raw/games/old-file.jsonl"],
        ) as mock_list_files,
        patch(
            "src.pipeline.orchestrator.mark_old_files_as_outdated",
            new_callable=AsyncMock,
        ) as mock_mark_old_files,
        patch("src.pipeline.orchestrator.EXECUTION_ORDER", ["games"]),
    ):
        mock_bp_instance = mock_batch_processor.return_value
        mock_bp_instance.process = AsyncMock(
            side_effect=Exception("IGDB API 호출 실패")
        )

        orchestrator = PipelineOrchestrator(
            **mock_dependencies,
            extractors=mock_extractors,
        )

        with pytest.raises(Exception, match="IGDB API 호출 실패"):
            await orchestrator.run(full_refresh=True)

        mock_list_files.assert_awaited_once()

        mock_mark_old_files.assert_not_called()


@pytest.mark.asyncio
async def test_orchestrator_popscore_no_outdated_tagging(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    PopScore 엔티티는 시계열 데이터이므로 Full Refresh 시에도 outdated 태그를 적용하지 않는지 테스트합니다.

    Verifies:
        1. Full Refresh 모드여도 list_files_with_tag가 호출되지 않는지
        2. mark_old_files_as_outdated가 호출되지 않는지
        3. 모든 파일이 'final' 상태로 유지되는지
    """
    target_date = "2025-01-01"

    mock_batch_results = BatchResult(
        uploaded_files=["raw/popscore/2025-01-01_popscore_1.jsonl"],
        total_count=1000,
        batch_count=1,
    )

    with (
        patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor,
        patch(
            "src.pipeline.orchestrator.list_files_with_tag",
            new_callable=AsyncMock,
        ) as mock_list_files,
        patch(
            "src.pipeline.orchestrator.mark_old_files_as_outdated",
            new_callable=AsyncMock,
        ) as mock_mark_old_files,
        patch(
            "src.pipeline.orchestrator.delete_files_in_partition",
            new_callable=AsyncMock,
            return_value=0,
        ) as mock_delete,
        patch(
            "src.pipeline.orchestrator.move_files_atomically",
            new_callable=AsyncMock,
            return_value=1,
        ) as mock_move,
        patch(
            "src.pipeline.orchestrator.update_manifest", new_callable=AsyncMock
        ) as mock_update_manifest,
        patch(
            "src.pipeline.orchestrator.tag_files_as_final", new_callable=AsyncMock
        ) as mock_tag_files,
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

        # 시계열 데이터는 outdated 처리를 하지 않음
        mock_list_files.assert_not_called()
        mock_mark_old_files.assert_not_called()

        # 대신 temp 디렉토리 방식 사용
        mock_delete.assert_awaited_once()  # 기존 파일 삭제
        mock_move.assert_awaited_once()  # temp → 본 디렉토리 이동

        # 항상 전체 추출 모드 (last_run_time=None)
        mock_bp_instance.process.assert_called_once()
        call_kwargs = mock_bp_instance.process.call_args.kwargs
        assert call_kwargs["extractor"] == mock_extractors["popscore"]
        assert call_kwargs["entity_name"] == "popscore"
        assert call_kwargs["dt_partition"].startswith(target_date)  # temp 경로 포함
        assert "_temp_" in call_kwargs["dt_partition"]  # temp 디렉토리 사용
        assert call_kwargs["last_run_time"] is None
        assert call_kwargs["concurrent"] is True

        # 새 파일은 final 태그 적용
        mock_tag_files.assert_awaited_once()
        mock_update_manifest.assert_awaited_once()

        assert len(results) == 1
        assert results[0].record_count == 1000
        assert results[0].mode == "full"


@pytest.mark.asyncio
async def test_orchestrator_popscore_always_full_extraction(
    mock_dependencies: dict[str, AsyncMock],
    mock_extractors: dict[str, AsyncMock],
):
    """
    PopScore 엔티티는 증분 추출을 지원하지 않으므로 항상 전체 추출을 수행하는지 테스트합니다.

    Verifies:
        1. 증분 모드로 실행해도 last_run_time이 None으로 전달되는지
        2. 시계열 데이터 특성상 항상 전체 추출을 수행하는지
    """
    target_date = "2025-01-02"

    mock_batch_results = BatchResult(
        uploaded_files=["raw/popscore/2025-01-02_popscore_1.jsonl"],
        total_count=1500,
        batch_count=1,
    )

    # 이전 실행 기록이 있는 상태로 설정
    last_run_time = datetime(2025, 1, 1)
    mock_dependencies["state_manager"].get_last_run_time = AsyncMock(
        return_value=last_run_time
    )

    with (
        patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor,
        patch(
            "src.pipeline.orchestrator.delete_files_in_partition",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "src.pipeline.orchestrator.move_files_atomically",
            new_callable=AsyncMock,
            return_value=1,
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

        # 증분 모드로 실행
        results = await orchestrator.run(full_refresh=False, target_date=target_date)

        # 시계열 데이터는 항상 last_run_time=None으로 전체 추출
        mock_bp_instance.process.assert_called_once()
        call_kwargs = mock_bp_instance.process.call_args.kwargs
        assert call_kwargs["extractor"] == mock_extractors["popscore"]
        assert call_kwargs["entity_name"] == "popscore"
        assert call_kwargs["dt_partition"].startswith(target_date)  # temp 경로 포함
        assert "_temp_" in call_kwargs["dt_partition"]  # temp 디렉토리 사용
        assert call_kwargs["last_run_time"] is None  # 증분 모드여도 None
        assert call_kwargs["concurrent"] is True

        assert len(results) == 1
        assert results[0].mode == "full"  # 항상 full 모드
