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

    with patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor, \
         patch("src.pipeline.orchestrator.mark_old_files_as_outdated", new_callable=AsyncMock) as mock_mark_old_files, \
         patch("src.pipeline.orchestrator.update_manifest", new_callable=AsyncMock) as mock_update_manifest, \
         patch("src.pipeline.orchestrator.tag_files_as_final", new_callable=AsyncMock) as mock_tag_files, \
         patch("src.pipeline.orchestrator.invalidate_cloudfront_cache", new_callable=AsyncMock) as mock_invalidate_cache, \
         patch("src.pipeline.orchestrator.EXECUTION_ORDER", ["games"]):

        mock_bp_instance = mock_batch_processor.return_value
        mock_bp_instance.process = AsyncMock(return_value=mock_batch_results)

        orchestrator = PipelineOrchestrator(
            **mock_dependencies,
            extractors=mock_extractors,
        )

        results = await orchestrator.run(full_refresh=True, target_date=target_date)

        mock_mark_old_files.assert_awaited_once_with(
            s3_client=mock_dependencies["s3_client"],
            bucket_name=mock_dependencies["bucket_name"],
            entity_name="games",
        )

        mock_bp_instance.process.assert_called_once_with(
            extractor=mock_extractors["games"],
            entity_name="games",
            dt_partition=target_date,
            last_run_time=None,
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
    mock_dependencies["state_manager"].get_last_run_time = AsyncMock(return_value=last_run_time)

    with patch("src.pipeline.orchestrator.BatchProcessor") as mock_batch_processor, \
         patch("src.pipeline.orchestrator.mark_old_files_as_outdated", new_callable=AsyncMock) as mock_mark_old_files, \
         patch("src.pipeline.orchestrator.update_manifest", new_callable=AsyncMock) as mock_update_manifest, \
         patch("src.pipeline.orchestrator.tag_files_as_final", new_callable=AsyncMock) as mock_tag_files, \
         patch("src.pipeline.orchestrator.EXECUTION_ORDER", ["games"]):

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
