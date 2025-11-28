import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from src.pipeline.manifest import update_manifest


@pytest.mark.asyncio
async def test_update_manifest_full_refresh(
    mock_s3_client: AsyncMock,
):
    """
    update_manifest 함수의 Full Refresh 모드를 테스트합니다.

    Verifies:
        1. S3에 매니페스트 파일이 새로 작성되는지
        2. 매니페스트 내용이 올바른지
    """
    # Act
    await update_manifest(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        entity_name="games",
        dt_partition="2025-01-01",
        new_files=["file1.jsonl", "file2.jsonl"],
        new_count=200,
        extraction_start=datetime.now(UTC),
        full_refresh=True,
    )

    # Assert: S3에 매니페스트 파일이 작성되었는지 확인
    mock_s3_client.put_object.assert_called_once()
    call_args = mock_s3_client.put_object.call_args
    assert call_args.kwargs["Bucket"] == "test-bucket"
    assert "_manifest.json" in call_args.kwargs["Key"]

    # 매니페스트 내용 검증
    body = json.loads(call_args.kwargs["Body"].decode("utf-8"))
    assert body["files"] == ["file1.jsonl", "file2.jsonl"]
    assert body["total_count"] == 200


@pytest.mark.asyncio
async def test_update_manifest_incremental_appends_to_existing(
    mock_s3_client: AsyncMock,
):
    """
    update_manifest 함수의 증분 모드를 테스트합니다.

    Verifies:
        1. 기존 매니페스트 파일이 로드되는지
        2. 새 파일과 카운트가 올바르게 추가되는지
    """
    # Arrange: 기존 매니페스트 파일 Mock 설정
    existing_manifest = {
        "files": ["old_file1.jsonl"],
        "total_count": 100,
        "created_at": "2025-01-01T00:00:00+00:00",
        "updated_at": "2025-01-01T00:00:00+00:00",
        "batch_count": 1,
    }
    mock_response = AsyncMock()
    mock_response["Body"].read = AsyncMock(
        return_value=json.dumps(existing_manifest).encode("utf-8")
    )
    mock_s3_client.get_object.return_value = mock_response

    # Act
    await update_manifest(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        entity_name="games",
        dt_partition="2025-01-01",
        new_files=["new_file1.jsonl"],
        new_count=50,
        extraction_start=datetime.now(UTC),
        full_refresh=False,
    )

    # Assert: S3에 매니페스트 파일이 작성되었는지 확인
    call_args = mock_s3_client.put_object.call_args
    body = json.loads(call_args.kwargs["Body"].decode("utf-8"))

    assert body["files"] == ["old_file1.jsonl", "new_file1.jsonl"]
    assert body["total_count"] == 150  # 100 + 50


@pytest.mark.asyncio
async def test_update_manifest_no_existing_file_creates_new(
    mock_s3_client: AsyncMock,
):
    """
    기존 매니페스트 파일이 없을 때 새로운 매니페스트 파일이 생성되는지 테스트합니다.

    Verifies:
        1. NoSuchKey 예외 처리
        2. 새 매니페스트 파일이 올바르게 작성되는지
    """
    # Arrange: NoSuchKey 예외 발생하도록 설정
    mock_s3_client.get_object.side_effect = mock_s3_client.exceptions.NoSuchKey

    # Act
    await update_manifest(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        entity_name="games",
        dt_partition="2025-01-01",
        new_files=["new_file1.jsonl"],
        new_count=50,
        extraction_start=datetime.now(UTC),
        full_refresh=False,
    )

    # Assert: S3에 매니페스트 파일이 작성되었는지 확인
    mock_s3_client.put_object.assert_called_once()
