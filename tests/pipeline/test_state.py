import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from botocore.exceptions import ClientError

from src.pipeline.state import S3StateManager
import pytest


@pytest.mark.asyncio
async def test_s3_state_manager_get_last_run_time_exists(mocker, mock_client):
    """
    [RED]
    S3StateManager가 존재하는 상태를 올바르게 조회하는지 테스트합니다.
    """

    mock_s3_client = mock_client
    state_data = {
        "games": "2025-11-10T10:00:00+00:00",
        "platforms": "2025-11-09T15:30:00+00:00",
    }
    mock_response = {
        "Body": AsyncMock()
    }
    mock_response["Body"].read = AsyncMock(return_value=json.dumps(state_data).encode())
    mock_s3_client.get_object = AsyncMock(return_value=mock_response)
    state_manager = S3StateManager(
        client=mock_s3_client,
        bucket_name="test-bucket",
        state_key="pipeline/state.json"
    )
    
    result = await state_manager.get_last_run_time("games")
    
    assert result is not None
    assert result.year == 2025
    assert result.month == 11
    assert result.day == 10
    assert result.hour == 10
    assert result.tzinfo == timezone.utc  # UTC 확인
    
    # S3 get_object 호출 확인
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="test-bucket",
        Key="pipeline/state.json"
    )


@pytest.mark.asyncio
async def test_s3_state_manager_get_last_run_time_not_exists(mocker, mock_client):
    """
    [RED]
    S3StateManager가 상태 파일이 없을 때 None을 반환하는지 테스트합니다.
    (첫 실행 시나리오)
    """

    mock_s3_client = mock_client
    mock_s3_client.get_object = AsyncMock(
        side_effect=ClientError(
            {"Error": {"Code": "NoSuchKey"}},
            "GetObject"
        )
    )
    state_manager = S3StateManager(
        client=mock_s3_client,
        bucket_name="test-bucket",
        state_key="pipeline/state.json"
    )

    result = await state_manager.get_last_run_time("games")
    
    assert result is None  # 전체 로드 시그널


@pytest.mark.asyncio
async def test_s3_state_manager_get_last_run_time_entity_not_in_state(mocker, mock_client):
    """
    [RED]
    상태 파일은 있지만 특정 엔티티가 없을 때 None 반환 테스트
    """

    mock_s3_client = mock_client
    state_data = {
        "platforms": "2025-11-09T15:30:00+00:00",
    }
    mock_response = {
        "Body": AsyncMock()
    }
    mock_response["Body"].read = AsyncMock(return_value=json.dumps(state_data).encode())
    mock_s3_client.get_object = AsyncMock(return_value=mock_response)
    state_manager = S3StateManager(
        client=mock_s3_client,
        bucket_name="test-bucket",
        state_key="pipeline/state.json"
    )
    
    result = await state_manager.get_last_run_time("games")
    
    assert result is None  # games는 첫 실행


@pytest.mark.asyncio
async def test_s3_state_manager_save_last_run_time_new_state(mocker, mock_client):
    """
    [RED]
    S3StateManager가 새로운 상태를 올바르게 저장하는지 테스트합니다.
    """

    mock_s3_client = mock_client
    mock_s3_client.get_object = AsyncMock(
        side_effect=ClientError(
            {"Error": {"Code": "NoSuchKey"}},
            "GetObject"
        )
    )
    
    mock_s3_client.put_object = AsyncMock()
    state_manager = S3StateManager(
        client=mock_s3_client,
        bucket_name="test-bucket",
        state_key="pipeline/state.json"
    )
    
    run_time = datetime(2025, 11, 11, 12, 30, 0, tzinfo=timezone.utc)
    await state_manager.save_last_run_time("games", run_time)
    
    mock_s3_client.put_object.assert_called_once()
    call_args = mock_s3_client.put_object.call_args
    
    assert call_args.kwargs["Bucket"] == "test-bucket"
    assert call_args.kwargs["Key"] == "pipeline/state.json"
    assert call_args.kwargs["ContentType"] == "application/json"
    
    saved_state = json.loads(call_args.kwargs["Body"])
    assert "games" in saved_state
    assert saved_state["games"] == "2025-11-11T12:30:00+00:00"


@pytest.mark.asyncio
async def test_s3_state_manager_save_last_run_time_update_existing(mocker, mock_client):
    """
    [RED]
    기존 상태를 업데이트하는 시나리오 테스트
    """
    
    mock_s3_client = mock_client
    existing_state = {
        "platforms": "2025-11-09T15:30:00+00:00",
    }
    mock_response = {
        "Body": AsyncMock()
    }
    mock_response["Body"].read = AsyncMock(return_value=json.dumps(existing_state).encode())
    mock_s3_client.get_object = AsyncMock(return_value=mock_response)
    mock_s3_client.put_object = AsyncMock()
    
    state_manager = S3StateManager(
        client=mock_s3_client,
        bucket_name="test-bucket",
        state_key="pipeline/state.json"
    )
    
    run_time = datetime(2025, 11, 11, 14, 0, 0, tzinfo=timezone.utc)
    await state_manager.save_last_run_time("games", run_time)
    
    call_args = mock_s3_client.put_object.call_args
    saved_state = json.loads(call_args.kwargs["Body"])
    
    assert "platforms" in saved_state
    assert saved_state["platforms"] == "2025-11-09T15:30:00+00:00"
    assert "games" in saved_state
    assert saved_state["games"] == "2025-11-11T14:00:00+00:00"
