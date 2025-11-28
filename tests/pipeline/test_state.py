import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from more_itertools import side_effect
import pytest
from botocore.exceptions import ClientError

from src.pipeline.state import S3StateManager


@pytest.mark.asyncio
async def test_s3_state_manager_get_last_run_time_exists(mock_client):
    """
    [GREEN]
    S3StateManager가 존재하는 상태를 올바르게 조회하는지 테스트합니다.
    """

    mock_s3_client = mock_client
    # 수정: 표준 키 "last_run_time" 사용
    state_data = {
        "last_run_time": "2025-11-10T10:00:00+00:00",
        "updated_at": "2025-11-10T10:05:00+00:00",
    }
    mock_response = {"Body": AsyncMock()}
    mock_response["Body"].read = AsyncMock(return_value=json.dumps(state_data).encode())
    mock_s3_client.get_object = AsyncMock(return_value=mock_response)
    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    result = await state_manager.get_last_run_time("games")

    assert result is not None
    assert result.year == 2025
    assert result.month == 11
    assert result.day == 10
    assert result.hour == 10
    assert result.tzinfo == UTC  # UTC 확인

    # S3 get_object 호출 확인 (엔티티별 파일)
    mock_s3_client.get_object.assert_called_once_with(
        Bucket="test-bucket", Key="pipeline/state/games.json"
    )


@pytest.mark.asyncio
async def test_s3_state_manager_get_last_run_time_not_exists(mock_client):
    """
    [GREEN]
    S3StateManager가 상태 파일이 없을 때 None을 반환하는지 테스트합니다.
    (첫 실행 시나리오)
    """

    mock_s3_client = mock_client
    mock_s3_client.get_object = AsyncMock(
        side_effect=ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    )
    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    result = await state_manager.get_last_run_time("games")

    assert result is None  # 전체 로드 시그널


@pytest.mark.asyncio
async def test_s3_state_manager_get_last_run_time_entity_not_in_state(
    mock_client
):
    """
    [GREEN]
    상태 파일은 있지만 'last_run_time' 키가 없을 때 None 반환 테스트
    """

    mock_s3_client = mock_client
    # 수정: 빈 상태 파일 또는 다른 키만 있는 경우
    state_data = {
        "updated_at": "2025-11-09T15:30:00+00:00",  # last_run_time 없음
    }
    mock_response = {"Body": AsyncMock()}
    mock_response["Body"].read = AsyncMock(return_value=json.dumps(state_data).encode())
    mock_s3_client.get_object = AsyncMock(return_value=mock_response)
    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    result = await state_manager.get_last_run_time("games")

    assert result is None  # last_run_time 키 없으면 전체 로드


@pytest.mark.asyncio
async def test_s3_state_manager_save_last_run_time_new_state(mock_client):
    """
    [GREEN]
    S3StateManager가 새로운 상태를 올바르게 저장하는지 테스트합니다.
    """

    mock_s3_client = mock_client
    mock_s3_client.get_object = AsyncMock(
        side_effect=ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    )

    mock_s3_client.put_object = AsyncMock()
    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    run_time = datetime(2025, 11, 11, 12, 30, 0, tzinfo=UTC)
    await state_manager.save_last_run_time("games", run_time)

    mock_s3_client.put_object.assert_called_once()
    call_args = mock_s3_client.put_object.call_args

    assert call_args.kwargs["Bucket"] == "test-bucket"
    assert call_args.kwargs["Key"] == "pipeline/state/games.json"
    assert call_args.kwargs["ContentType"] == "application/json"

    # 수정: 표준 키 검증
    saved_state = json.loads(call_args.kwargs["Body"])
    assert "last_run_time" in saved_state
    assert saved_state["last_run_time"] == "2025-11-11T12:30:00+00:00"
    assert "updated_at" in saved_state  # 메타데이터 확인


@pytest.mark.asyncio
async def test_s3_state_manager_save_last_run_time_update_existing(mock_client):
    """
    [GREEN]
    기존 상태를 업데이트하는 시나리오 테스트
    (메타데이터 유지 확인)
    """

    mock_s3_client = mock_client
    # 수정: 기존 상태에 메타데이터 포함
    existing_state = {
        "last_run_time": "2025-11-09T15:30:00+00:00",
        "updated_at": "2025-11-09T15:35:00+00:00",
        "records_processed": 220,  # 추가 메타데이터
    }
    mock_response = {"Body": AsyncMock()}
    mock_response["Body"].read = AsyncMock(
        return_value=json.dumps(existing_state).encode()
    )
    mock_s3_client.get_object = AsyncMock(return_value=mock_response)
    mock_s3_client.put_object = AsyncMock()

    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    run_time = datetime(2025, 11, 11, 14, 0, 0, tzinfo=UTC)
    await state_manager.save_last_run_time("games", run_time)

    call_args = mock_s3_client.put_object.call_args
    saved_state = json.loads(call_args.kwargs["Body"])

    # 수정: 표준 키로 업데이트 확인
    assert "last_run_time" in saved_state
    assert saved_state["last_run_time"] == "2025-11-11T14:00:00+00:00"
    assert "updated_at" in saved_state  # 업데이트 시간 갱신됨
    assert "records_processed" in saved_state  # 기존 메타데이터 유지
    assert saved_state["records_processed"] == 220


@pytest.mark.asyncio
async def test_s3_state_manager_save_last_run_time_naive_datetime(mock_client):
    """
    [GREEN]
    timezone-naive datetime을 저장할 때 UTC로 간주하는지 테스트합니다.
    """

    mock_s3_client = mock_client
    existing_state = {
        "last_run_time": "2025-11-09T15:30:00+00:00",
        "updated_at": "2025-11-09T15:35:00+00:00",
    }
    mock_response = {"Body": AsyncMock()}
    mock_response["Body"].read = AsyncMock(
        return_value=json.dumps(existing_state).encode()
    )
    mock_s3_client.get_object = AsyncMock(return_value=mock_response)
    mock_s3_client.put_object = AsyncMock()

    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    # timezone-naive datetime
    run_time = datetime(2025, 11, 11, 14, 0, 0)
    await state_manager.save_last_run_time("games", run_time)

    call_args = mock_s3_client.put_object.call_args
    saved_state = json.loads(call_args.kwargs["Body"])

    # 수정: 표준 키 검증
    assert "last_run_time" in saved_state
    assert saved_state["last_run_time"] == "2025-11-11T14:00:00+00:00"  # UTC로 변환됨


@pytest.mark.asyncio
async def test_s3_state_manager_reset_state(mock_client):
    """
    [GREEN]
    S3StateManager의 상태 초기화 기능 테스트
    """
    mock_s3_client = mock_client
    mock_s3_client.delete_object = AsyncMock()

    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    await state_manager.reset_state("games")

    mock_s3_client.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="pipeline/state/games.json"
    )


@pytest.mark.asyncio
async def test_s3_state_manager_list_states(mock_client):
    """
    [GREEN]
    모든 엔티티 상태 조회 기능 테스트
    """
    mock_s3_client = mock_client

    # list_objects_v2 paginator 모킹
    mock_paginator = AsyncMock()
    mock_s3_client.get_paginator = lambda op: mock_paginator

    # S3에 2개의 상태 파일 존재
    async def mock_paginate(**kwargs):
        yield {
            "Contents": [
                {"Key": "pipeline/state/games.json"},
                {"Key": "pipeline/state/platforms.json"},
            ]
        }

    mock_paginator.paginate = mock_paginate

    # 각 엔티티별 상태 파일 응답 모킹
    async def mock_get_object(**kwargs):
        key = kwargs.get("Key", "")
        if "games" in key:
            body_data = {
                "last_run_time": "2025-11-10T10:00:00+00:00",
            }
        else:  # platforms
            body_data = {
                "last_run_time": "2025-11-09T15:30:00+00:00",
            }

        mock_response = {"Body": AsyncMock()}
        mock_response["Body"].read = AsyncMock(
            return_value=json.dumps(body_data).encode()
        )
        return mock_response

    mock_s3_client.get_object = mock_get_object

    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    # Act
    result = await state_manager.list_states()

    # Assert
    assert len(result) == 2
    assert "games" in result
    assert "platforms" in result
    assert result["games"].isoformat() == "2025-11-10T10:00:00+00:00"
    assert result["platforms"].isoformat() == "2025-11-09T15:30:00+00:00"

@pytest.mark.asyncio
async def test_s3_state_manager_client_error_handling(mock_client):
    """
    S3StateManager가 S3 클라이언트 오류를 적절히 처리하는지 테스트합니다.
    """
    mock_s3_client = mock_client
    mock_s3_client.get_object = AsyncMock(
        side_effect=ClientError({"Error": {"Code": "500", "Message": "InternalError"}}, "GetObject")
    )

    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    with pytest.raises(ClientError) as exc_info:
        await state_manager.get_last_run_time("games")

    assert exc_info.value.response["Error"]["Code"] == "500"
    assert exc_info.value.response["Error"]["Message"] == "InternalError"

@pytest.mark.asyncio
async def test_s3_state_manager_exception_handling(mock_client):
    """
    S3StateManager가 일반 예외를 적절히 처리하는지 테스트합니다.
    """
    mock_s3_client = mock_client
    mock_s3_client.get_object = AsyncMock(
        side_effect=ValueError("Some unexpected error")
    )

    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket", state_prefix="pipeline/state/"
    )

    result = await state_manager.get_last_run_time("games")
    assert result is None

@pytest.mark.asyncio
async def test_s3_state_manager_save_last_run_time_client_error_handling(mock_client):
    """
    S3StateManager의 save_last_run_time 메서드가 S3 클라이언트 오류를 적절히 처리하는지 테스트합니다.
    """
    mock_s3_client = mock_client
    mock_s3_client.get_object = AsyncMock(
        side_effect=ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "GetObject"
        )
    )

    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket"
    )

    with pytest.raises(ClientError) as exc_info:
        await state_manager.save_last_run_time("games", datetime.now(UTC))

    assert exc_info.value.response["Error"]["Code"] == "AccessDenied"
    # put_object이 호출되지 않았는지 확인
    mock_s3_client.put_object.assert_not_called()

@pytest.mark.asyncio
async def test_s3_state_manager_save_last_run_time_put_object_failure(mock_client):
    """
    S3StateManager의 save_last_run_time 메서드가 S3 put_object 실패를 적절히 처리하는지 테스트합니다.

    실패 시에 예외를 전파함
    """
    mock_s3_client = mock_client
    mock_s3_client.get_object = AsyncMock(
        side_effect=ClientError(
            {"Error": {"Code": "NoSuchKey"}},
            "GetObject"
        )
    )

    mock_s3_client.put_object = AsyncMock(
        side_effect=ClientError(
            {"Error": {"Code": "InternalError", "Message": "Internal Server Error"}},
            "PutObject"
        )
    )

    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket"
    )

    with pytest.raises(ClientError) as exc_info:
        await state_manager.save_last_run_time("games", datetime.now(UTC))

    assert exc_info.value.response["Error"]["Code"] == "InternalError"

@pytest.mark.asyncio
async def test_s3_state_manager_reset_state_failure(mock_client):
    """
    S3StateManager의 reset_state 메서드가 S3 삭제 실패를 적절히 처리하는지 테스트합니다.
    """
    mock_s3_client = mock_client
    mock_s3_client.delete_object = AsyncMock(
        side_effect=ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "DeleteObject"
        )
    )
    
    state_manager = S3StateManager(
        client=mock_s3_client, bucket_name="test-bucket"
    )
    
    with pytest.raises(ClientError) as exc_info:
        await state_manager.reset_state("games")
    
    assert exc_info.value.response["Error"]["Code"] == "AccessDenied"