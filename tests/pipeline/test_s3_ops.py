from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.pipeline.s3_ops import (
    create_clients,
    delete_files_in_partition,
    invalidate_cloudfront_cache,
    list_files_with_tag,
    mark_old_files_as_outdated,
    move_files_atomically,
    tag_files_as_final,
)


@pytest.mark.asyncio
async def test_create_clients_initialization_and_teardown():
    """
    S3 및 CloudFront 클라이언트 생성과 종료가 올바르게 수행되는지 테스트합니다.

    Verifies:
        1. S3 및 CloudFront 클라이언트가 올바르게 생성되는지
        2. 클라이언트 종료 메서드가 호출되는지
    """
    mock_session_cls = MagicMock()
    mock_session_instance = MagicMock()
    mock_session_cls.return_value = mock_session_instance

    mock_s3_context = AsyncMock()
    mock_cloudfront_context = AsyncMock()

    mock_s3_client = AsyncMock(name="mock_s3_client")
    mock_s3_context.__aenter__.return_value = mock_s3_client

    mock_cloudfront_client = AsyncMock(name="mock_cloudfront_client")
    mock_cloudfront_context.__aenter__.return_value = mock_cloudfront_client

    def client_side_effect(service_name, **kwargs):
        if service_name == "s3":
            return mock_s3_context
        elif service_name == "cloudfront":
            return mock_cloudfront_context
        return AsyncMock()

    mock_session_instance.client.side_effect = client_side_effect

    with (
        patch("src.pipeline.s3_ops.aioboto3.Session", mock_session_cls),
        patch("src.pipeline.s3_ops.httpx.AsyncClient") as mock_httpx_cls,
    ):
        # Act
        mock_httpx_instance = AsyncMock(name="mock_httpx_client")
        mock_httpx_cls.return_value = mock_httpx_instance
        mock_httpx_instance.__aenter__.return_value = mock_httpx_instance

        async with create_clients() as (http_client, s3_client, cloudfront_client):
            # Assert: 클라이언트가 올바르게 반환되는지 확인
            assert http_client == mock_httpx_instance
            assert s3_client == mock_s3_client
            assert cloudfront_client == mock_cloudfront_client

            mock_httpx_cls.assert_called_once()
            call_kwargs = mock_httpx_cls.call_args.kwargs
            assert "timeout" in call_kwargs
            assert call_kwargs["timeout"].connect == 10.0

        mock_httpx_instance.__aexit__.assert_called_once()
        mock_s3_context.__aexit__.assert_called_once()
        mock_cloudfront_context.__aexit__.assert_called_once()


@pytest.mark.asyncio
async def test_list_files_with_tag(
    mock_s3_client: AsyncMock,
):
    """
    지정된 태그를 가진 S3 파일들의 키 목록을 올바르게 조회하는지 테스트합니다.

    Verifies:
        1. S3에서 파일 목록을 올바르게 조회하는지
        2. 지정된 태그를 가진 파일들의 키가 올바르게 반환되는지
    """
    page_data = {
        "Contents": [
            {"Key": "raw/games/file1.jsonl"},
            {"Key": "raw/games/file2.jsonl"},
            {"Key": "raw/games/file3.jsonl"},
        ]
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    mock_s3_client.get_object_tagging.side_effect = [
        {"TagSet": [{"Key": "status", "Value": "final"}]},
        {"TagSet": [{"Key": "status", "Value": "temp"}]},
        {"TagSet": [{"Key": "status", "Value": "final"}]},
    ]

    result = await list_files_with_tag(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        prefix="raw/games/",
        tag_key="status",
        tag_value="final",
    )

    assert mock_s3_client.get_paginator.called
    assert mock_s3_client.get_object_tagging.call_count == 3

    expected_keys = [
        "raw/games/file1.jsonl",
        "raw/games/file3.jsonl",
    ]
    assert result == expected_keys


@pytest.mark.asyncio
async def test_list_files_with_tag_no_contents(
    mock_s3_client: AsyncMock,
):
    """
    S3에서 파일이 없을 때 빈 목록을 반환하는지 테스트합니다.

    Verifies:
        1. S3에서 파일이 없을 때 빈 목록이 반환되는지
    """
    page_data = {
        # "Contents" 키가 없음
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    result = await list_files_with_tag(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        prefix="raw/games/",
        tag_key="status",
        tag_value="final",
    )

    assert mock_s3_client.get_paginator.called
    assert mock_s3_client.get_object_tagging.call_count == 0

    assert result == []


@pytest.mark.asyncio
async def test_list_files_with_tag_tagging_failure(
    mock_s3_client: AsyncMock,
):
    """
    파일 태그 조회 실패 시나리오를 테스트합니다.

    Verifies:
        1. 태그 조회 실패 시에도 나머지 파일들은 정상 처리되는지
    """
    page_data = {
        "Contents": [
            {"Key": "raw/games/file1.jsonl"},
            {"Key": "raw/games/file2.jsonl"},
            {"Key": "raw/games/file3.jsonl"},
        ]
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    # 두 번째 파일 태그 조회 시 예외 발생
    async def get_object_tagging_side_effect(*args, **kwargs):
        if kwargs["Key"] == "raw/games/file2.jsonl":
            raise Exception("S3 Error")
        return {"TagSet": [{"Key": "status", "Value": "final"}]}

    mock_s3_client.get_object_tagging.side_effect = get_object_tagging_side_effect

    result = await list_files_with_tag(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        prefix="raw/games/",
        tag_key="status",
        tag_value="final",
    )

    assert mock_s3_client.get_paginator.called
    assert mock_s3_client.get_object_tagging.call_count == 3

    expected_keys = [
        "raw/games/file1.jsonl",
        "raw/games/file3.jsonl",
    ]
    assert result == expected_keys


@pytest.mark.asyncio
async def test_mark_old_files_tags_final_files_as_outdated(
    mock_s3_client: AsyncMock,
):
    """
    주어진 파일들을 status=outdated로 태그 변경하는지 테스트합니다.

    Verifies:
        1. S3에 대해 올바른 put_object_tagging 호출이 이루어지는지
    """
    file_keys = [
        "raw/games/dt=2025-01-01/batch-0.jsonl",
        "raw/games/dt=2025-01-01/batch-1.jsonl",
    ]

    await mark_old_files_as_outdated(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        file_keys=file_keys,
    )

    assert mock_s3_client.put_object_tagging.call_count == 2

    for call_args in mock_s3_client.put_object_tagging.call_args_list:
        assert call_args.kwargs["Tagging"]["TagSet"][0]["Value"] == "outdated"


@pytest.mark.asyncio
async def test_mark_old_files_no_file_keys(
    mock_s3_client: AsyncMock,
):
    """
    빈 파일 키 목록을 전달할 때 태그 변경이 수행되지 않는지 테스트합니다.

    Verifies:
        1. S3의 put_object_tagging 메서드가 호출되지 않는지
    """
    await mark_old_files_as_outdated(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        file_keys=[],
    )

    assert mock_s3_client.put_object_tagging.call_count == 0


@pytest.mark.asyncio
async def test_mark_old_files_tagging_failure(
    mock_s3_client: AsyncMock,
):
    """
    태그 변경 실패 시나리오를 테스트합니다.

    Verifies:
        1. 태그 변경 실패 시에도 나머지 파일들은 정상 처리되는지
    """
    file_keys = [
        "raw/games/dt=2025-01-01/batch-0.jsonl",
        "raw/games/dt=2025-01-01/batch-1.jsonl",
    ]

    # 첫 번째 파일 태그 변경 시 예외 발생
    async def put_object_tagging_side_effect(*args, **kwargs):
        if kwargs["Key"] == "raw/games/dt=2025-01-01/batch-0.jsonl":
            raise Exception("S3 Error")
        return None

    mock_s3_client.put_object_tagging.side_effect = put_object_tagging_side_effect

    await mark_old_files_as_outdated(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        file_keys=file_keys,
    )

    assert mock_s3_client.put_object_tagging.call_count == 2  # 두 파일 모두 시도


@pytest.mark.asyncio
async def test_tag_files_as_final(
    mock_s3_client: AsyncMock,
):
    """
    새로 업로드된 파일들을 status=final로 태그하는지 테스트합니다.

    Verifies:
        1. S3에 대해 올바른 put_object_tagging 호출이 이루어지는지
    """
    new_files = [
        "raw/games/file1.jsonl",
        "raw/games/file2.jsonl",
    ]

    await tag_files_as_final(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        entity_name="games",
        file_keys=new_files,
    )

    assert mock_s3_client.put_object_tagging.call_count == 2

    expected_calls = [
        call(
            Bucket="test-bucket",
            Key="raw/games/file1.jsonl",
            Tagging={"TagSet": [{"Key": "status", "Value": "final"}]},
        ),
        call(
            Bucket="test-bucket",
            Key="raw/games/file2.jsonl",
            Tagging={"TagSet": [{"Key": "status", "Value": "final"}]},
        ),
    ]
    mock_s3_client.put_object_tagging.assert_has_calls(expected_calls, any_order=True)


@pytest.mark.asyncio
async def test_tag_files_as_final_tagging_failure(
    mock_s3_client: AsyncMock,
):
    """
    새 파일 태그 변경 실패 시나리오를 테스트합니다.

    Verifies:
        1. 태그 변경 실패 시에도 나머지 파일들은 정상 처리되는지
    """
    new_files = [
        "raw/games/file1.jsonl",
        "raw/games/file2.jsonl",
    ]

    # 첫 번째 파일 태그 변경 시 예외 발생
    async def put_object_tagging_side_effect(*args, **kwargs):
        if kwargs["Key"] == "raw/games/file1.jsonl":
            raise Exception
        return None

    mock_s3_client.put_object_tagging.side_effect = put_object_tagging_side_effect

    await tag_files_as_final(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        entity_name="games",
        file_keys=new_files,
    )

    assert mock_s3_client.put_object_tagging.call_count == 2  # 두 파일 모두 시도


@pytest.mark.asyncio
async def test_invalidate_cloudfront_cache(
    mock_cloudfront_client: AsyncMock,
):
    """
    CloudFront 캐시 무효화가 올바르게 수행되는지 테스트합니다.

    Verifies:
        1. S3 클라이언트의 create_invalidation 메서드가 올바르게 호출되는지
    """
    distribution_id = "TEST_DISTRIBUTION_ID"

    await invalidate_cloudfront_cache(
        cloudfront_client=mock_cloudfront_client,
        cloudfront_distribution_id=distribution_id,
        dt_partition="2025-01-01",
    )

    mock_cloudfront_client.create_invalidation.assert_called_once()
    _, kwargs = mock_cloudfront_client.create_invalidation.call_args

    assert kwargs["DistributionId"] == distribution_id
    invalidation_paths = kwargs["InvalidationBatch"]["Paths"]["Items"]
    assert any(
        "/raw/games/dt=2025-01-01/_manifest.json" in path for path in invalidation_paths
    )


@pytest.mark.asyncio
async def test_invalidate_cloudfront_cache_no_distribution_id(
    mock_cloudfront_client: AsyncMock,
):
    """
    CloudFront 배포 ID가 없을 때 캐시 무효화가 수행되지 않는지 테스트합니다.

    Verifies:
        1. S3 클라이언트의 create_invalidation 메서드가 호출되지 않는지
    """
    await invalidate_cloudfront_cache(
        cloudfront_client=mock_cloudfront_client,
        cloudfront_distribution_id=None,
        dt_partition="2025-01-01",
    )

    mock_cloudfront_client.create_invalidation.assert_not_called()


@pytest.mark.asyncio
async def test_invalidate_cloudfront_cache_failure(
    mock_cloudfront_client: AsyncMock,
):
    """
    CloudFront 캐시 무효화 실패 시나리오를 테스트합니다.

    Verifies:
        1. 예외가 발생해도 함수가 정상 종료되는지
    """
    mock_cloudfront_client.create_invalidation.side_effect = Exception(
        "CloudFront Error"
    )

    await invalidate_cloudfront_cache(
        cloudfront_client=mock_cloudfront_client,
        cloudfront_distribution_id="TEST_DISTRIBUTION_ID",
        dt_partition="2025-01-01",
    )

    mock_cloudfront_client.create_invalidation.assert_called_once()


@pytest.mark.asyncio
async def test_delete_files_in_partition(
    mock_s3_client: AsyncMock,
):
    """
    지정된 파티션의 모든 파일을 삭제하는지 테스트합니다.

    Verifies:
        1. paginator를 통해 파일 목록을 조회하는지
        2. S3 클라이언트의 delete_objects 메서드가 올바르게 호출되는지
        3. 삭제된 파일 수가 올바르게 반환되는지
    """
    page_data = {
        "Contents": [
            {"Key": "raw/popscore/dt=2025-01-15/batch-0.jsonl"},
            {"Key": "raw/popscore/dt=2025-01-15/batch-1.jsonl"},
        ]
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    deleted_count = await delete_files_in_partition(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        prefix="raw/popscore/dt=2025-01-15/",
    )

    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")

    mock_s3_client.delete_objects.assert_awaited_once()
    delete_call_args = mock_s3_client.delete_objects.call_args.kwargs
    deleted_keys = [obj["Key"] for obj in delete_call_args["Delete"]["Objects"]]
    assert deleted_keys == [
        "raw/popscore/dt=2025-01-15/batch-0.jsonl",
        "raw/popscore/dt=2025-01-15/batch-1.jsonl",
    ]

    assert deleted_count == 2


@pytest.mark.asyncio
async def test_delete_files_in_partition_failure(
    mock_s3_client: AsyncMock,
):
    """
    S3 파일 삭제 실패 시나리오를 테스트합니다.

    Verifies:
        1. 예외가 발생해도 함수가 정상 종료되는지
    """
    page_data = {
        "Contents": [
            {"Key": "raw/popscore/dt=2025-01-15/batch-0.jsonl"},
            {"Key": "raw/popscore/dt=2025-01-15/batch-1.jsonl"},
        ]
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    # delete_objects에서 예외 발생
    async def delete_objects_side_effect(*args, **kwargs):
        raise Exception("S3 Delete Error")

    mock_s3_client.delete_objects.side_effect = delete_objects_side_effect

    # 예외가 발생해야 함
    with pytest.raises(Exception, match="S3 Delete Error"):
        await delete_files_in_partition(
            s3_client=mock_s3_client,
            bucket_name="test-bucket",
            prefix="raw/popscore/dt=2025-01-15/",
        )

    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_s3_client.delete_objects.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_files_in_partition_no_files(
    mock_s3_client: AsyncMock,
):
    """
    지정된 파티션에 파일이 없을 때 삭제가 수행되지 않는지 테스트합니다.

    Verifies:
        1. S3 클라이언트의 delete_objects 메서드가 호출되지 않는지
        2. 삭제된 파일 수가 0으로 반환되는지
    """

    async def async_paginate(*args, **kwargs):
        # 빈 페이지 반환
        if False:
            yield

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    deleted_count = await delete_files_in_partition(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        prefix="raw/popscore/dt=2025-01-15/",
    )

    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_s3_client.delete_objects.assert_not_awaited()

    assert deleted_count == 0


@pytest.mark.asyncio
async def test_move_files_atomically(
    mock_s3_client: AsyncMock,
):
    """
    S3 파일들을 원자적으로 이동하는지 테스트합니다.

    Verifies:
        1. paginator를 통해 파일 목록을 조회하는지
        2. S3 클라이언트의 copy_object 및 delete_object 메서드가 올바르게 호출되는지
        3. 이동된 파일 수가 올바르게 반환되는지
    """
    source_prefix = "raw/popscore/dt=2025-01-15/_temp_123/"
    dest_prefix = "raw/popscore/dt=2025-01-15/"

    # paginator 모킹
    page_data = {
        "Contents": [
            {"Key": f"{source_prefix}batch-0.jsonl"},
            {"Key": f"{source_prefix}batch-1.jsonl"},
        ]
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    moved_count = await move_files_atomically(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        source_prefix=source_prefix,
        dest_prefix=dest_prefix,
    )

    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")

    expected_copy_calls = [
        call(
            Bucket="test-bucket",
            CopySource={
                "Bucket": "test-bucket",
                "Key": f"{source_prefix}batch-0.jsonl",
            },
            Key=f"{dest_prefix}batch-0.jsonl",
            TaggingDirective="COPY",
        ),
        call(
            Bucket="test-bucket",
            CopySource={
                "Bucket": "test-bucket",
                "Key": f"{source_prefix}batch-1.jsonl",
            },
            Key=f"{dest_prefix}batch-1.jsonl",
            TaggingDirective="COPY",
        ),
    ]
    mock_s3_client.copy_object.assert_has_calls(expected_copy_calls, any_order=True)

    expected_delete_calls = [
        call(Bucket="test-bucket", Key=f"{source_prefix}batch-0.jsonl"),
        call(Bucket="test-bucket", Key=f"{source_prefix}batch-1.jsonl"),
    ]
    mock_s3_client.delete_object.assert_has_calls(expected_delete_calls, any_order=True)

    assert moved_count == 2


@pytest.mark.asyncio
async def test_move_files_atomically_failure(
    mock_s3_client: AsyncMock,
):
    """
    S3 파일 이동 실패 시나리오를 테스트합니다.

    Verifies:
        1. 예외가 발생해도 함수가 정상 종료되는지
    """
    source_prefix = "raw/popscore/dt=2025-01-15/_temp_123/"
    dest_prefix = "raw/popscore/dt=2025-01-15/"

    # paginator 모킹
    page_data = {
        "Contents": [
            {"Key": f"{source_prefix}batch-0.jsonl"},
            {"Key": f"{source_prefix}batch-1.jsonl"},
        ]
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    # copy_object에서 예외 발생
    async def copy_object_side_effect(*args, **kwargs):
        raise Exception("S3 Copy Error")

    mock_s3_client.copy_object.side_effect = copy_object_side_effect

    # 예외가 발생해야 함
    with pytest.raises(Exception, match="S3 Copy Error"):
        await move_files_atomically(
            s3_client=mock_s3_client,
            bucket_name="test-bucket",
            source_prefix=source_prefix,
            dest_prefix=dest_prefix,
        )

    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_s3_client.copy_object.assert_called_once()  # 첫 번째 호출에서 예외 발생
    mock_s3_client.delete_object.assert_not_called()  # 삭제는 수행되지 않음
