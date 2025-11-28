from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.pipeline.s3_ops import (
    create_clients,
    invalidate_cloudfront_cache,
    mark_old_files_as_outdated,
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
async def test_mark_old_files_tags_final_files_as_outdated(
    mock_s3_client: AsyncMock,
):
    """
    status=final인 파일들을 status=outdated로 태그 변경하는지 테스트합니다.

    Verifies:
        1. S3에서 파일 목록을 올바르게 조회하는지
        2. status=final 태그가 있는 파일들이 status=outdated로 변경되는지
        3. manifest 파일과 폴더는 무시되는지
    """
    page_data = {
        "Contents": [
            {"Key": "raw/games/file1.jsonl"},
            {"Key": "raw/games/file2.jsonl"},
            {"Key": "raw/games/_manifest.json"},  # 매니페스트 파일 (무시 대상)
            {"Key": "raw/games/"},  # 폴더 (무시 대상)
        ]
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()
    mock_s3_client.get_object_tagging.return_value = {
        "TagSet": [{"Key": "status", "Value": "final"}]
    }

    await mark_old_files_as_outdated(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        entity_name="games",
    )

    assert mock_s3_client.get_paginator.called
    assert mock_s3_client.put_object_tagging.call_count == 2  # final 파일 2개 변경


@pytest.mark.asyncio
async def test_mark_old_files_dimension_entity_no_contents(
    mock_s3_client: AsyncMock,
):
    """
    Dimension 엔티티에서 Contents가 없는 페이지를 처리하는지 테스트합니다.

    Verifies:
        1. S3에서 파일 목록을 올바르게 조회하는지
        2. Contents가 없는 경우에도 에러 없이 처리되는지
    """
    page_data = {}  # Contents 없음

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.return_value = async_paginate()

    await mark_old_files_as_outdated(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        entity_name="genres",
    )

    assert mock_s3_client.get_paginator.called
    assert mock_s3_client.put_object_tagging.call_count == 0  # 변경된 파일 없음


@pytest.mark.asyncio
async def test_mark_old_files_tagging_failure(
    mock_s3_client: AsyncMock,
):
    """
    태그 변경 실패 시나리오를 테스트합니다.

    Verifies:
        1. S3에서 파일 목록을 올바르게 조회하는지
        2. 태그 변경 실패 시에도 나머지 파일들은 정상 처리되는지
    """
    page_data = {
        "Contents": [
            {"Key": "raw/games/file1.jsonl"},
            {"Key": "raw/games/file2.jsonl"},
        ]
    }

    async def async_paginate(*args, **kwargs):
        yield page_data

    paginator = mock_s3_client.get_paginator.return_value
    paginator.paginate.side_effect = async_paginate
    mock_s3_client.get_object_tagging.return_value = {
        "TagSet": [{"Key": "status", "Value": "final"}]
    }

    # 첫 번째 파일 태그 변경 시 예외 발생
    async def put_object_tagging_side_effect(*args, **kwargs):
        if kwargs["Key"] == "raw/games/file1.jsonl":
            raise Exception
        return None

    mock_s3_client.put_object_tagging.side_effect = put_object_tagging_side_effect

    await mark_old_files_as_outdated(
        s3_client=mock_s3_client,
        bucket_name="test-bucket",
        entity_name="games",
    )

    assert mock_s3_client.get_paginator.called
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
