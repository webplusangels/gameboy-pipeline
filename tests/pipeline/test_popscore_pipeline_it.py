import asyncio
import json

import aioboto3
import httpx
import pytest

from src.config import settings
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.extractors import IgdbPopScoreExtractor
from src.pipeline.loaders import S3Loader
from src.pipeline.s3_ops import (
    delete_files_in_partition,
    move_files_atomically,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="function")
async def s3_client():
    """실제 aioboto3 S3 클라이언트 세션을 생성합니다."""
    region = settings.aws_default_region

    session = aioboto3.Session(region_name=region)
    async with session.client("s3", region_name=region) as client:
        yield client


@pytest.mark.asyncio
async def test_popscore_pipeline_e2e_with_temp_directory(s3_client):
    """
    [E2E]
    PopScore 전체 파이프라인을 실제 환경에서 테스트합니다.

    Verifies:
        1. Extractor → BatchProcessor → Loader 전체 흐름
        2. Temp directory를 사용한 atomic replacement
        3. UUID 없는 파일명 사용 (idempotency)
        4. 실제 S3 업로드 및 파일 이동
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id
    bucket_name = settings.s3_bucket_name

    if not token or not client_id or not bucket_name:
        pytest.skip(
            "IGDB_STATIC_TOKEN, IGDB_CLIENT_ID 또는 S3_BUCKET_NAME 환경 변수가 설정되지 않았습니다."
        )

    # 테스트용 날짜 파티션 (실제 운영 데이터와 구분)
    test_date = "2099-12-31"  # 미래 날짜 사용
    temp_suffix = "test_temp"
    final_prefix = f"raw/popscore/dt={test_date}/"
    temp_prefix = f"raw/popscore/dt={test_date}/_temp_{temp_suffix}/"

    try:
        async with httpx.AsyncClient() as client:
            auth_provider = StaticAuthProvider(token=token)
            extractor = IgdbPopScoreExtractor(
                client=client, auth_provider=auth_provider, client_id=client_id
            )
            loader = S3Loader(client=s3_client, bucket_name=bucket_name)

            # === 1. Extract & Load to Temp Directory ===

            test_item_count = 250  # 3 배치 정도 수집
            collected_data = []

            async for item in extractor.extract():
                collected_data.append(item)
                if len(collected_data) >= test_item_count:
                    break

            # 배치 생성 및 temp directory에 업로드 (100개씩)
            batch_size = 100
            batches = []
            for i in range(0, len(collected_data), batch_size):
                batch_data = collected_data[i : i + batch_size]
                batch_num = i // batch_size
                # UUID 없는 파일명 생성 (PopScore는 TIME_SERIES_ENTITIES)
                batch_key = f"{temp_prefix}batch-{batch_num}.jsonl"
                batches.append((batch_data, batch_key))
                print(
                    f"Uploading batch {batch_num} to {batch_key} ({len(batch_data)} items)"
                )
                await loader.load(data=batch_data, key=batch_key)

            print(f"Total batches created: {len(batches)}")
            assert len(batches) == 3  # 100, 100, 50

            # === 2. Verify Temp Files (with retry for eventual consistency) ===
            paginator = s3_client.get_paginator("list_objects_v2")
            temp_files = []

            # Retry logic for S3 eventual consistency (exponential backoff)
            max_retries = 10
            for retry in range(max_retries):
                temp_files = []
                async for page in paginator.paginate(
                    Bucket=bucket_name, Prefix=temp_prefix
                ):
                    if "Contents" in page:
                        temp_files.extend([obj["Key"] for obj in page["Contents"]])

                if len(temp_files) == 3:
                    break

                if retry < max_retries - 1:
                    # Exponential backoff: 1s, 2s, 4s, 8s, ...
                    wait_time = min(2**retry, 8)  # Cap at 8 seconds
                    print(
                        f"Retry {retry + 1}/{max_retries}: Temp files found: {len(temp_files)}, waiting {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)

            print(f"Temp files found: {len(temp_files)}")
            for f in temp_files:
                print(f"  - {f}")

            assert len(temp_files) == 3
            # UUID 없는 파일명 확인
            for file_key in temp_files:
                assert "batch-" in file_key
                assert file_key.endswith(".jsonl")

            # === 3. Delete Old Files (if any) ===
            _ = await delete_files_in_partition(
                s3_client=s3_client,
                bucket_name=bucket_name,
                prefix=final_prefix,
            )

            # === 4. Move Files Atomically ===
            moved_count = await move_files_atomically(
                s3_client=s3_client,
                bucket_name=bucket_name,
                source_prefix=temp_prefix,
                dest_prefix=final_prefix,
            )

            assert moved_count == 3

            # === 5. Verify Final Files ===
            final_files = []
            async for page in paginator.paginate(
                Bucket=bucket_name, Prefix=final_prefix
            ):
                if "Contents" in page:
                    final_files.extend([obj["Key"] for obj in page["Contents"]])

            # Temp directory는 제외하고 최종 파일만 카운트
            final_files = [f for f in final_files if "/_temp_" not in f]
            assert len(final_files) == 3

            # === 6. Verify Data Integrity ===
            total_items = 0
            for file_key in final_files:
                response = await s3_client.get_object(Bucket=bucket_name, Key=file_key)
                body_bytes = await response["Body"].read()
                body_str = body_bytes.decode("utf-8")

                lines = body_str.strip().split("\n")
                for line in lines:
                    item = json.loads(line)
                    assert "id" in item
                    assert "game_id" in item
                    assert "popularity_type" in item
                    assert "value" in item
                    total_items += 1

            assert total_items == test_item_count

    finally:
        # 테스트 후 정리: 테스트 파일 삭제
        await delete_files_in_partition(
            s3_client=s3_client,
            bucket_name=bucket_name,
            prefix=final_prefix,
        )


@pytest.mark.asyncio
async def test_popscore_idempotency_with_same_date_rerun(s3_client):
    """
    [E2E]
    PopScore 파이프라인의 동일 날짜 재실행 시 idempotency를 테스트합니다.

    Verifies:
        1. 같은 날짜로 두 번 실행 시 동일한 결과
        2. UUID 없는 파일명으로 덮어쓰기
        3. Temp directory를 통한 atomic replacement
    """
    token = settings.igdb_static_token
    client_id = settings.igdb_client_id
    bucket_name = settings.s3_bucket_name

    if not token or not client_id or not bucket_name:
        pytest.skip(
            "IGDB_STATIC_TOKEN, IGDB_CLIENT_ID 또는 S3_BUCKET_NAME 환경 변수가 설정되지 않았습니다."
        )

    # 테스트용 날짜 파티션
    test_date = "2099-12-30"
    temp_suffix_1 = "test_run1"
    temp_suffix_2 = "test_run2"
    final_prefix = f"raw/popscore/dt={test_date}/"

    try:
        async with httpx.AsyncClient() as client:
            auth_provider = StaticAuthProvider(token=token)
            extractor = IgdbPopScoreExtractor(
                client=client, auth_provider=auth_provider, client_id=client_id
            )
            loader = S3Loader(client=s3_client, bucket_name=bucket_name)

            test_item_count = 100
            batch_size = 50

            # === First Run ===
            temp_prefix_1 = f"raw/popscore/dt={test_date}/_temp_{temp_suffix_1}/"

            collected_data = []
            async for item in extractor.extract():
                collected_data.append(item)
                if len(collected_data) >= test_item_count:
                    break

            # 배치 생성 및 업로드
            batches = []
            for i in range(0, len(collected_data), batch_size):
                batch_data = collected_data[i : i + batch_size]
                batch_num = i // batch_size
                batch_key = f"{temp_prefix_1}batch-{batch_num}.jsonl"
                batches.append(batch_key)
                await loader.load(data=batch_data, key=batch_key)

            # Wait for S3 eventual consistency (increased for cross-version stability)
            await asyncio.sleep(3)

            await delete_files_in_partition(
                s3_client=s3_client,
                bucket_name=bucket_name,
                prefix=final_prefix,
            )

            moved_count_1 = await move_files_atomically(
                s3_client=s3_client,
                bucket_name=bucket_name,
                source_prefix=temp_prefix_1,
                dest_prefix=final_prefix,
            )

            # 첫 번째 실행 후 파일 목록 저장
            paginator = s3_client.get_paginator("list_objects_v2")
            first_run_files = []
            async for page in paginator.paginate(
                Bucket=bucket_name, Prefix=final_prefix
            ):
                if "Contents" in page:
                    first_run_files.extend(
                        [
                            obj["Key"]
                            for obj in page["Contents"]
                            if "/_temp_" not in obj["Key"]
                        ]
                    )

            first_run_files.sort()

            # === Second Run (Same Date) ===
            temp_prefix_2 = f"raw/popscore/dt={test_date}/_temp_{temp_suffix_2}/"

            # 배치 생성 및 업로드
            batches_2 = []
            for i in range(0, len(collected_data), batch_size):
                batch_data = collected_data[i : i + batch_size]
                batch_num = i // batch_size
                batch_key = f"{temp_prefix_2}batch-{batch_num}.jsonl"
                batches_2.append(batch_key)
                await loader.load(data=batch_data, key=batch_key)

            # Wait for S3 eventual consistency (increased for cross-version stability)
            await asyncio.sleep(3)

            await delete_files_in_partition(
                s3_client=s3_client,
                bucket_name=bucket_name,
                prefix=final_prefix,
            )

            moved_count_2 = await move_files_atomically(
                s3_client=s3_client,
                bucket_name=bucket_name,
                source_prefix=temp_prefix_2,
                dest_prefix=final_prefix,
            )

            # 두 번째 실행 후 파일 목록 저장
            second_run_files = []
            async for page in paginator.paginate(
                Bucket=bucket_name, Prefix=final_prefix
            ):
                if "Contents" in page:
                    second_run_files.extend(
                        [
                            obj["Key"]
                            for obj in page["Contents"]
                            if "/_temp_" not in obj["Key"]
                        ]
                    )

            second_run_files.sort()

            # === Verify Idempotency ===
            # 같은 개수의 파일
            assert moved_count_1 == moved_count_2
            assert len(first_run_files) == len(second_run_files)

            # 같은 파일명 (UUID 없으므로 동일해야 함)
            assert first_run_files == second_run_files

            # 각 파일명이 UUID를 포함하지 않는지 확인
            for file_key in second_run_files:
                # batch-0.jsonl, batch-1.jsonl 형식
                filename = file_key.split("/")[-1]
                assert filename.startswith("batch-")
                assert filename.endswith(".jsonl")
                # UUID는 포함하지 않음 (36자 길이의 UUID 패턴이 없어야 함)
                assert len(filename) < 20  # "batch-X.jsonl" 형식

    finally:
        # 테스트 후 정리
        await delete_files_in_partition(
            s3_client=s3_client,
            bucket_name=bucket_name,
            prefix=final_prefix,
        )
