import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import aioboto3
import httpx
from loguru import logger

from src.config import settings
from src.pipeline.constants import DIMENSION_ENTITIES


@asynccontextmanager
async def create_clients() -> AsyncGenerator[tuple[httpx.AsyncClient, Any, Any], None]:
    """
    EL 파이프라인에 필요한 비동기 클라이언트를 생성하고 세션을 관리합니다.
    Yields:
        tuple[httpx.AsyncClient, Any, Any]: HTTP 클라이언트와 기타 필요한 클라이언트 객체
    """
    logger.info("HTTPX AsyncClient 세션 생성...")
    region = settings.aws_default_region
    session = aioboto3.Session(region_name=region)
    timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

    async with (
        httpx.AsyncClient(timeout=timeout, http2=True) as http_client,
        session.client("s3", region_name=region) as s3_client,
        session.client("cloudfront", region_name=region) as cloudfront_client,
    ):
        try:
            yield http_client, s3_client, cloudfront_client
        finally:
            logger.info("클라이언트 세션 종료...")


async def list_files_with_tag(
    s3_client: Any,
    bucket_name: str,
    prefix: str,
    tag_key: str,
    tag_value: str,
) -> list[str]:
    """
    지정된 태그를 가진 S3 파일들의 키 목록을 반환합니다.

    Args:
        s3_client (Any): S3 클라이언트 객체.
        bucket_name (str): S3 버킷 이름.
        prefix (str): 파일 키의 접두사.
        tag_key (str): 검색할 태그 키.
        tag_value (str): 검색할 태그 값.

    Returns:
        list[str]: 지정된 태그를 가진 파일들의 S3 키 목록.
    """
    matching_files: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")

    async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]

            try:
                response = await s3_client.get_object_tagging(
                    Bucket=bucket_name,
                    Key=key,
                )
                tags = response.get("TagSet", [])

                for tag in tags:
                    if tag["Key"] == tag_key and tag["Value"] == tag_value:
                        matching_files.append(key)
                        break

            except Exception as e:
                logger.error(
                    f"파일 태그 조회 실패: s3://{bucket_name}/{key} - 오류: {e}"
                )
                continue

    return matching_files


async def mark_old_files_as_outdated(
    s3_client: Any,
    bucket_name: str,
    file_keys: list[str],
) -> None:
    """
    Full Refresh 후 기존 파일들의 태그를 'status=outdated'로 업데이트합니다.

    Args:
        s3_client (Any): S3 클라이언트 객체.
        bucket_name (str): S3 버킷 이름.
        file_keys (list[str]): 태그를 업데이트할 파일들의 S3 키 접두사 목록.
    """
    if not file_keys:
        logger.info("태그를 업데이트할 파일 키가 없습니다. 작업을 건너뜁니다.")
        return

    logger.info(f"기존 파일 {len(file_keys)}개를 'outdated'로 태그 변경 시작...")

    tagged_count = 0
    failed_files: list[str] = []

    for key in file_keys:
        try:
            await s3_client.put_object_tagging(
                Bucket=bucket_name,
                Key=key,
                Tagging={"TagSet": [{"Key": "status", "Value": "outdated"}]},
            )
            tagged_count += 1
        except Exception as e:
            logger.error(
                f"파일 태그 업데이트 실패: s3://{bucket_name}/{key} - 오류: {e}"
            )
            failed_files.append(key)

    if failed_files:
        logger.warning(f"태그 업데이트에 실패한 파일들: {len(failed_files)}개")

    logger.info(
        f"기존 파일들을 'outdated'로 태그 변경 완료. 총 {tagged_count}개 파일이 업데이트되었습니다."
    )


async def tag_files_as_final(
    s3_client: Any,
    bucket_name: str,
    entity_name: str,
    file_keys: list[str],
) -> None:
    """
    지정된 파일들의 태그를 'status=final'로 설정합니다.

    Args:
        s3_client (Any): S3 클라이언트 객체.
        bucket_name (str): S3 버킷 이름.
        entity_name (str): 엔티티 이름.
        file_keys (list[str]): 태그를 업데이트할 파일들의 S3 키 목록.
    """
    logger.info(f"'{entity_name}' 엔티티의 새 파일들을 'final'로 태그 변경 시작...")

    tagged_count = 0

    for key in file_keys:
        try:
            await s3_client.put_object_tagging(
                Bucket=bucket_name,
                Key=key,
                Tagging={"TagSet": [{"Key": "status", "Value": "final"}]},
            )
            tagged_count += 1

        except Exception as e:
            logger.error(
                f"파일 태그 업데이트 실패: s3://{bucket_name}/{key} - 오류: {e}"
            )
            continue

    logger.info(
        f"'{entity_name}' 엔티티의 새 파일들을 'final'로 태그 변경 완료. 총 {tagged_count}개 파일이 업데이트되었습니다."
    )


async def invalidate_cloudfront_cache(
    cloudfront_client: Any,
    cloudfront_distribution_id: str | None,
    dt_partition: str,
) -> None:
    """
    CloudFront 캐시를 무효화합니다.

    Args:
        cloudfront_client (Any): CloudFront 클라이언트 객체.
        cloudfront_distribution_id (str | None): CloudFront 배포 ID.
        dt_partition (str): 날짜 파티션 문자열.
    Returns:
        None
    """
    if cloudfront_distribution_id:
        logger.info("CloudFront 캐시 무효화 시작...")
        try:
            fact_manifest_path = f"/raw/games/dt={dt_partition}/_manifest.json"
            dim_manifest_path = [
                f"/raw/dimensions/{entity}/_manifest.json"
                for entity in DIMENSION_ENTITIES
            ]
            invalidation_path = [fact_manifest_path] + dim_manifest_path

            await cloudfront_client.create_invalidation(
                DistributionId=cloudfront_distribution_id,
                InvalidationBatch={
                    "Paths": {
                        "Quantity": len(invalidation_path),
                        "Items": invalidation_path,
                    },
                    "CallerReference": str(uuid.uuid4()),
                },
            )
            logger.success(
                f"CloudFront 캐시 무효화 요청 완료: {len(invalidation_path)}개 경로"
            )
        except Exception as e:
            logger.error(f"CloudFront 캐시 무효화 중 오류 발생: {e}")
    else:
        logger.warning(
            "CloudFront Distribution ID가 설정되지 않아 캐시 무효화를 건너뜁니다."
        )


async def delete_files_in_partition(
    s3_client: Any,
    bucket_name: str,
    prefix: str,
) -> int:
    """
    지정된 파티션의 모든 파일을 삭제합니다.
    시계열 데이터의 멱등성을 보장하기 위해 같은 날짜 파티션을 재실행할 때 사용됩니다.

    Args:
        s3_client (Any): S3 클라이언트 객체
        bucket_name (str): S3 버킷 이름
        prefix (str): 삭제할 파일들의 접두사 (예: "raw/popscore/dt=2025-01-15/")

    Returns:
        int: 삭제된 파일 개수
    """
    deleted_count = 0
    files_to_delete: list[str] = []

    paginator = s3_client.get_paginator("list_objects_v2")

    async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            # _manifest.json은 제외 (나중에 업데이트됨)
            # _temp_ 디렉토리도 제외 (atomic replacement 중)
            if not key.endswith("_manifest.json") and "/_temp_" not in key:
                files_to_delete.append(key)

    if not files_to_delete:
        logger.info(f"삭제할 파일이 없습니다: {prefix}")
        return 0

    logger.info(f"파티션 내 파일 {len(files_to_delete)}개 삭제 시작: {prefix}")

    # S3 batch delete (최대 1000개씩)
    for i in range(0, len(files_to_delete), 1000):
        batch = files_to_delete[i : i + 1000]
        try:
            await s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    "Objects": [{"Key": key} for key in batch],
                    "Quiet": True,
                },
            )
            deleted_count += len(batch)
        except Exception as e:
            logger.error(f"파일 삭제 중 오류 발생: {e}")
            raise

    logger.info(f"파티션 내 파일 {deleted_count}개 삭제 완료: {prefix}")
    return deleted_count


async def move_files_atomically(
    s3_client: Any,
    bucket_name: str,
    source_prefix: str,
    dest_prefix: str,
) -> int:
    """
    temp 디렉토리에서 본 디렉토리로 파일들을 이동합니다.

    Args:
        s3_client (Any): S3 클라이언트 객체
        bucket_name (str): S3 버킷 이름
        source_prefix (str): 소스 경로 접두사 (예: "raw/popscore/dt=2025-01-15/_temp_abc/")
        dest_prefix (str): 목적지 경로 접두사 (예: "raw/popscore/dt=2025-01-15/")

    Returns:
        int: 이동된 파일 개수
    """
    import asyncio

    moved_count = 0
    paginator = s3_client.get_paginator("list_objects_v2")

    async for page in paginator.paginate(Bucket=bucket_name, Prefix=source_prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            source_key = obj["Key"]
            # source_prefix를 dest_prefix로 교체
            relative_path = source_key[len(source_prefix) :]
            dest_key = dest_prefix + relative_path

            # S3 eventual consistency를 위한 재시도 로직
            max_retries = 5
            for retry in range(max_retries):
                try:
                    # Copy with tagging
                    # TaggingDirective="COPY"를 명시하여 원본 태그(status=temp) 복사
                    # 이후 tag_files_as_final에서 status=final로 변경됨
                    await s3_client.copy_object(
                        Bucket=bucket_name,
                        CopySource={"Bucket": bucket_name, "Key": source_key},
                        Key=dest_key,
                        TaggingDirective="COPY",
                    )

                    # Delete source
                    await s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=source_key,
                    )

                    moved_count += 1
                    break  # Success, exit retry loop

                except s3_client.exceptions.NoSuchKey:
                    if retry < max_retries - 1:
                        wait_time = 2**retry  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                        logger.warning(
                            f"NoSuchKey for {source_key}, retrying in {wait_time}s... (attempt {retry + 1}/{max_retries})"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(
                            f"파일 이동 실패 (NoSuchKey after {max_retries} retries): {source_key} -> {dest_key}"
                        )
                        raise

                except Exception as e:
                    logger.error(f"파일 이동 실패: {source_key} -> {dest_key}: {e}")
                    raise

    logger.info(f"{moved_count}개 파일을 {source_prefix}에서 {dest_prefix}로 이동 완료")
    return moved_count
