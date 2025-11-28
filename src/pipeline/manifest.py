import json
from datetime import datetime
from typing import Any, TypedDict

from loguru import logger

from src.pipeline.utils import get_s3_path


class Manifest(TypedDict, total=False):
    files: list[str]
    total_count: int
    created_at: str
    updated_at: str
    batch_count: int


async def update_manifest(
    s3_client: Any,
    bucket_name: str,
    entity_name: str,
    dt_partition: str,
    new_files: list[str],
    new_count: int,
    extraction_start: datetime,
    full_refresh: bool,
) -> None:
    """
    S3에 저장된 manifest 파일을 업데이트합니다.

    Args:
        s3_client (Any): S3 클라이언트 객체.
        bucket_name (str): S3 버킷 이름.
        entity_name (str): 엔티티 이름.
        dt_partition (str): 날짜 파티션 문자열(예: '2023-10-01').
        new_files (list[str]): 새로 추가된 파일 목록.
        new_count (int): 새로 추가된 데이터 항목 수.
        extraction_start (datetime): 추출 시작 시간.
        full_refresh (bool): 전체 갱신 여부.
    """
    s3_prefix = get_s3_path(entity_name, dt_partition)
    manifest_key = f"{s3_prefix}/_manifest.json"

    if full_refresh:
        logger.info(
            f"Full Refresh 모드: 기존 매니페스트 파일을 초기화합니다: {manifest_key}"
        )
        manifest_data: Manifest = {
            "files": new_files,
            "total_count": new_count,
            "created_at": extraction_start.isoformat(),
            "updated_at": extraction_start.isoformat(),
            "batch_count": len(new_files),
        }
    else:
        # 기존 매니페스트 읽기 시도
        try:
            resp = await s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
            content = await resp["Body"].read()
            manifest_data = json.loads(content.decode("utf-8"))
            logger.info(f"기존 매니페스트 파일 로드 완료: {manifest_key}")

        except s3_client.exceptions.NoSuchKey:
            logger.info(f"기존 매니페스트 파일이 없으므로 생성합니다: {manifest_key}")
            manifest_data = {
                "files": [],
                "total_count": 0,
                "created_at": extraction_start.isoformat(),
            }

        manifest_data["files"].extend(new_files)
        manifest_data["total_count"] += new_count
        manifest_data["updated_at"] = extraction_start.isoformat()
        manifest_data["batch_count"] = len(manifest_data["files"])

    await s3_client.put_object(
        Bucket=bucket_name,
        Key=manifest_key,
        Body=json.dumps(manifest_data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(
        f"매니페스트 파일 {'교체' if full_refresh else '업데이트'} 완료: {manifest_key} (총 {len(manifest_data['files'])}개 파일)"
    )
