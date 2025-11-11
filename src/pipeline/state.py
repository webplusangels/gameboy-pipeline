import json
from datetime import datetime, timezone
from typing import Any

from botocore.exceptions import ClientError
from loguru import logger

from src.pipeline.interfaces import StateManager


class S3StateManager(StateManager):
    """
    S3 기반 StateManager 구현체.
    
    파이프라인의 실행 상태를 엔티티별 개별 S3 JSON 파일로 관리합니다.
    증분 업데이트를 지원하며, 엔티티별 병렬 실행 시 경합 조건을 방지합니다.
    
    State 파일 구조:
    - S3 Key: {state_prefix}{entity}.json
    - 예: pipeline/state/games.json, pipeline/state/platforms.json
    
    파일 형식 예시 (pipeline/state/games.json):
    {
        "last_run_time": "2025-11-11T10:00:00+00:00",
        "records_processed": 342000
    }
    """

    def __init__(
        self,
        client: Any,
        bucket_name: str,
        state_prefix: str = "pipeline/state/",
    ) -> None:
        """
        Args:
            client: aioboto3 S3 클라이언트
            bucket_name: S3 버킷 이름
            state_prefix: 상태 파일의 S3 접두사 (기본: "pipeline/state/")
        """
        self._client = client
        self._bucket_name = bucket_name
        self._state_prefix = state_prefix

    async def get_last_run_time(self, entity: str) -> datetime | None:
        """
        지정된 엔티티의 마지막 성공 실행 시간을 S3에서 조회합니다.

        Args:
            entity: 엔티티 이름 (예: "games", "platforms")

        Returns:
            마지막 실행 시간(UTC) 또는 첫 실행이면 None.
        """
        state_key = f"{self._state_prefix}{entity}.json"
        
        try:
            response = await self._client.get_object(
                Bucket=self._bucket_name, Key=state_key
            )
            body = await response["Body"].read()
            state = json.loads(body)

            timestamp_str = state.get("last_run_time")
            if timestamp_str:
                last_run = datetime.fromisoformat(timestamp_str)
                logger.debug(
                    f"엔티티 '{entity}' 마지막 실행 시간: {last_run.isoformat()}"
                )
                return last_run
            else:
                logger.warning(
                    f"엔티티 '{entity}' 상태 파일에 'last_run_time' 키 없음. 전체 로드 실행."
                )
                return None

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info(
                    f"엔티티 '{entity}' 상태 파일 없음 ({state_key}). 전체 로드 실행."
                )
                return None
            else:
                logger.error(f"S3 상태 조회 중 오류 발생: {e}")
                raise

        except Exception as e:
            logger.error(f"상태 조회 중 예상치 못한 오류: {e}")
            # 안전하게 전체 로드로 폴백
            return None

    async def save_last_run_time(self, entity: str, run_time: datetime) -> None:
        """
        지정된 엔티티의 마지막 성공 실행 시간을 S3에 저장합니다.

        Args:
            entity: 엔티티 이름 (예: "games", "platforms")
            run_time: 저장할 실행 시간 (UTC, timezone-aware 권장)

        Raises:
            Exception: S3 저장 실패 시 예외 발생
        """
        state_key = f"{self._state_prefix}{entity}.json"
        
        try:
            state = {}
            try:
                response = await self._client.get_object(
                    Bucket=self._bucket_name, Key=state_key
                )
                body = await response["Body"].read()
                state = json.loads(body)
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    # 상태 파일 없으면 새로 생성
                    logger.info(f"엔티티 '{entity}' 새 상태 파일 생성")
                else:
                    raise

            if run_time.tzinfo is None:
                logger.warning(
                    f"run_time이 timezone-naive입니다. UTC로 간주합니다: {run_time}"
                )
                run_time = run_time.replace(tzinfo=timezone.utc)

            state["last_run_time"] = run_time.isoformat()
            state["updated_at"] = datetime.now(timezone.utc).isoformat()

            await self._client.put_object(
                Bucket=self._bucket_name,
                Key=state_key,
                Body=json.dumps(state, indent=2, ensure_ascii=False),
                ContentType="application/json",
            )

            logger.success(
                f"엔티티 '{entity}' 상태 저장 완료: {run_time.isoformat()}"
            )

        except Exception as e:
            logger.error(f"엔티티 '{entity}' 상태 저장 실패: {e}")
            raise

    async def reset_state(self, entity: str) -> None:
        """
        지정된 엔티티의 상태 파일을 S3에서 삭제하여 상태를 초기화합니다.

        Args:
            entity: 엔티티 이름 (예: "games", "platforms")

        Raises:
            Exception: S3 삭제 실패 시 예외 발생
        """
        state_key = f"{self._state_prefix}{entity}.json"
        
        try:
            await self._client.delete_object(
                Bucket=self._bucket_name,
                Key=state_key,
            )
            logger.info(f"엔티티 '{entity}' 상태 파일 삭제 완료: {state_key}")

        except Exception as e:
            logger.error(f"엔티티 '{entity}' 상태 파일 삭제 실패: {e}")
            raise
    
    async def list_states(self) -> dict[str, datetime | None]:
        """
        S3에서 모든 엔티티의 상태 파일을 나열하고 각 엔티티의 마지막 실행 시간을 반환합니다.

        Returns:
            dict[str, datetime | None]: 엔티티 이름을 키로, 마지막 실행 시간을 값으로 하는 딕셔너리.
        """
        states = {}
        paginator = self._client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=self._bucket_name,
            Prefix=self._state_prefix,
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                entity = key.replace(self._state_prefix, "").replace(".json", "")
                last_run = await self.get_last_run_time(entity)
                states[entity] = last_run
        return states