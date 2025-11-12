import json
from typing import Any

from src.pipeline.interfaces import Loader


class S3Loader(Loader):
    """S3에 데이터를 로드하는 Loader 구현체."""

    def __init__(self, client: Any, bucket_name: str) -> None:
        """
        Args:
            client: 비동기 S3 클라이언트 (aioboto3.client 등)
            bucket_name: 데이터를 적재할 S3 버킷 이름
        """
        self._s3_client = client
        self._bucket_name = bucket_name

    async def load(self, data: list[dict[str, Any]], key: str) -> None:
        """
        데이터를 S3 버킷에 적재합니다.

        Args:
            data (list[dict[str, Any]]): Extractor가 생성한 데이터 배치.
            key (str): S3 등 데이터가 적재될 위치를 나타내는 키.
        """
        if not data:
            return

        jsonl_data = "\n".join(json.dumps(item) for item in data)
        await self._s3_client.put_object(
            Bucket=self._bucket_name,
            Key=key,
            Body=jsonl_data,
            ContentType="application/x-jsonlines",
            Tagging="status=temp",
        )
