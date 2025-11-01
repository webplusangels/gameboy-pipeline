from collections.abc import AsyncGenerator
from typing import Any

from src.pipeline.interfaces import Extractor


class IgdbExtractor(Extractor):
    """IGDB API로부터 게임 데이터를 추출하는 Extractor 구현체."""

    def __init__(self, client: Any, api_key: str) -> None:
        self._client = client
        self._api_key = api_key

    async def extract(self) -> AsyncGenerator[dict[str, Any], None]:
        response = await self._client.post(url="...", data="...")
        response.raise_for_status()
        response_data = response.json()

        for item in response_data:
            yield item
