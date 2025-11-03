from collections.abc import AsyncGenerator
from typing import Any

from src.pipeline.interfaces import Extractor


class IgdbExtractor(Extractor):
    """IGDB API로부터 게임 데이터를 추출하는 Extractor 구현체."""

    def __init__(
        self,
        client: Any,
        auth_provider: Any,
        client_id: str,
    ) -> None:
        """
        Args:
            client: HTTP 클라이언트 (httpx.AsyncClient 등)
            auth_provider: 인증 토큰을 제공하는 AuthProvider
            client_id: 클라이언트 ID
        """
        self._client = client
        self._auth_provider = auth_provider
        self._client_id = client_id
        self._API_URL = "https://api.igdb.com/v4/games"
        self._BASE_QUERY = "fields *;"
        self._LIMIT = 500

    async def extract(self) -> AsyncGenerator[dict[str, Any], None]:
        """
        IGDB API에서 게임 데이터를 추출합니다.

        Yields:
            dict[str, Any]: 게임 데이터 제너레이터 객체
        """
        token = await self._auth_provider.get_valid_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Client-ID": self._client_id,
        }

        offset = 0
        while True:
            paginated_query = (
                f"{self._BASE_QUERY} limit {self._LIMIT}; offset {offset};"
            )
            response = await self._client.post(
                url=self._API_URL, data=paginated_query, headers=headers
            )

            response.raise_for_status()
            response_data = response.json()

            if not response_data:
                break

            for item in response_data:
                yield item

            offset += self._LIMIT
