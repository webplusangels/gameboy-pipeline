from collections.abc import AsyncGenerator
from typing import Any

from loguru import logger

from src.pipeline.interfaces import AuthProvider, Extractor


class BaseIgdbExtractor(Extractor):
    """
    IGDB API Extractor의 공통 로직 베이스 클래스.
    페이징, 인증, 헤더 설정 등을 처리합니다.
    """

    # === 서브클래스에서 정의해야 하는 속성 ===
    _API_URL: str
    _BASE_QUERY: str
    _LIMIT: int

    def __init__(
        self,
        client: Any,
        auth_provider: AuthProvider,
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

        if (
            not hasattr(self, "_API_URL")
            or not hasattr(self, "_BASE_QUERY")
            or not hasattr(self, "_LIMIT")
        ):
            raise NotImplementedError(
                "서브클래스에서 _API_URL, _BASE_QUERY, _LIMIT 속성을 정의해야 합니다."
            )

    async def extract(self) -> AsyncGenerator[dict[str, Any], None]:
        """
        IGDB API에서 데이터를 추출합니다.

        Yields:
            dict[str, Any]: 데이터 제너레이터 객체
        """
        class_name = self.__class__.__name__
        logger.info(f"IGDB {class_name} 데이터 추출 시작...")

        # === 인증 헤더 설정 ===
        token = await self._auth_provider.get_valid_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Client-ID": self._client_id,
        }

        # === 페이징을 통한 데이터 추출 ===
        offset = 0
        while True:
            paginated_query = (
                f"{self._BASE_QUERY} limit {self._LIMIT}; offset {offset};"
            )
            logger.debug(f"{class_name} - API 요청: {paginated_query}")

            try:
                response = await self._client.post(
                    url=self._API_URL, content=paginated_query, headers=headers
                )
                response.raise_for_status()
                response_data = response.json()

                if not response_data:
                    logger.info(f"IGDB {class_name} 모든 데이터 추출 완료.")
                    break

                for item in response_data:
                    yield item

                offset += self._LIMIT

            except Exception as e:
                logger.error(
                    f"IGDB {class_name} 데이터 추출 중 오류 발생 (offset={offset}): {e}"
                )
                raise


class IgdbExtractor(BaseIgdbExtractor):
    """IGDB API로부터 게임 데이터를 추출하는 Extractor 구현체."""

    _API_URL = "https://api.igdb.com/v4/games"
    _BASE_QUERY = "fields *;"
    _LIMIT = 500


class IgdbPlatformExtractor(BaseIgdbExtractor):
    """IGDB API로부터 플랫폼 데이터를 추출하는 Extractor 구현체."""

    _API_URL = "https://api.igdb.com/v4/platforms"
    _BASE_QUERY = "fields *;"
    _LIMIT = 50


class IgdbGenreExtractor(BaseIgdbExtractor):
    """IGDB API로부터 장르 데이터를 추출하는 Extractor 구현체."""

    _API_URL = "https://api.igdb.com/v4/genres"
    _BASE_QUERY = "fields *;"
    _LIMIT = 50


class IgdbGameModeExtractor(BaseIgdbExtractor):
    """IGDB API로부터 게임 모드 데이터를 추출하는 Extractor 구현체."""

    _API_URL = "https://api.igdb.com/v4/game_modes"
    _BASE_QUERY = "fields *;"
    _LIMIT = 50
