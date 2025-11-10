from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any

from loguru import logger

from src.pipeline.interfaces import AuthProvider, Extractor


class BaseIgdbExtractor(Extractor, ABC):
    """
    IGDB API Extractor의 공통 로직 베이스 클래스.
    페이징, 인증, 헤더 설정 등을 처리합니다.
    """

    # === 서브클래스에서 정의해야 하는 속성 ===
    @property
    @abstractmethod
    def api_url(self) -> str:
        """API 엔드포인트 URL. 서브클래스에서 정의해야 함."""
        pass

    @property
    def base_query(self) -> str:
        """기본 쿼리 문자열. 기본 값 정의."""
        return "fields *; sort id asc;"

    @property
    def limit(self) -> int:
        """페이지당 데이터 제한 개수. 기본 값 정의."""
        return 500

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
            paginated_query = f"{self.base_query} limit {self.limit}; offset {offset};"
            logger.debug(f"{class_name} - API 요청: {paginated_query}")

            try:
                response = await self._client.post(
                    url=self.api_url, content=paginated_query, headers=headers
                )
                response.raise_for_status()
                response_data = response.json()

                if not response_data:
                    logger.info(f"IGDB {class_name} 모든 데이터 추출 완료.")
                    break

                for item in response_data:
                    yield item

                offset += self.limit

            except Exception as e:
                logger.error(
                    f"IGDB {class_name} 데이터 추출 중 오류 발생 (offset={offset}): {e}"
                )
                raise


class IgdbExtractor(BaseIgdbExtractor):
    """IGDB API로부터 게임 데이터를 추출하는 Extractor 구현체."""

    @property
    def api_url(self) -> str:
        return "https://api.igdb.com/v4/games"


class IgdbPlatformExtractor(BaseIgdbExtractor):
    """IGDB API로부터 플랫폼 데이터를 추출하는 Extractor 구현체."""

    @property
    def api_url(self) -> str:
        return "https://api.igdb.com/v4/platforms"


class IgdbGenreExtractor(BaseIgdbExtractor):
    """IGDB API로부터 장르 데이터를 추출하는 Extractor 구현체."""

    @property
    def api_url(self) -> str:
        return "https://api.igdb.com/v4/genres"

class IgdbGameModeExtractor(BaseIgdbExtractor):
    """IGDB API로부터 게임 모드 데이터를 추출하는 Extractor 구현체."""

    @property
    def api_url(self) -> str:
        return "https://api.igdb.com/v4/game_modes"



class IgdbPlayerPerspectiveExtractor(BaseIgdbExtractor):
    """IGDB API로부터 플레이어 시점 데이터를 추출하는 Extractor 구현체."""

    @property
    def api_url(self) -> str:
        return "https://api.igdb.com/v4/player_perspectives"



class IgdbThemeExtractor(BaseIgdbExtractor):
    """IGDB API로부터 테마 데이터를 추출하는 Extractor 구현체."""

    @property
    def api_url(self) -> str:
        return "https://api.igdb.com/v4/themes"


