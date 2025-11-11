from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
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
        """전체 추출을 위한 기본 쿼리 문자열."""
        return "fields *; sort id asc;"

    @property
    def incremental_query(self) -> str:
        """
        증분 추출을 위한 쿼리 문자열.
        
        Note:
            - updated_at으로 정렬하여 시간순 처리
            - where 절은 extract() 메서드에서 동적 추가
        """
        return "fields *;"

    @property
    def limit(self) -> int:
        """페이지당 데이터 제한 개수. 기본 값 정의."""
        return 500

    @property
    def safety_margin_minutes(self) -> int:
        """
        증분 쿼리 시 적용할 안전 마진(분 단위).
        
        클럭 스큐 문제로 인한 데이터 누락을 방지하기 위해
        last_updated_at에서 이 값만큼 빼고 쿼리합니다.
        
        예: last_updated_at = 10:00, safety_margin = 5
        → 쿼리: "where updated_at > 09:55"
        → 중복 허용, 누락 방지 (dbt에서 incremental 처리)
        """
        return 5

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

    async def extract(self, last_updated_at: datetime | None = None) -> AsyncGenerator[dict[str, Any], None]:
        """
        IGDB API에서 데이터를 추출합니다.

        Args:
            last_updated_at: 증분 추출을 위한 마지막 업데이트 시간 (없으면 전체 추출)

        Yields:
            dict[str, Any]: 데이터 제너레이터 객체
        """
        entity_name = self.__class__.__name__
        logger.info(f"IGDB {entity_name} 데이터 추출 시작...")

        # === 인증 헤더 설정 ===
        token = await self._auth_provider.get_valid_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Client-ID": self._client_id,
        }

        # === 쿼리 설정 ===
        query_str: str
        if last_updated_at:
            # 안전 마진 적용
            safe_timestamp = last_updated_at - timedelta(
                minutes=self.safety_margin_minutes
            )
            query_timestamp = int(safe_timestamp.timestamp())
            
            logger.info(
                f"IGDB {entity_name} 증분 추출: "
                f"last_updated_at={last_updated_at.isoformat()} "
                f"→ safe_timestamp={safe_timestamp.isoformat()} "
                f"(margin: {self.safety_margin_minutes}분, timestamp={query_timestamp})"
            )

            # IGDB Apicalypse 쿼리 문법: where 절 뒤에 sort 추가
            query_str = (
                f"{self.incremental_query} "
                f"where updated_at > {query_timestamp}; "
                f"sort updated_at asc, id asc;"
            )
        else:
            logger.info(f"IGDB {entity_name} 전체 추출 실행.")
            query_str = self.base_query

        # === 페이징을 통한 데이터 추출 ===
        offset = 0
        total_extracted = 0
        
        while True:
            paginated_query = f"{query_str} limit {self.limit}; offset {offset};"
            logger.debug(f"{entity_name} - API 요청: {paginated_query}")

            try:
                response = await self._client.post(
                    url=self.api_url, content=paginated_query, headers=headers
                )
                response.raise_for_status()
                response_data = response.json()

                if not response_data:
                    logger.info(
                        f"IGDB {entity_name} 모든 데이터 추출 완료. "
                        f"총 {total_extracted}개 추출 "
                        f"({'증분' if last_updated_at else '전체'} 모드)"
                    )
                    break

                for item in response_data:
                    yield item
                    total_extracted += 1

                offset += self.limit

            except Exception as e:
                logger.error(
                    f"IGDB {entity_name} 데이터 추출 중 오류 발생 "
                    f"(offset={offset}, extracted={total_extracted}): {e}"
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


