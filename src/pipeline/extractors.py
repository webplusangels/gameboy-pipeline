import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.pipeline.interfaces import AuthProvider, Extractor
from src.pipeline.rate_limiter import IgdbRateLimiter, optional_rate_limiter


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
        rate_limiter: IgdbRateLimiter | None = None,
    ) -> None:
        """
        Args:
            client: HTTP 클라이언트 (httpx.AsyncClient 등)
            auth_provider: 인증 토큰을 제공하는 AuthProvider
            client_id: 클라이언트 ID
            rate_limiter: IGDB API 호출 속도 제한기 (기본값: None)
        """
        self._client = client
        self._auth_provider = auth_provider
        self._client_id = client_id
        self._rate_limiter = rate_limiter

    async def extract(
        self, last_updated_at: datetime | None = None
    ) -> AsyncGenerator[dict[str, Any], None]:
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
            query_str = f"{self.incremental_query} where updated_at > {query_timestamp}; sort id asc;"
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
        reraise=True,
    )
    async def _fetch_page(
        self,
        offset: int,
        query_str: str,
        headers: dict[str, str],
    ) -> tuple[int, list[dict[str, Any]]]:
        """
        단일 페이지 데이터를 IGDB API에서 추출합니다.

        Args:
            offset: 페이지 오프셋
            query_str: IGDB 쿼리 문자열
            headers: HTTP 요청 헤더

        Returns:
            tuple[int, list[dict[str, Any]]]: (offset, 페이지 데이터 목록)
        """
        paginated_query = f"{query_str} limit {self.limit}; offset {offset};"

        async with optional_rate_limiter(self._rate_limiter):
            response = await self._client.post(
                url=self.api_url, content=paginated_query, headers=headers
            )
            response.raise_for_status()

        data = response.json()
        logger.debug(f"Fetched offset={offset}, records={len(data) if data else 0}")

        return offset, data if data else []

    async def extract_concurrent(
        self, last_updated_at: datetime | None = None, batch_size: int = 8
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        IGDB API에서 데이터를 병렬로 추출합니다.

        Args:
            last_updated_at: 증분 추출을 위한 마지막 업데이트 시간 (없으면 전체 추출)
            batch_size: 동시 요청할 페이지 수

        Yields:
            dict[str, Any]: 데이터 제너레이터 객체
        """
        entity_name = self.__class__.__name__
        logger.info(f"IGDB {entity_name} 병렬 데이터 추출 시작...")

        # === 인증 헤더 설정 ===
        token = await self._auth_provider.get_valid_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Client-ID": self._client_id,
        }

        # === 쿼리 설정 ===
        query_str: str
        if last_updated_at:
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

            query_str = f"{self.incremental_query} where updated_at > {query_timestamp}; sort id asc;"
        else:
            logger.info(f"IGDB {entity_name} 전체 추출 실행.")
            query_str = self.base_query

        # === 병렬 페이징 데이터 추출 ===
        offset = 0
        total_extracted = 0
        is_finished = False

        while not is_finished:
            tasks: list[asyncio.Task[tuple[int, list[dict[str, Any]]]]] = []

            try:
                async with asyncio.TaskGroup() as tg:
                    for _ in range(batch_size):
                        task = tg.create_task(
                            self._fetch_page(offset, query_str, headers)
                        )
                        tasks.append(task)
                        offset += self.limit
            except* Exception as e:
                for exc in e.exceptions:
                    logger.error(
                        f"IGDB {entity_name} 병렬 데이터 추출 중 오류 발생 "
                        f"(offset={offset}, extracted={total_extracted}): {exc}"
                    )
                raise

            results = [task.result() for task in tasks]
            results.sort(key=lambda x: x[0])  # offset 기준 정렬

            for _, data in results:
                if not data:
                    is_finished = True
                    logger.info(
                        f"IGDB {entity_name} 모든 데이터 추출 완료. "
                        f"총 {total_extracted}개 추출 "
                        f"({'증분' if last_updated_at else '전체'} 모드)"
                    )
                    break

                for item in data:
                    yield item
                    total_extracted += 1

        logger.info(
            f"IGDB {entity_name} 병렬 추출 종료. 총 {total_extracted}개 레코드 추출."
        )


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


# PopScore Extractor
class IgdbPopScoreExtractor(BaseIgdbExtractor):
    """IGDB API로부터 인기 점수 데이터를 추출하는 Extractor 구현체."""

    @property
    def api_url(self) -> str:
        return "https://api.igdb.com/v4/popularity_primitives"

    @property
    def base_query(self) -> str:
        """
        Popularity Types (전체 11개):
        1: Visits (IGDB 조회수)
        2: Want to Play (기대 지수)
        3: Playing (현재 플레이 중)
        4: Played (플레이 한 적 있음)
        5: 24hr Peak Players (Steam)
        6: Positive Reviews (Steam)
        7: Negative Reviews (Steam)
        8: Total Reviews (Steam)
        9: Global Top Sellers (Steam)
        10: Most Wishlisted Upcoming (Steam)
        34: 24hr Hours Watched (Twitch)
        """
        target_types = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 34)
        return f"fields game_id, popularity_type, value; where popularity_type = {target_types}; sort id asc;"

    async def extract(
        self, last_updated_at: datetime | None = None
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        IGDB API에서 데이터를 추출합니다.

        Args:
            last_updated_at: 증분 추출을 위한 마지막 업데이트 시간 (없으면 전체 추출)

        Yields:
            dict[str, Any]: 데이터 제너레이터 객체
        """
        # PopScore는 증분 추출을 지원하지 않음
        if last_updated_at:
            logger.warning(
                "IgdbPopScoreExtractor는 증분 추출을 지원하지 않습니다. 전체 추출을 수행합니다."
            )

        async for item in super().extract(last_updated_at=None):
            yield item

    async def extract_concurrent(
        self, last_updated_at: datetime | None = None, batch_size: int = 8
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        IGDB API에서 데이터를 병렬로 추출합니다.

        Args:
            last_updated_at: 증분 추출을 위한 마지막 업데이트 시간 (없으면 전체 추출)
            batch_size: 병렬 요청 배치 크기

        Yields:
            dict[str, Any]: 데이터 제너레이터 객체
        """
        # PopScore는 증분 추출을 지원하지 않음
        if last_updated_at:
            logger.warning(
                "IgdbPopScoreExtractor는 증분 추출을 지원하지 않습니다. 전체 추출을 수행합니다."
            )

        async for item in super().extract_concurrent(
            last_updated_at=None, batch_size=batch_size
        ):
            yield item


class IgdbPopularityTypesExtractor(BaseIgdbExtractor):
    """IGDB API로부터 인기 점수 유형(Popularity Types) 데이터를 추출하는 Extractor 구현체."""

    @property
    def api_url(self) -> str:
        return "https://api.igdb.com/v4/popularity_types"
