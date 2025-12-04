import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Self

from aiolimiter import AsyncLimiter


class IgdbRateLimiter:
    """
    IGDB API 요청 속도 제한기.

    IGDB는 계정당 초당 4개의 요청 제한과 최대 8개의 동시 요청 제한이 있습니다.
    이 클래스는 AsyncLimiter(토큰 버킷)와 Semaphore(동시성 제한)를
    결합하여 API 요청을 안전하게 제어합니다.

    Example:
        >>> limiter = IgdbRateLimiter()
        >>> async with limiter:
        ...     response = await client.post(url, ...)
    """

    def __init__(
        self,
        requests_per_second: float = 3.2,
        max_concurrency: int = 4,
    ) -> None:
        """
        Rate limiter 초기화.

        Args:
            requests_per_second: 초당 허용되는 최대 요청 수. IGDB 제한은 4 req/sec.
            max_concurrency: 동시에 처리할 수 있는 최대 요청 수. IGDB 제한은 동시 요청 8개입니다.
        """
        self._rate_limiter = AsyncLimiter(requests_per_second, time_period=1.0)
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def __aenter__(self) -> Self:
        """
        Rate limit 획득. 토큰 버킷과 세마포어를 순차적으로 획득.

        동시 연결 슬롯을 확보한 후, 전송 직전 요청 속도 제한을 적용합니다.
        """
        await self._semaphore.acquire()
        await self._rate_limiter.acquire()
        return self

    async def __aexit__(self, *args: object) -> None:
        """세마포어 해제. 예외 발생 여부와 관계없이 항상 해제됨."""
        self._semaphore.release()


@asynccontextmanager
async def optional_rate_limiter(
    limiter: IgdbRateLimiter | None,
) -> AsyncGenerator[None, None]:
    """
    Rate limiter가 None일 때도 안전하게 사용할 수 있는 컨텍스트 매니저.

    Args:
        limiter: IgdbRateLimiter 인스턴스 또는 None

    Yields:
        None (rate limiting 적용 후 제어권 반환)

    Example:
        >>> async with optional_rate_limiter(self._rate_limiter):
        ...     response = await client.post(...)
    """
    if limiter is not None:
        async with limiter:
            yield
    else:
        yield
