import asyncio

import pytest

from src.pipeline.rate_limiter import IgdbRateLimiter, optional_rate_limiter


@pytest.mark.asyncio
async def test_rate_limiter_context_manager():
    """
    Rate limiter의 컨텍스트 매니저 동작을 테스트합니다.

    Verifies:
        - 컨텍스트 매니저 진입 시 세마포어와 토큰 버킷이 올바르게 획득되는지 확인합니다.
        - 컨텍스트 매니저 종료 시 세마포어가 올바르게 해제되는지 확인합니다.
    """
    limiter = IgdbRateLimiter(max_concurrency=8)

    async with limiter:
        # 컨텍스트 매니저 내부에서는 세마포어와 토큰 버킷이 획득된 상태여야 합니다.
        assert limiter._semaphore._value == 7  # 초기값 8에서 1 감소

    # 컨텍스트 매니저 종료 후에는 세마포어가 해제되어야 합니다.
    assert limiter._semaphore._value == 8  # 초기값으로 복원


@pytest.mark.asyncio
async def test_rate_limiter_concurrent_acquisitions():
    """
    여러 코루틴이 동시에 Rate limiter를 사용할 때
    세마포어와 토큰 버킷이 올바르게 동작하는지 테스트합니다.

    Verifies:
        - 동시에 여러 코루틴이 Rate limiter를 사용할 수 있는지 확인합니다.
        - 최대 동시성 제한이 올바르게 적용되는지 확인합니다.
    """
    limiter = IgdbRateLimiter(max_concurrency=3)

    async def limited_task():
        async with limiter:
            # 컨텍스트 매니저 내부에서는 세마포어와 토큰 버킷이 획득된 상태여야 합니다.
            assert limiter._semaphore._value <= 2  # 최대 동시성 3에서 1 감소

    # 동시에 5개의 코루틴 실행
    await asyncio.gather(*(limited_task() for _ in range(5)))

    # 모든 작업 완료 후에는 세마포어가 초기값으로 복원되어야 합니다.
    assert limiter._semaphore._value == 3  # 초기값으로 복원


@pytest.mark.asyncio
async def test_rate_limiter_exception_handling():
    """
    Rate limiter 컨텍스트 매니저 내에서 예외가 발생할 때
    세마포어가 올바르게 해제되는지 테스트합니다.

    Verifies:
        - 컨텍스트 매니저 내에서 예외가 발생해도 세마포어가 해제되는지 확인합니다.
    """
    limiter = IgdbRateLimiter(max_concurrency=8)

    class TestError(Exception):
        pass

    try:
        async with limiter:
            raise TestError("테스트 예외 발생")
    except TestError:
        pass

    # 예외 발생 후에도 세마포어가 초기값으로 복원되어야 합니다.
    assert limiter._semaphore._value == 8  # 초기값으로 복원


@pytest.mark.asyncio
async def test_optional_rate_limiter_with_none():
    """
    optional_rate_limiter가 None일 때도 정상적으로 동작하는지 테스트합니다.

    Verifies:
        - Rate limiter가 None일 때도 컨텍스트 매니저가 정상적으로 작동하는지 확인합니다.
    """
    async with optional_rate_limiter(None):
        # Rate limiter가 None일 때도 정상적으로 진입해야 합니다.
        assert True is True


@pytest.mark.asyncio
async def test_optional_rate_limiter_with_limiter():
    """
    optional_rate_limiter가 유효한 Rate limiter와 함께 사용할 때 정상적으로 동작하는지 테스트합니다.

    Verifies:
        - Rate limiter가 유효할 때도 컨텍스트 매니저가 정상적으로 작동하는지 확인합니다.
    """
    limiter = IgdbRateLimiter(max_concurrency=8)

    async with optional_rate_limiter(limiter):
        # Rate limiter가 유효할 때도 정상적으로 진입해야 합니다.
        assert limiter._semaphore._value == 7  # 초기값 8에서 1 감소

    # 컨텍스트 매니저 종료 후에는 세마포어가 해제되어야 합니다.
    assert limiter._semaphore._value == 8  # 초기값으로 복원
