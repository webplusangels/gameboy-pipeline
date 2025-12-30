# 비동기 병렬 수집기 구현 (Async Parallel Collection)

## 개요

이 문서는 파이프라인에서 구현된 비동기 병렬 수집기의 핵심 코드 스니펫과 구현 원리를 소개합니다.

기존의 순차적 데이터 추출 방식에서 **비동기 병렬 추출 방식**으로 전환하여 **약 2.17배의 성능 향상**을 달성했습니다.

### 성능 개선 결과

| 추출 방식 | games (344k) | 총 시간 | 개선율 |
|----------|--------------|---------|--------|
| 순차 추출 | 830.6초 | 834.4초 (13분 54초) | - |
| 병렬 추출 | 374.3초 | 378.7초 (6분 18초) | **2.17배** |

> 자세한 성능 분석은 [04_Performance.md](./04_Performance.md)를 참조하세요.

---

## 핵심 구현 요소

비동기 병렬 수집기는 다음 3가지 핵심 요소로 구성됩니다:

1. **병렬 페이지 추출** (`extract_concurrent`)
2. **속도 제한기** (`IgdbRateLimiter`)
3. **배치 프로세서 통합** (`BatchProcessor`)

---

## 1. 병렬 페이지 추출 (Concurrent Page Extraction)

### 코드 스니펫: `extract_concurrent()` 메서드

```python
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
            # asyncio.TaskGroup으로 배치 단위 병렬 처리
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
```

### 핵심 포인트

#### 1. `asyncio.TaskGroup` 사용

```python
async with asyncio.TaskGroup() as tg:
    for _ in range(batch_size):
        task = tg.create_task(
            self._fetch_page(offset, query_str, headers)
        )
        tasks.append(task)
        offset += self.limit
```

- **Python 3.11+에서 권장되는 비동기 작업 그룹**
- `asyncio.gather()`보다 안전한 예외 처리
- 배치 내 원자성 보장: 하나라도 실패하면 전체 배치 취소
- ExceptionGroup을 통한 모든 예외 수집

#### 2. 배치 단위 병렬 처리

```python
batch_size = 8  # 동시에 8개 페이지 요청
```

- 한 번에 모든 페이지를 요청하지 않고 배치 단위로 분할
- 메모리 부족 방지
- API 요청 제한 준수

#### 3. 정렬을 통한 순서 보장

```python
results = [task.result() for task in tasks]
results.sort(key=lambda x: x[0])  # offset 기준 정렬
```

- 병렬 처리로 순서가 뒤바뀐 결과를 offset 기준으로 재정렬
- 데이터 일관성 보장

---

## 2. 속도 제한기 (Rate Limiter)

IGDB API는 계정당 **초당 4개 요청**과 **최대 8개 동시 요청** 제한이 있습니다. 이를 안전하게 준수하기 위해 `IgdbRateLimiter`를 구현했습니다.

### 코드 스니펫: `IgdbRateLimiter` 클래스

```python
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
```

### 핵심 포인트

#### 1. 이중 제어 메커니즘

```python
self._rate_limiter = AsyncLimiter(requests_per_second, time_period=1.0)
self._semaphore = asyncio.Semaphore(max_concurrency)
```

- **`AsyncLimiter` (토큰 버킷)**: 초당 요청 수 제한
  - 매초 토큰을 채우고, 요청 시 토큰 소비
  - 자동으로 토큰 보충 (명시적 해제 불필요)
  
- **`asyncio.Semaphore` (세마포어)**: 동시 요청 수 제한
  - 최대 동시 연결 슬롯 제한
  - 요청 완료 시 명시적으로 슬롯 해제 필요

#### 2. 컨텍스트 매니저 패턴

```python
async with limiter:
    response = await client.post(url, ...)
```

- `__aenter__`에서 세마포어와 토큰 획득
- `__aexit__`에서 세마포어 해제 (예외 발생 시에도 안전)
- Pythonic한 리소스 관리

### 코드 스니펫: Rate Limiter 적용

```python
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

    # Rate limiter 적용
    async with optional_rate_limiter(self._rate_limiter):
        response = await self._client.post(
            url=self.api_url, content=paginated_query, headers=headers
        )
        response.raise_for_status()

    data = response.json()
    logger.debug(f"Fetched offset={offset}, records={len(data) if data else 0}")

    return offset, data if data else []
```

#### 3. Optional Rate Limiter

```python
@asynccontextmanager
async def optional_rate_limiter(
    limiter: IgdbRateLimiter | None,
) -> AsyncGenerator[None, None]:
    """
    Rate limiter가 None일 때도 안전하게 사용할 수 있는 컨텍스트 매니저.
    """
    if limiter is not None:
        async with limiter:
            yield
    else:
        yield
```

- Rate limiter가 없을 때(테스트 등)도 동일한 코드로 작동
- 유연한 설계로 다양한 환경에서 사용 가능

---

## 3. 배치 프로세서 통합 (Batch Processor Integration)

### 코드 스니펫: `BatchProcessor` 병렬 처리

```python
async def process(
    self,
    extractor: Extractor,
    entity_name: str,
    dt_partition: str,
    last_run_time: datetime | None = None,
    concurrent: bool = False,
) -> BatchResult:
    """
    데이터를 배치 단위로 추출하고 적재합니다.

    Args:
        extractor: 데이터 추출기 인스턴스
        entity_name: 엔티티 이름 (예: "games", "platforms")
        dt_partition: 날짜 파티션 문자열 (예: "2025-01-15")
        last_run_time: 마지막 실행 시간 (증분 추출용, None이면 전체 추출)
        concurrent: 병렬 추출 사용 여부 (기본값: False)

    Returns:
        BatchResult: 적재된 파일 목록, 총 레코드 수, 배치 수

    Note:
        concurrent=True 사용 시, extractor 생성 시 rate_limiter를 설정해야 합니다.
    """
    uploaded_files: list[str] = []
    total_count = 0
    batch: list[dict[str, Any]] = []
    batch_count = 0

    s3_path_prefix = get_s3_path(entity_name, dt_partition)

    # 추출 방식 선택: 순차 또는 병렬
    if concurrent:
        data_stream = extractor.extract_concurrent(
            last_updated_at=last_run_time,
        )
    else:
        data_stream = extractor.extract(last_updated_at=last_run_time)

    async for item in data_stream:
        batch.append(item)
        total_count += 1

        if len(batch) >= self._batch_size:
            key = self._generate_batch_key(s3_path_prefix, batch_count, entity_name)
            await self._loader.load(batch, key)
            uploaded_files.append(key)
            logger.debug(
                f"S3에 '{entity_name}' 배치 {batch_count} 적재 완료: "
                f"{len(batch)}개 항목"
            )
            batch.clear()
            batch_count += 1

    # 남은 배치 처리
    if batch:
        key = self._generate_batch_key(s3_path_prefix, batch_count, entity_name)
        await self._loader.load(batch, key)
        uploaded_files.append(key)
        batch_count += 1

    return BatchResult(
        uploaded_files=uploaded_files,
        total_count=total_count,
        batch_count=batch_count,
    )
```

### 핵심 포인트

#### 1. 추출 방식 전환

```python
# 추출 방식 선택: 순차 또는 병렬
if concurrent:
    data_stream = extractor.extract_concurrent(
        last_updated_at=last_run_time,
    )
else:
    data_stream = extractor.extract(last_updated_at=last_run_time)
```

- `concurrent` 플래그로 간단하게 병렬/순차 전환
- 동일한 인터페이스로 두 방식 모두 지원
- 하위 호환성 유지

---

## 4. 오케스트레이터 통합 (Orchestrator Integration)

### 코드 스니펫: `PipelineOrchestrator`에서 병렬 추출 사용

```python
async def _run_entity(
    self,
    extractor: Extractor,
    entity_name: str,
    dt_partition: str,
    full_refresh: bool,
) -> PipelineResult:
    """
    단일 엔티티에 대해 파이프라인을 실행합니다.
    """
    logger.info(f"엔티티 '{entity_name}' 파이프라인 실행 시작...")
    start_time = perf_counter()
    extraction_start = datetime.now(UTC)

    # ... (생략) ...

    # 추출 및 적재 (병렬 모드 활성화)
    batch_result = await self._batch_processor.process(
        extractor=extractor,
        entity_name=entity_name,
        dt_partition=dt_partition,
        last_run_time=last_run_time,
        concurrent=True,  # 병렬 추출 활성화
    )
    new_files = batch_result.uploaded_files
    new_count = batch_result.total_count

    # ... (후처리 로직) ...

    return PipelineResult(
        entity_name=entity_name,
        record_count=new_count,
        file_count=len(new_files),
        elapsed_seconds=elapsed,
        mode=mode,
    )
```

### 핵심 포인트

#### 1. 프로덕션에서 병렬 추출 사용

```python
concurrent=True,  # 병렬 추출 활성화
```

- 프로덕션 환경에서 기본적으로 병렬 추출 사용
- 성능 최적화된 상태로 운영

---

## 아키텍처 다이어그램

```
┌─────────────────────────────────────────────────────────────┐
│                     PipelineOrchestrator                     │
│                    (concurrent=True 설정)                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                      BatchProcessor                          │
│         (extract_concurrent 또는 extract 선택)               │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  BaseIgdbExtractor                           │
│              extract_concurrent() 메서드                     │
│                                                               │
│  ┌───────────────────────────────────────────────────┐      │
│  │  asyncio.TaskGroup (batch_size=8)                 │      │
│  │  ┌─────────────────────────────────────────┐     │      │
│  │  │ Task 1: _fetch_page(offset=0)            │     │      │
│  │  │   └─> IgdbRateLimiter (세마포어 + 토큰)  │     │      │
│  │  ├─────────────────────────────────────────┤     │      │
│  │  │ Task 2: _fetch_page(offset=500)          │     │      │
│  │  │   └─> IgdbRateLimiter (세마포어 + 토큰)  │     │      │
│  │  ├─────────────────────────────────────────┤     │      │
│  │  │ ...                                      │     │      │
│  │  ├─────────────────────────────────────────┤     │      │
│  │  │ Task 8: _fetch_page(offset=3500)         │     │      │
│  │  │   └─> IgdbRateLimiter (세마포어 + 토큰)  │     │      │
│  │  └─────────────────────────────────────────┘     │      │
│  └───────────────────────────────────────────────────┘      │
│                                                               │
│  결과 정렬 (offset 기준) 후 yield                            │
└───────────────────────┬───────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                    IgdbRateLimiter                           │
│                                                               │
│  ┌─────────────────────────────────────────────────┐        │
│  │ AsyncLimiter (토큰 버킷)                         │        │
│  │  - 초당 3.2개 토큰 충전                          │        │
│  │  - 요청 시 토큰 소비                             │        │
│  └─────────────────────────────────────────────────┘        │
│                                                               │
│  ┌─────────────────────────────────────────────────┐        │
│  │ Semaphore (동시성 제어)                          │        │
│  │  - 최대 4개 동시 슬롯                            │        │
│  │  - 요청 완료 시 슬롯 해제                        │        │
│  └─────────────────────────────────────────────────┘        │
└───────────────────────┬───────────────────────────────────┘
                        │
                        ▼
                  IGDB API (httpx)
```

---

## 사용 예시

### 1. Extractor 생성 시 Rate Limiter 설정

```python
import httpx
from src.pipeline.extractors import IgdbExtractor
from src.pipeline.rate_limiter import IgdbRateLimiter
from src.pipeline.auth import IgdbAuthProvider

# Rate limiter 생성
rate_limiter = IgdbRateLimiter(
    requests_per_second=3.2,
    max_concurrency=4,
)

# Extractor 생성
async with httpx.AsyncClient() as client:
    extractor = IgdbExtractor(
        client=client,
        auth_provider=auth_provider,
        client_id=client_id,
        rate_limiter=rate_limiter,  # Rate limiter 설정
    )

    # 병렬 추출
    async for item in extractor.extract_concurrent(batch_size=8):
        print(item)
```

### 2. 순차 추출과 병렬 추출 비교

```python
# 순차 추출 (기존 방식)
async for item in extractor.extract():
    process(item)

# 병렬 추출 (개선된 방식)
async for item in extractor.extract_concurrent(batch_size=8):
    process(item)
```

### 3. BatchProcessor에서 병렬 추출 사용

```python
processor = BatchProcessor(loader=s3_loader)

# 병렬 추출 활성화
result = await processor.process(
    extractor=game_extractor,
    entity_name="games",
    dt_partition="2025-01-15",
    concurrent=True,  # 병렬 추출 활성화
)

print(f"처리 완료: {result.total_count}개 레코드")
```

---

## 설계 원칙

### 1. 안전한 병렬 처리

- **`asyncio.TaskGroup` 사용**: 예외 발생 시 모든 작업 취소
- **Rate Limiter**: API 제한 준수로 429 에러 방지
- **Retry 메커니즘**: 일시적 오류 자동 재시도

### 2. 유연한 설계

- **Optional Rate Limiter**: 테스트 환경에서 rate limiter 없이 실행 가능
- **Sequential/Concurrent 전환**: `concurrent` 플래그로 간단하게 전환
- **인터페이스 일관성**: 두 방식 모두 동일한 `AsyncGenerator` 반환

### 3. 성능 최적화

- **배치 단위 처리**: 메모리 효율성 확보
- **결과 정렬**: 데이터 순서 보장
- **안전 마진**: 클럭 스큐 문제 방지

---

## 한계와 트레이드오프

### 1. 작은 데이터셋에서는 오버헤드

```python
# 작은 데이터셋 (< 1000개)
# 순차 추출: 0.7초
# 병렬 추출: 0.99초 (배치 생성 오버헤드)
```

- 배치 크기보다 작은 데이터셋에서는 순차 추출이 더 빠를 수 있음
- 주로 대용량 데이터셋(`games` 엔티티)에서 효과적

### 2. API 제한 준수 필수

- Rate limiter 없이 병렬 추출 시 429 에러 발생 가능
- 프로덕션에서는 반드시 rate limiter 설정 필요

### 3. 복잡도 증가

- 순차 추출보다 코드 복잡도 증가
- 예외 처리와 상태 관리 주의 필요

---

## 관련 문서

- [01. 프로젝트와 구조](./01_Project.md)
- [02. 기술 스택](./02_Tech_Stacks.md)
- [03. 학습 기록](./03_Learning_Log.md) - 병렬 추출 구현 과정
- [04. 성능 측정](./04_Performance.md) - 벤치마크 결과

---

## 결론

비동기 병렬 수집기는 다음과 같은 이점을 제공합니다:

✅ **2.17배 성능 향상** (대용량 데이터 기준)  
✅ **안전한 병렬 처리** (`asyncio.TaskGroup` + Rate Limiter)  
✅ **유연한 설계** (순차/병렬 전환 가능)  
✅ **프로덕션 검증 완료** (GitHub Actions에서 안정적 운영)

이를 통해 파이프라인의 실행 시간을 대폭 단축하고, 데이터 수집 효율성을 크게 개선했습니다.
