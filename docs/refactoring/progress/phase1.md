# Phase 1

## 1. `mypy` 12 -> 0

### 에러 발생 로그

```powershell
scripts\run_pipeline.py:12: error: Skipping analyzing "aioboto3": module is installed, but missing library stubs or py.typed marker  [import-untyped]
scripts\run_pipeline.py:12: note: See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
scripts\run_pipeline.py:186: error: Unexpected keyword argument "last_updated_at" for "extract" of "Extractor"  [call-arg]
src\pipeline\interfaces.py: note: "extract" of "Extractor" defined here
scripts\run_pipeline.py:299: error: "object" has no attribute "extend"  [attr-defined]
scripts\run_pipeline.py:300: error: Unsupported operand types for + ("object" and "int")  [operator]
scripts\run_pipeline.py:302: error: Argument 1 to "len" has incompatible type "object"; expected "Sized"  [arg-type]
scripts\run_pipeline.py:310: error: Argument 1 to "len" has incompatible type "object"; expected "Sized"  [arg-type]
scripts\run_pipeline.py:375: error: Argument "token" to "StaticAuthProvider" has incompatible type "str | None"; expected "str"  [arg-type]
scripts\run_pipeline.py:385: error: Argument "bucket_name" to "S3StateManager" has incompatible type "str | None"; expected "str"  [arg-type]
scripts\run_pipeline.py:386: error: Argument "bucket_name" to "S3Loader" has incompatible type "str | None"; expected "str"  [arg-type]
scripts\run_pipeline.py:389: error: Cannot instantiate abstract class "BaseIgdbExtractor" with abstract attribute "api_url"  [abstract]
scripts\run_pipeline.py:392: error: Argument "client_id" has incompatible type "str | None"; expected "str"  [arg-type]
scripts\run_pipeline.py:405: error: Argument "bucket_name" to "run_entity_pipeline" has incompatible type "str | None"; expected "str"  [arg-type]
Found 12 errors in 1 file (checked 9 source files)
```

### 수정 사항

**1. `aioboto3` 임포트 에러 문제**

```toml
<!-- pyproject.toml -->
[[tool.mypy.overrides]]
module = ["botocore.*", "aioboto3.*"]
ignore_missing_imports = true
```

**2. `extract` 모듈**

```python
@abstractmethod
async def extract(self, last_updated_at: datetime | None) -> AsyncGenerator[dict[str, Any], None]:
    """
    외부 소스로부터 데이터를 추출합니다.

    Args:
        last_updated_at (datetime | None): 증분 추출을 위한 마지막 업데이트 시간.
            None인 경우 전체 데이터를 추출합니다.

    Yields:
        dict[str, Any]: 추출된 데이터 항목.
    """
    raise NotImplementedError
    yield
```

다음과 같이 `last_updated_at`을 인자로 받아 미래 유지 보수와 타입 안정성 확보

**3. Manifest 타입 안정성**

JSON 데이터를 읽어 값을 갱신하고 저장하기 위한 용도로, `manifest_data`의 자료형이 딕셔너리인 것을 알지만 `mypy`는 보수적으로 추론하기 때문에 특정 자료형에 해당하는 메소드를 불러올 때 에러를 발생시킴

```python
class Manifest(TypedDict, total=False):
    files: list[str]
    total_count: int
    created_at: str
    updated_at: str
    batch_count: int

...

manifest_data: Manifest = {
    "files": new_files,
    "total_count": new_count,
    "created_at": extraction_start.isoformat(),
    "updated_at": extraction_start.isoformat(),
    "batch_count": len(new_files),
}

...
```

`TypedDict`를 통해 JSON으로 읽은 `Manifest`의 자료형을 정의해주고, 타입 안정성을 확보할 수 있음

`Total` 옵션을 False로 설정하면 모든 값을 채워주지 않더라도 타입 추론이 가능함

`DataClass`와 같은 방법도 있지만, 변환 과정이 추가로 필요하기 때문에 코드가 간결하다는 단점이 있음

**4. MyPy 오탐 현상**

문맥을 이해하지 못해서 `if not all([client_id, static_token, bucket_name])`이 런타임 환경에서는 작동하지만 `mypy`가 똑똑하게 추론할 수 없기 때문에 `str | None`에서 `None`의 가능성을 제시하는 에러를 발생시킴

```python
assert client_id is not None
assert static_token is not None
assert bucket_name is not None
```

해당 코드를 `if not all...`코드 뒤에 붙이면 `None`값이 나오지 않는다는 사실을 확정지을 수 있기 때문에 가장 간단한 해결책

개별적인 `if`문이나, `pydantic-settings`를 통해 검증하는 방법을 도입하는 것은 장기적으로 추천되지만 일단 위의 방법으로도 충분히 에러를 제거할 수 있음

**5. `ALL_ENTITIES`**

`ALL_ENTITIES`에 Extractor 클래스들을 포함하고 있는데, 여기서 추상 메서드로 타입 구체화를 시켜주지 않아서 에러가 생김.

```python
ALL_ENTITIES: dict[str, type[BaseIgdbExtractor]] = {
    "games": IgdbExtractor,
    "platforms": IgdbPlatformExtractor,
    "genres": IgdbGenreExtractor,
    "game_modes": IgdbGameModeExtractor,
    "player_perspectives": IgdbPlayerPerspectiveExtractor,
    "themes": IgdbThemeExtractor,
}
```

`BaseIgdbExtractor`로 타입 힌트를 준다면 이렇게 수정하면 해결할 수 있음

## 2. `ruff` 24 -> 0

> 대부분 --fix 옵션으로 고칠 수 있음 (공백, import 순서 등)

## 3. `settings` 임포트

제대로 임포트되어 있지 않던 `src\config.py`의 `settings`를 가져와 별도의 라이브러리 없이 환경변수를 참조할 수 있도록 변경

## 4. 모듈 분리

`run_pipeline.py`의 코드가 비대하고(457줄), 역할이 중첩되어 있기 때문에 `pipeline/`에 여러 모듈로 분리하였음

```plain
src/
├─ __init__.py
├─ config.py                       # settings 임포트/환경변수 접근
└─ pipeline/
    ├─ __init__.py
    ├─ interfaces.py                # Extractor 인터페이스 등
    ├─ constants.py                 # 상수
    ├─ manifest.py                  # Manifest TypedDict 및 관련 유틸
    ├─ utils.py                     # 공용 유틸 함수들
    ├─ extractors.py                # Extractor 관련 구현
    ├─ loaders.py                   # S3Loader 등 로더 관련 구현
    ├─ state.py                     # S3StateManager 등 상태 관리
    ├─ s3_ops.py                    # S3 관련 로직 구현
    └─ registry.py                  # 설정값
```

## 5. 테스트 가능한 구조로 만들기

`run_pipeline.py` 스크립트가 길고 따라서 테스트하기에 적합하지 않은 구조이기 때문에 4번의 모듈 분리를 이어서 진행하되, 관심사의 분리를 중점적으로 해당 파일을 Orchestrator와 Wrapper, Helper(Componenets)로 나누어 모든 과정을 테스트할 수 있도록 작은 단위로 나눔

## 6. 트러블슈팅

### 1. Mock 객체로 Exception 처리 시 TypeError

**증상**

- **에러:** `TypeError: catching classes that do not inherit from BaseException is not allowed`
- **상황:** `try: ... except s3_client.exceptions.NoSuchKey:` 구문 테스트 중 발생.

**원인**

- `AsyncMock`의 속성(`s3_client.exceptions.NoSuchKey`)에 접근하면 기본적으로 또 다른 **Mock 객체**가 반환됨.
- Python의 `except` 절은 **Mock 객체가 아닌, 실제 `BaseException`을 상속받은 클래스**만 허용함.

**해결책 (`conftest.py`)**

> Mock 경로에 **진짜 예외 클래스**를 할당하여 해결.

```python
@pytest.fixture
def mock_s3_client(mocker):
    mock = mocker.AsyncMock()
    # 가짜 예외 클래스 정의 및 할당
    class NoSuchKey(Exception): pass
    mock.exceptions = MagicMock()
    mock.exceptions.NoSuchKey = NoSuchKey
    return mock
```

> [!TIP]
> 주의: 예외 처리를 테스트할 땐, Mock 속성에 반드시 **실제 Exception 클래스**를 연결해야 한다.

### 2. 동기 메서드(get_paginator)가 코루틴을 반환하는 문제

**증상**

- **에러:** `AttributeError: 'coroutine' object has no attribute 'paginate'`
- **상황:** `paginator = s3_client.get_paginator(...)` 호출 후 `paginator.paginate` 접근 시 발생.

**원인**

- `AsyncMock`은 모든 메서드 호출 결과를 **코루틴(awaitable)**으로 감싸서 반환함.
- 하지만 `boto3`의 `get_paginator`는 서버 통신 없이 설정 객체만 반환하는 **동기(Sync) 메서드**임.
- 코드에서 `await` 없이 호출했으나, Mock은 코루틴을 줘버려서 속성 접근이 불가능해짐.

**해결책**

> `get_paginator` 메서드만 `MagicMock`(동기)으로 덮어쓰기.

```python
# conftest.py 권장
mock_s3_client.get_paginator = MagicMock()

# test_xxx.py
paginator = mock_s3_client.get_paginator.return_value
paginator.paginate.side_effect = async_generator_func # 내부는 비동기 제너레이터 연결
```

> [!TIP]
> 주의: `AsyncMock` 객체라도, 실제 코드가 `await` 없이 부르는 메서드(`get_paginator`, `generate_presigned_url` 등)는 `MagicMock`으로 재정의해야 한다.

### 3. `async for`와 Mocking (Async Generator)

**증상**

- **에러:** `TypeError: 'async for' requires an object with __aiter__ method, got coroutine`
- **상황:** `async for item in extractor.extract(...):` 테스트 중 발생.

**원인**

- `extractor`가 `AsyncMock`이라서 `.extract()` 호출 시 **코루틴**이 반환됨.
- `async for`는 코루틴이 아니라, **Async Iterator(비동기 반복자)** 객체를 원함.

**해결책**

> 해당 메서드를 `MagicMock`으로 바꾸고, `side_effect`에 비동기 제너레이터 함수를 연결.

```python
async def my_async_gen(): yield {"id": 1}

# extract 호출 자체는 동기 -> 결과물로 비동기 제너레이터 반환
mock_extractor.extract = MagicMock()
mock_extractor.extract.side_effect = my_async_gen
```

> [!TIP]
> 주의: `async for`로 소비하는 메서드는 `AsyncMock`이 아니라 `MagicMock` + `async generator` 조합이어야 한다.

### 4. `pytest.raises`가 동작하지 않음 (Exception Swallowing)

**증상**

- **상황:** `pytest.raises(Exception)`을 썼는데 테스트가 실패(예외 발생 안 함).
- **코드:** `try: ... except Exception: logger.error(...)`

**원인**

- 프로덕션 코드 내부에서 `try-except`로 에러를 이미 잡아서 처리(Swallowing)했기 때문에, 테스트 함수 밖으로 에러가 전파되지 않음.

**해결책**

> `pytest.raises`를 제거하고, "호출 여부"나 "로그 기록"을 검증.

```python
# 에러가 나도 함수는 정상 종료되므로 await만 수행
await invalidate_cloudfront_cache(...)
mock_client.create_invalidation.assert_called_once() # 호출 시도는 했는지 확인
```

> [!TIP]
> 주의: 코드가 에러를 안전하게 처리(Catch)하도록 설계되었다면, `raises`가 아니라 Side Effect를 검증해야 한다.

### 5. AsyncMock 생성자 인자 실수 (`spec` vs `name`)

**증상**

- **에러:** `AttributeError: Mock object has no attribute '__aenter__'`
- **상황:** `AsyncMock("my_mock")`로 객체 생성 시 발생.

**원인**

- `Mock` 생성자의 첫 번째 인자는 `spec`(스펙 정의)임.
- 문자열을 넘기면 "이 Mock은 문자열(str)처럼 동작해라"라는 뜻이 됨. 문자열엔 `__aenter__`가 없어서 에러 발생.

**해결책**

> 이름을 지정하려면 반드시 키워드 인자(`name=`) 사용.

```python
# ❌ Bad
mock = AsyncMock("my_client")

# ⭕ Good
mock = AsyncMock(name="my_client")
```

### 6. Patch 적용 시점과 객체 생성 시점의 불일치

**증상**

- **에러:** `Expected 'process' to be called once. Called 0 times.`
- **상황:** `Orchestrator` 테스트에서 Mock이 전혀 호출되지 않음.

**원인**

- `patch` 컨텍스트(`with patch...`) 밖에서 이미 `Orchestrator`를 인스턴스화(`new Orchestrator()`)함.
- 객체 생성 시점(`__init__`)에 이미 **진짜 클래스**가 주입되어 버림. 뒤늦게 patch 해봤자 소용없음.

**해결책**

> **`with patch` 블록 내부**에서 테스트 대상 객체를 생성해야 함.

```python
with patch("src...BatchProcessor") as MockBP:
    # Patch가 적용된 상태에서 생성해야 Mock이 주입됨
    orchestrator = PipelineOrchestrator(...)
    await orchestrator.run(...)
```

> [!TIP]
> 주의: 생성자(`__init__`)에서 의존성을 직접 생성하는 클래스를 테스트할 땐, **인스턴스 생성 시점**이 Patch 범위 안에 있는지 확인해야 한다.

## 7. `await` 대상이 아닌 객체를 리턴함

**증상**

- **에러:** `TypeError: object BatchResult can't be used in 'await' expression`
- **상황:** `result = await batch_processor.process(...)` 테스트 중 발생.

**원인**

- Mock킹 할 때 `return_value`로 일반 객체(`BatchResult`)를 줌.
- 하지만 코드는 `await`를 사용 중. 일반 객체는 `await` 할 수 없음.

**해결책**

메서드를 `AsyncMock`으로 만들어, 호출 시 **코루틴**을 반환하게 함.

```python
# ❌ Bad (동기 리턴)
mock_bp.process.return_value = BatchResult(...)

# ⭕ Good (비동기 리턴)
mock_bp.process = AsyncMock(return_value=BatchResult(...))
```

> [!TIP]
> 주의: `await func()`로 호출되는 메서드의 Mock은 반드시 `AsyncMock`이어야 한다.
