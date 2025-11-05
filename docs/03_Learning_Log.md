# 03. 학습 및 문제 해결 로그

이 문서는 프로젝트를 진행하며 마주친 주요 기술적 문제와 이를 해결한 과정을 기록합니다. 실제 제가 고민한 문제들에 대해 순차적으로 기술하겠습니다.

<!-- [Architecture] - 설계 관련
[Testing] - 테스트 작성/실행
[CI/CD] - GitHub Actions
[Tooling] - uv, ruff, mypy
[Python] - 언어 자체 이슈 -->

## [Architecture] TDD

2025-10-31

### 문제

> [!WARNING]
> TDD의 자세한 구현 방식을 모른다!

이 프로젝트를 시작하기 위해 목표로 삼은 것은 작지만 현대적인 요소가 모두 들어갈 수 있는 빠르고 확장성 있는 파이프라인 만들기였습니다.

이 정의에 가장 알맞는 개발 방법론은 테스트를 먼저 작성한 뒤 테스트 통과에 필요한 기능들을 순차적으로 개발하는 단순하며 문제 해결 중심의 **TDD**라고 생각했습니다.

문제가 있었다면 제가 TDD에 대해 이론적으로, 혹은 구현의 극히 일부만 알고 있었다는 사실입니다.

### 해결

`Gemini`와 `Github Copilot`의 도움을 받아 [`CONTRIBUTING.md`](../CONTRIBUTING.md) 문서를 통해 가이드를 만들었습니다 (~~기승전AI~~). 기초적인 순서를 제가 개발하는 코드에 적용한다고 가정하면 다음과 같습니다.

1. **🔴 RED: 실패하는 테스트 작성**

   - `tests/` 디렉터리에 새 기능에 대한 테스트 코드(`test_*.py`)를 작성합니다.
   - `mocker`를 사용해 외부 의존성(API, S3)을 철저히 모킹(Mocking)합니다.
   - `pytest`를 실행하여 **테스트가 예상대로 실패하는 것을 확인**합니다.

2. **🟢 GREEN: 테스트를 통과하는 최소한의 코드 작성**

   - `src/` 디렉터리에 `RED` 단계의 테스트를 **겨우 통과할 만큼의 최소한의 코드**를 작성합니다.
   - `pytest`를 실행하여 **모든 테스트가 통과하는 것을 확인**합니다.

3. **🟡 REFACTOR: 코드 개선**
   - 테스트가 통과하는 "안전망" 위에서 코드의 구조를 개선하고, 중복을 제거하며, 가독성을 높입니다.
   - 리팩토링 후에도 `pytest`를 실행하여 **모든 테스트가 계속 통과하는지 확인**합니다.

**RED** -> **GREEN** -> **REFACTOR** 각 단계에서 커밋을 반복하고, 기능 개발이 완료되면 이 브랜치를 push하게 됩니다. main 브랜치에 merge하는 과정에서 추가로 Github Actions의 CI 파이프라인이 다시 테스트를 실행하게 되는데요. 이는 뒤의 CI 파이프라인 작성 문제에서 자세히 설명합니다.

TDD에서 Git Branch 전략은 보통 'Squash and Merge'를 사용하는데, 쉽게 말해 브랜치를 단순히 합치는게 아닌 기능 개발 과정에서 진행한 커밋의 이력들을 main 브랜치에 하나의 새로운 커밋 형태로 합치는 형태입니다. 자잘한 TDD 히스토리를 기능 단위인 하나의 커밋으로 병합되는 것이죠.

또 하나 중요한건 TDD는 **외부 환경과 분리된 순수한 로직 테스트를 지향**한다는 것입니다. 현재의 데이터 파이프라인은 서드파티 API(IGDB)를 사용하고 있고, 이 API는 완전한 오픈소스에다 Amazon에서 운영하고 있는 만큼 안정성은 높지만 테스트 과정에서는 모킹된 샘플 데이터를 사용하는 것이 좋다고 생각합니다. 이를 위해 실제 IGDB API 응답을 기록해 모킹 응답을 저장하고 이를 테스트에 쓰는 방안을 고려 중입니다.

### 결과

이 과정을 통해 실제로 어떤 과정을 통해 TDD가 진행되는지 배울 수 있었습니다. 이 개발 플로우는 프로젝트 종료까지 계속해서 반복하게 되는 만큼, 선 테스트 후 구현 과정이 익숙하지는 않지만 익숙해져야 할 필요가 있다고 느꼈습니다.

현재는 ETL의 `extractors` 개발 중, 에러 케이스를 포함한 5개의 기본적인 테스트를 통과하는 코드를 구현하고, 이를 리팩토링하는 과정에 있습니다.

## [CI/CD] Github Actions CI 파이프라인

2025-11-01

### 문제

> [!WARNING]
> CI 파이프라인 작성의 효율적이고 필수적인 방법을 모른다!

처음 작성한 CI 워크플로우는 다음과 같은 항목을 포함하고 있었습니다.

- Python 단일 버전
- pytest만 실행

하지만 이 워크플로우는 `pyproject.toml`에서 기술한 `>=3.11`을 준수하지 않았고, 포맷팅, 린팅 에러 등을 포함할 수 있었습니다.

### 해결

이번에도 베스트 프랙티스를 찾기 위해 AI와 개선안을 논의하며 다음과 같은 항목을 포함하는 구조로 변경하였습니다.

- Python 버전 매트릭스로 3개 버전 동시 테스트 (3.11, 3.12, 3.13)
- `uv` 캐싱으로 CI 속도 향상
- `--extra dev`로 개발 의존성 명시적 설치
- 환경 변수 주입
- Codecov 업로드로 커버리지 추적
- 포맷팅/린팅과 테스트 분리해 병렬 실행

이를 포함한 워크플로우는 다음과 같습니다.

```yaml
name: CI with uv

on:
  push:
    branches: ["main"]
  pull_request:
    branches: ["main"]

jobs:
  lint:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Set up uv
        uses: astral-sh/setup-uv@v1
        with:
          enable-cache: true

      - name: Install dependencies with uv
        run: |
          uv venv
          uv sync --extra dev

      - name: Run Ruff linter
        run: |
          uv run ruff check src tests

      - name: Check code formatting
        run: |
          uv run ruff format --check src tests

      - name: Run type checker with uv
        run: |
          uv run mypy src

  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        python-version: ["3.11", "3.12", "3.13"]

    steps:
      # 코드 체크아웃
      - uses: actions/checkout@v4

      # Python 설정
      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}

      # 의존성 설치 - uv
      - name: Set up uv
        uses: astral-sh/setup-uv@v1
        with:
          enable-cache: true

      # 의존성 설치 - 프로젝트
      - name: Install dependencies with uv
        run: |
          uv venv
          uv sync --extra dev

      # 테스트 실행
      - name: Run tests with uv
        env:
          IGDB_CLIENT_ID: "test-client-id"
          IGDB_CLIENT_SECRET: "test-client-secret"
          IGDB_RATE_LIMIT: 4
          LOG_LEVEL: "INFO"
        run: |
          uv run pytest --cov=src --cov-report=xml

      # 코드 커버리지 업로드
      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v4
        if: matrix.python-version == '3.11'
        with:
          files: ./coverage.xml
          flags: unittests
          name: codecov-py${{ matrix.python-version }}
          fail_ci_if_error: false
```

### 결과

이번 작업에서 배운 점은 다음과 같습니다.

**1. 병렬 실행**

Github Actions에서 병렬 실행이 가능한거 알고 계셨나요?

**2. 캐싱**

캐싱으로 CI 시간을 단축할 수 있다는 사실도 처음 알았습니다. Github Actions가 가상 머신을 쓸 것이라는 추측을 어렴풋이 했지만 이런 기능도 가능하다니...

**3. 매트릭스 전략**

Python 버전 병렬 테스트로 호환성도 보장할 수 있습니다.

**4. CodeCov**

간단한 설정으로 현 코드의 테스트 커버리지 보고서를 업로드하고 시각적으로 확인할 수 있는 서비스입니다.

하다못해 Github Actions 워크플로우 파일도 최적화하는데 고려해야 할 점이 이렇게 많네요. 특히 가상 환경으로 `uv`를 처음 쓰면서 새로 알게 된 사실이 많았습니다.

## [Architecture] 추상 클래스 ABC

2025-11-01

### 문제

> [!WARNING]
> 의존성 역전 원칙(DIP)을 구현하는 방법을 모른다!

의존성 역전 원칙(DIP)은 다음과 같습니다.

> 고수준 모듈은 저수준 모듈에 의존해서는 안 된다. 둘 모두 추상화(인터페이스)에 의존해야 한다.

Python에서는 추상 클래스(인터페이스)를 어떻게 정의할 수 있을까요?

### 해결

`abc` 라이브러리의 `ABC`와 `abstactmethod`를 통해 쉽게 구현할 수 있습니다. 쉽게 말해 클래스가 `ABC`를 상속하면 해당 클래스를 추상 클래스라고 선언하고, 내부 메서드에 `@abstractmethod` 데코레이터를 붙이면 이 추상 클래스를 상속하는 자식 클래스들 내부에서 반드시 구현해야 하는 메서드임을 표현합니다. 예시는 다음과 같습니다.

```python
class Extractor(ABC):
    @abstractmethod
    async def extract(self) -> AsyncGenerator[dict[str, Any], None]:
        pass
```

추가로 `AsyncGenerator`는 비동기 제너레이터를 타입 힌트로 표현하는 방식입니다. `AsyncGenerator[dict[str, Any], None]`는 `yield`(제너레이터 반환 키워드)로 반환하는 값의 타입은 `dict[str, Any]`, 제너레이터 내부로 주입하는 값의 타입이 `None`이라는 뜻입니다.

추상 클래스와 비슷한 방식으로 작동하는 문법으로 `Protocols`이 존재하며 이는 클래스 상속이 아닌 덕타이핑 방식으로 작동합니다. 자세한 내용은 [문서](https://typing.python.org/en/latest/spec/protocol.html)를 참조해주세요.

### 결과

저는 ABC를 선택하였습니다. 물론 `Protocols`가 편리한 덕 타이핑을 제공하지만, 이 프로젝트에서는 `Extractor(ABC)`처럼 명시적인 상속을 사용하는 것이 TDD의 서브 클래스 검증이나 엄격한 타입 추론에서 안정적이고 명확하다는 판단 때문입니다.

이 과정을 통해 Python에서의 추상 클래스 구현에 대해 자세히 알아볼 수 있었습니다. 추상 클래스는 특정 규모 이상의 소프트웨어를 설계할 때 빠지지 않는 존재임을 체감했습니다. 추상클래스는 또한 의존성 역전을 구현하는 가장 명확한 방법이기도 합니다.

현재 ELT의 핵심 클래스들을 추상 클래스를 통해 안정적으로 구현하기 위한 과정에 있습니다.

## [Design] AuthProvider의 책임 범위

2025-11-02

### 문제

> [!WARNING]
> 환경 변수 등을 관리하는 인증 클래스는 어디까지 책임을 가지고 있는가?

ELT의 첫 모듈인 `Extractor`를 구현하던 중, Extractor는 인증을 필요로 한다는 사실을 깨달았습니다.

IGDB는 `Client ID`와 `Client Secret`을 통해 OAuth 토큰을 발급한 뒤, API 요청을 보내야 합니다. 두 값을 환경 변수로 설정한 상황에서 OAuth 토큰은 TTL이 설정되어 오기 때문에, 이를 관리할 새로운 클래스가 필요하며 이를 `AuthProvider`로 구현하기로 했습니다.

현재 상황에서 사용하는 방식은 복잡할 필요 없이 액세스 토큰을 직접 주입하고 반환하는 식으로 `AuthProvider` 인터페이스 - `StaticAuthProvider` 클래스 순서로 구현합니다. 이를 통해 현 코드 또한 테스트 코드와 의존성 없이 분리할 수 있고, 차후에 Redis와 같은 애플리케이션으로 실시간 토큰 발급과 재사용 여부를 확인할 수 있는 복잡한 Provider를 구현하게 되더라도 Extractor의 코드를 변경할 필요가 없어집니다.

하지만 client_id와 같은 값은 언제 넣어야 하는 걸까요? 실제 AuthProvider의 책임 범위는 어디까지여야 할지 궁금해졌습니다.

### 해결

`AuthProvider`는 토큰만 제공하는 것이 좋겠다고 결론내렸습니다.

일단 client_id와 같은 값은 IGDB API 설정이지, 인증 정보가 아닙니다. 그리고 `AuthProvider`에 더 많은 값을 두게 될 수록 해당 모듈의 의존성과 결합도가 높아지기 때문에 일반적으로도 여러 값을 관리하는 것은 바람직하지 않은 방향이기도 합니다.

그리고 본래 client_id는 환경 변수에 설정이 되어 있으므로 모듈이 환경 변수를 몰라도 되며 실제 호출 코드에서 지정해주는 것이 자연스럽지요.

### 결과

일단 모든 테스트를 GREEN으로 만든 후에 client_id를 `AuthProvider` 부분에서 삭제함으로서, Extractor는 직접 client_id를 받게 됩니다. 그렇게 테스트 코드에서도 완전한 의존성 분리와, 해당 테스트 코드에서 테스트 해야만 하는 부분을 좀 더 쉽게 인식할 수 있었습니다.

## [Architecture] Loader의 설계

2025-11-03

### 문제

> [!WARNING]
> Loader의 두 가지 방식: list vs generator

처음엔 `Loader`의 인터페이스를 다음과 같이 설계했습니다.

```python
class Loader(ABC):
    """
    Loader 인터페이스.

    이 인터페이스는 비동기적으로 데이터를 로드하는 메서드를 정의합니다.
    """

    @abstractmethod
    async def load(self, data: list[dict[str, Any]], key: str) -> None:
        """
        데이터 배치를 'key'라는 이름으로 Data Lake에 적재합니다.

        Args:
            data (list[dict[str, Any]]): Extractor가 생성한 데이터 배치.
            key (str): S3 등 데이터가 적재될 위치를 나타내는 키.
        """
        raise NotImplementedError
        return None

```

그러나 코드를 보던 중 이런 의문이 들었습니다.

> `data`가 배치(Batch)가 아니라면?

이렇게 구현하면 예상되는 문제점들은 다음과 같습니다.

```plain
❌ 메모리 압박: 10만 개 데이터를 list로 받으면 메모리 부족
❌ 유연성 부족: 스트리밍 처리 불가능
❌ Extractor와 불일치: Extractor는 AsyncGenerator인데 Loader는 list
```

그렇게 이 문제들을 해결하기 위해 제시된 대안은 **스트리밍** 방식으로 `Loader`역시 `AsyncGenerator`를 이용해 좀 더 유연한 방식으로 대체하는 것이었습니다. 과연 둘의 트레이드 오프를 고려했을 때 `Loader`의 알맞은 구조는 무엇일까요?

### 해결

저의 결론은 `list` 인터페이스가 더 실용적인 설계라는 것이었습니다.

일단 제 파이프라인에서 `Loader`는 S3와 같은 파일 기반의 데이터 레이크에 적재를 목적으로 하고 있습니다. 만약 스트리밍 방식으로 이를 구현하게 되면 Extract 단계를 포함해 O(1)의 메모리를 사용하면서 파이프라인 전체가 흐르는-스트리밍되며 좋은 설계 구조가 되겠지만, `Loader`의 복잡도는 매우 높아지게 됩니다.

예를 들어 S3는 파일 단위이며, 스트림을 파일로 만들기 위해서는 `Loader`내부에 업로드 로직이나 내부에서 배치 처리를 직접 구현해야 하는 복잡성이 추가됩니다. 이는 결정을 인터페이스 구현체에게 맡기는 유연하고 확장적인 구조이며 이로 인해 추출된 데이터들을 여러 방식으로 Load할 수 있다는 장점이 존재하지만, 제가 구현해야 할 작은 크기의 파이프라인에서는 S3 외에 당장 구현할 필요도 없습니다.

더해서 데이터 삽입 시 부과되는 S3의 요금을 생각하면 `Loader` 내부의 배치 처리가 강제되고, 그렇지 않으면 요금 폭탄을 맞게 되는 이지선다에 놓이게 됩니다.

### 결과

S3를 데이터 레이크로 결정한 이상 list 형태로 `Loader`를 구현하는 것으로 합니다. 하지만 이 방식에도 풀리지 않은 문제가 없는 것은 아닌데요. 예를 들어 "메모리의 압박은 어떻게 할 것인가?" 가 남아있습니다.

이에 대한 결론은 '"Orchestrator"의 책임으로 구분하는 것이 좋다'입니다. `Loader`의 책임은 배치를 파일로 변환하는 것입니다. 그리고 `Orchestrator`의 책임은 스트림을 배치로 만드는 것이죠. 만약 메모리에서 처리할 수 있는 배치의 크기를 `Orchestrator`에서 제어할 수 있다면 이 문제는 해결할 수 있습니다 예를 들어 다음과 같이 설계합니다.

```python
async def run_pipeline():
    ...
    batch = []
    BATCH_SIZE = 1000  # <-- 메모리 한계를 1000개로 제어

    # 파이프라인 시작
    async for item in extractor.extract():
        batch.append(item)

        # 배치가 차면 Loader 호출
        if len(batch) >= BATCH_SIZE:
            key = f"raw/games/{uuid.uuid4()}.jsonl"
            await loader.load(batch, key) # <-- list(배치) 전달
            batch.clear() # 메모리 해제

    # 만약 배치가 남았다면
    if batch:
        key = f"raw/games/{uuid.uuid4()}.jsonl"
        await loader.load(batch, key)

    # 파이프라인 종료
```

이러면 메모리의 압박에서 벗어남은 물론, 다른 부가 처리 없이 `extractor`만으로 파이프라인이 스트리밍으로 동작하게 됩니다.

## [CI] S3 통합 테스트 CI 환경 `AccessDenied`

2025-11-04

### 문제

> [!WARNING]
> Github Actions에서의 S3 접속 에러가 생긴다

ELT 파이프라인의 "L"의 기초 구현을 마무리하고, 통합 테스트를 작성하였습니다. 단위 테스트와는 다르게 S3 상태에 의존하고 있기 때문에, 환경 변수를 비롯해 꼼꼼한 설정이 필요합니다. 이때 테스트 작성 흐름은 다움과 같습니다.

- S3 세션 생성하기
  - 비동기 세션 사용 (~~boto3~~ -> **aioboto3**)
- S3 버킷, AWS 액세스 키, AWS 액세스 시크릿 불러오기
  - 이 단계에서 이미 AWS 서비스에서 IAM을 생성하고, 이 파이프라인 전용 버킷을 만든 뒤 최소 권한을 설정해주어야 합니다.
- 새로운 `jsonl` 테스트 파일을 생성하고, 테스트 데이터를 작성한 뒤 `S3Loader`로 실제 S3 세션에 접속해 업로드합니다.
- 업로드 후 `response`를 읽어 실제로 데이터가 S3 버킷에 쓰여졌는지 확인합니다.
- 모든 테스트가 종료되면 업로드된 파일을 삭제합니다.

여기서는 비동기 세션, `yield`, try-finally 패턴 등 비동기적인 패턴을 안전하게 처리하기 위한 코드가 많았습니다.

통합 테스트를 로컬에서 성공한 것을 확인하고 Github Secret에 환경 변수를 설정 후, Github Actions에 해당 브랜치를 머지하기 하기 위해 PR을 만들었습니다. PR 단계에서 실행된 CI 파이프라인에서 실패가 발생했고, 로그를 확인해보니 `AccessDenied` 에러를 반환하였습니다.

권한 거부 에러는 보통 AWS IAM에서 발생하는 경우가 많기 때문에 이 부분을 확인했으나, 로컬과 같은 IAM Key를 사용했고 로컬에서는 아무 문제가 없었기 때문에 다른 문제가 원인이 되었을 가능성이 높았습니다.

### 해결

첫 번째로 CI 로그에 디버깅 단계를 추가하고, `Arn`을 로컬과 비교했으나 모두 일치했습니다. 이는 인증이 성공했음을 의미합니다.

두 번째로 S3 버킷의 이름을 확인하였습니다. Github Secret을 다시 재수정하고, CI 로그에서 버킷 이름을 출력하는 디버깅 단계를 추가하였습니다. 로그에 `***`으로 마스킹되었고, 이는 올바르게 Secret을 로드하고 있음을 의미합니다.

마지막으로 환경 변수를 비롯해 TDD 코드와 IAM 정책에 아무런 문제가 없음을 증명하고자 Actions에서 직접 `awscli`를 이용해 S3에 파일을 직접 올리고 이를 CI 로그에 디버깅하는 테스트를 진행하였습니다. 이 디버깅이 성공하였고 결국 문제는 코드의 `aioboto3`에 있었습니다.

결과적으로는 `aioboto3`의 `region_name`을 명시적으로 주입하지 않아 버킷을 찾지 못하는 것이었고, 이를 아래와 같이 수정하니 해결되었습니다.

```python
@pytest.fixture(scope="function")
async def s3_client():
    """실제 aioboto3 S3 클라이언트 세션을 생성합니다."""
    region = os.getenv("AWS_DEFAULT_REGION")

    session = aioboto3.Session(region_name=region)
    async with session.client("s3", region_name=region) as client:
        yield client
```

이 이후에도 다른 문제로 AWS의 최종 일관성 문제가 발생시키는 간헐적인 오류가 발생하였습니다. 이는 `Gemini`에 따르면 `PutObject`후 바로 `Getobject`를 시도하여 (최종 일관성 모델을 따르는 S3에서는) 간헐적으로 실패가 생기고, 이때 AWS SDK에서 해당 파일이 있는지 확인하기 위해 내부적으로 `ListBucket`을 시도하다가 권한 문제로 실패하면서 생기는 에러였습니다. IAM 권한에 `ListBucket`권한을 해당 버킷에 추가하여 최종 일관성 문제를 해결하였습니다.

### 결과

IAM 정책에 `s3:ListBucket` 권한을 추가하여 CI의 Flaky Test(간헐적 실패)를 최종적으로 해결하였습니다.

근본적인 원인은 `aioboto3`에서 region_name을 채워주지 않아서였습니다. 거기에 까다로운 에러까지 겹쳐 평소에는 할 일이 없기도 하고 디버깅하기도 까다로운 CI 파이프라인을 디버깅하는 방법에 대해서 조금이나마 찾아볼 수 있었습니다.

더해서 S3의 최종 일관성에 대해 알아볼 수 있었습니다. S3 노드 간에는 데이터 전파 지연이 생기고 데이터를 업로드하는 즉시 확인하면 Not Found가 생길 수도 있다는 점, 그리고 그 후에 AWS에서 자동적으로 ListBucket을 시도한다는 점도 흥미로웠습니다.

현재는 Loader를 마무리하고 `EL` 두 단계를 E2E 테스트를 통해 검증하고, T로 넘어가도록 하겠습니다.

## [Design] TDD 사이클과 리팩토링

2025-11-05

### 문제

> [!WARNING]
> TDD 사이클에서 리팩토링의 적절한 기준이란?

TDD 사이클에 따라 기본적인 `EL`을 E2E 테스트까지 진행한 후의 다음 작업은 N+1 문제를 해결하는 것이었습니다.

IGDB API의 game 엔드포인트 응답은 거의 모든 item 항목이 정수로 되어 있습니다. 아마 모든 데이터가 정규화 원칙에 따라 잘 구조화되어 있는 것이겠지요. 하지만 모든 데이터를 추출해야 하는 저는 결국 game 테이블 외에 다른 모든 테이블들을 추출해야 하는 입장에 놓이고 말았습니다. 결국 게임 데이터 1개를 가져오기 위해서는 다른 테이블들(N개)를 참조해야만 하는 N+1 문제가 생긴 것입니다.

저는 platform, genre 등 주요 테이블에 대한 Extractor를 추가하는 것을 시작으로 했습니다. 모든 테이블을 서둘러 추가하지 않는 이유는 결국 이 파이프라인의 핵심은 파이프라인의 구조적인 비즈니스 로직이지, 테이블 하나 하나가 아니기 때문입니다.

같은 방식으로 테스트 코드를 작성한 뒤, 모든 테스트를 통과하는 Extractor를 만들었지만 이제 여기서 중복되는 코드가 생기기 시작합니다. 관측 가능성을 높이는 로그 시스템을 넣기 적합한 타이밍(현재의 코드 베이스가 아직은 적기 때문에)이기도 하기 때문에 기능 개발이 아닌 이런 모든 리팩토링을 TDD 개발 사이클에서 언제 진행해야 하는지 고민이 되었습니다.

### 해결

TDD에서 `REFACTOR` 단계는 다음과 같이 정의합니다.

> 새로운 기능 추가 없이, 테스트가 GREEN을 유지하는 상태에서 코드 구조를 개선하는 것.

이 원칙에 따르면 기능을 추가하거나 수정하는 것이 아닌 중복 코드를 묶어주는 행위는 `REFACTOR` 단계에서 진행할 수 있습니다. 로그 또한 테스트를 변경해야 하는 기능 추가 단위의 코드 수정이 아니기 때문에 이 단계에서 진행하는 것이 옳겠죠.

게다가 중복되는 코드의 발생은 오히려 좋은 징조일 수 있습니다. 이 후에 추가할 테이블들을 추출하는 Extractor들은 대부분 같은 코드를 공유하게 될 것이기 때문입니다. 게다가 로깅을 비롯해 Retry(재시도) 로직 등, Extractor 간에 공통적으로 가져가야 할 로직이나 라이브러리 추가와 같은 코드 수정을 하나의 코드로 통일한다면 훨씬 효율적으로 구조화할 수 있습니다.

따라서 저는 이 브랜치(`feature/platform-extractor`)에서 `BaseIgdbExtractor` 클래스를 생성해 모든 공통 로직을 포함하고, 달라지는 부분 (엔드포인트, 응답 개수) 등을 클래스 변수로 정의하도록 리팩토링했습니다. 달라진 코드는 다음과 같습니다.

```python
# src/pipeline/extractors.py

# (REFACTOR) 공통 로직이 Base 클래스로 이동
class BaseIgdbExtractor(Extractor):
    _API_URL: str
    _BASE_QUERY: str = "fields *;"
    _LIMIT: int = 500

    def __init__(...):
        # (공통 __init__)

    async def extract(...):
        # (공통 페이징, 인증, 로깅, 에러 핸들링 로직)

# (REFACTOR) 자식 클래스들은 설정만 남김
class IgdbExtractor(BaseIgdbExtractor):
    _API_URL = "[https://api.igdb.com/v4/games](https://api.igdb.com/v4/games)"
    _LIMIT = 500

class IgdbPlatformExtractor(BaseIgdbExtractor):
    _API_URL = "[https://api.igdb.com/v4/platforms](https://api.igdb.com/v4/platforms)"
    _LIMIT = 50
```

### 결과

리팩토링 후 `uv run pytest`를 실행했을 때, 모든 테스트가 [GREEN]임을 확인하면 완료입니다.

다음 차원을 추가하는 작업 또한 굉장히 간단해졌고, 테스트 코드 또한 해당 원칙을 이용해 중복 코드를 생략하는 방식으로 접근할 수 있습니다. 다만 테스트 코드의 경우, 테스트의 목적인 명확성을 위해 테스트 과정이 아닌 `conftest.py`의 fixture를 통해 데스트 셋업의 중복 코드를 줄이는 것을 목표로 합니다.

TDD에서 리팩토링은 수시로 해줘야 하는 것 같습니다. 최소 기능 단위로 개발하고, 이를 테스트로 검증하면서 수시로 리팩토링하는 접근법 또한 실용성이 돋보이는 것 같습니다.
