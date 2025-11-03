# 03. 학습 및 문제 해결 로그

이 문서는 프로젝트를 진행하며 마주친 주요 기술적 문제와 이를 해결한 과정을 기록합니다. 실제 제가 고민한 문제들에 대해 순차적으로 기술하겠습니다.

<!-- [Architecture] - 설계 관련
[Testing] - 테스트 작성/실행
[CI/CD] - GitHub Actions
[Tooling] - uv, ruff, mypy
[Python] - 언어 자체 이슈 -->

## 1. [Architecture] TDD

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

## 2. [CI/CD] Github Actions CI 파이프라인

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

이번 작업을 배운 점은 다음과 같습니다.

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

이 과정을 통해 Python에서의 추상 클래스 구현에 대해 자세히 알아볼 수 있었습니다. 추상 클래스는 특정 규모 이상의 소프트웨어를 설계할 때 빠지지 않는 존재임을 체감했습니다. 추상클래스는 또한 의존성 역전을 구현하는 가장 명확한 방법이기도 합니다.

현재 ELT의 핵심 클래스들을 추상 클래스를 통해 안정적으로 구현하기 위한 과정에 있습니다.
