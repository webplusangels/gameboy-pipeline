# 🚀 프로젝트 개발 워크플로우 (TDD 기반)

이 문서는 TDD(테스트 주도 개발) 방법론을 따르는 데이터 파이프라인의 전체 개발, 커밋, 병합 워크플로우를 정의합니다. AI로 작성되었습니다.

## 1. 🎯 프로젝트 목표

작고(Small) 현대적인(Modern) IGDB 데이터 파이프라인을 구축합니다.

- **E (Extract):** IGDB API에서 데이터 추출
- **L (Load):** 원본 데이터를 S3 Data Lake에 JSONL 형식으로 적재
- **T (Transform):** S3의 Raw 데이터를 dbt + DuckDB를 사용해 정제된 Mart로 변환

### 개발 환경 요구사항

- **Python:** 3.11 이상 (3.11, 3.12, 3.13 모두 지원)
- **패키지 관리:** `uv` (빠른 의존성 설치 및 가상 환경 관리)
- **테스트:** `pytest` + `pytest-asyncio` + `pytest-mock`
- **린팅:** `ruff` (linter + formatter)
- **타입 체크:** `mypy` (strict mode)

## 2. 🏛️ 핵심 아키텍처 원칙

- **관심사 분리 (SoC):** Extractor, Loader, Transformer는 서로를 몰라야 합니다.
- **인터페이스 기반 설계:** 컴포넌트는 `src/pipeline/interfaces.py`의 추상 클래스(ABC) 또는 Protocol을 준수해야 합니다.
  - `Extractor`: Abstract Base Class로 정의, `async def extract(...)` 메서드 구현 강제
  - `Loader`: Protocol 또는 ABC로 정의 예정, `async def load(...)` 메서드 준수
- **의존성 주입 (DI):** `IgdbExtractor`는 HTTP 클라이언트를, `S3Loader`는 S3 클라이언트를 외부에서 주입받습니다.
  - 테스트에서는 `mocker.AsyncMock()`으로 의존성 모킹
  - 프로덕션에서는 실제 클라이언트(`httpx.AsyncClient`, `aioboto3` 등) 주입
  - 이는 테스트 용이성과 유연성을 극대화합니다.

---

## 3. 🧪 개발 방법론: TDD (Red-Green-Refactor)

모든 신규 기능은 TDD 사이클을 따릅니다.

1.  **🔴 RED: 실패하는 테스트 작성**

    - `tests/` 디렉터리에 새 기능에 대한 테스트 코드(`test_*.py`)를 작성합니다.
    - `mocker`를 사용해 외부 의존성(API, S3)을 철저히 모킹(Mocking)합니다.
    - `pytest`를 실행하여 **테스트가 예상대로 실패하는 것을 확인**합니다.

2.  **🟢 GREEN: 테스트를 통과하는 최소한의 코드 작성**

    - `src/` 디렉터리에 `RED` 단계의 테스트를 **겨우 통과할 만큼의 최소한의 코드**를 작성합니다.
    - `pytest`를 실행하여 **모든 테스트가 통과하는 것을 확인**합니다.

3.  **🟡 REFACTOR: 코드 개선**
    - 테스트가 통과하는 "안전망" 위에서 코드의 구조를 개선하고, 중복을 제거하며, 가독성을 높입니다.
    - 리팩토링 후에도 `pytest`를 실행하여 **모든 테스트가 계속 통과하는지 확인**합니다.

---

## 4. 💾 Git 커밋 및 브랜치 전략

TDD 사이클은 Git 커밋과 1:1로 매핑됩니다.

### A. 브랜치 전략

- `main` 브랜치는 **항상 모든 테스트를 통과하는(Always GREEN)** 상태여야 합니다.
- 모든 작업은 `main`에서 분기한 `feature/` 브랜치에서 수행합니다.

  ```bash
  git checkout main
  git pull
  git checkout -b feature/igdb-extractor
  ```

- `feature/` 브랜치는 TDD 사이클(`RED` → `GREEN` → `REFACTOR`)을 명시적으로 기록합니다.
- PR 병합 시 `main`으로 Squash and Merge하여 히스토리를 깔끔하게 유지합니다.

### B. TDD 커밋 전략

로컬 `feature/` 브랜치에서는 TDD 각 단계를 명시하는 커밋 프리픽스(Prefix)를 사용합니다.

1.  **`RED:`**

    - 실패하는 테스트 코드를 추가했을 때 사용합니다.
    - `git commit -m "RED: Add failing test for IgdbExtractor paging"`

2.  **`GREEN:`**

    - 테스트를 통과시키는 최소한의 코드를 추가했을 때 사용합니다.
    - `git commit -m "GREEN: Implement minimal paging logic in IgdbExtractor"`

3.  **`REFACTOR:`**
    - `GREEN` 이후 코드를 개선했을 때 사용합니다.
    - `git commit -m "REFACTOR: Clean up paging query builder"`

이러한 세분화된 커밋은 `feature/` 브랜치에만 존재하며, PR 리뷰 시 개발 과정을 명확하게 보여줍니다.

---

## 5. 🤖 CI 및 병합 (GitHub Actions)

`main` 브랜치의 품질을 유지하는 자동화 프로세스입니다.

### A. CI (Continuous Integration)

- **플랫폼:** GitHub Actions (`.github/workflows/ci.yml`)
- **도구:** `uv` (빠른 의존성 설치 및 가상 환경 관리)
- **트리거:** `main` 브랜치로의 `push` 또는 `pull_request`가 발생할 때마다 실행됩니다.

#### CI 파이프라인 구조 (병렬 실행)

**Job 1: `lint` (코드 품질 검증)**

- Python 3.11 단일 버전 사용
- 캐싱: `uv` 캐시 활성화로 의존성 재사용
- 검증 단계:
  1. `uv venv` + `uv sync --extra dev`로 개발 의존성 설치
  2. `uv run ruff check src tests` - 코드 품질 규칙 검증 (pycodestyle, pyflakes, isort 등)
  3. `uv run ruff format --check src tests` - 코드 포맷팅 일관성 검증
  4. `uv run mypy src` - 타입 안정성 검증 (strict mode)

**Job 2: `test` (테스트 실행)**

- Python 버전 매트릭스: **3.11, 3.12, 3.13** (병렬 실행)
- 캐싱: `uv` 캐시 활성화
- 테스트 단계:
  1. `uv venv` + `uv sync --extra dev`로 개발 의존성 설치
  2. 환경 변수 주입 (테스트용 더미 값):
     - `IGDB_CLIENT_ID`, `IGDB_CLIENT_SECRET`
     - `IGDB_RATE_LIMIT`, `LOG_LEVEL`
  3. `uv run pytest --cov=src --cov-report=xml` - 테스트 + 커버리지 수집
  4. Codecov 업로드 (Python 3.11에서만)

**실행 순서:**

- `lint`와 `test` job은 **병렬로 실행**되어 빠른 피드백 제공
- 린트 실패 시 즉시 확인 가능
- 각 Python 버전별 테스트도 병렬 실행 (총 3개 버전)

### B. 병합 전략 (Pull Request)

1. `feature/` 브랜치를 GitHub로 푸시(`git push origin feature/igdb-extractor`)하고 PR을 생성합니다.
2. **CI(GitHub Actions) 통과 확인:**
   - ✅ `lint` job: Ruff linter, formatter check, mypy 모두 통과
   - ✅ `test` job: Python 3.11/3.12/3.13 모든 버전에서 테스트 통과
   - ✅ 커버리지 리포트가 Codecov에 업로드됨
3. 코드 리뷰가 완료되면, **"Squash and Merge"** 옵션을 사용해 `main` 브랜치로 병합합니다.
4. **머지 커밋 메시지**는 `RED/GREEN/REFACTOR`이 아닌, **기능 단위의 커밋 메시지**로 새로 작성합니다.
   - 권장 포맷: [Conventional Commits](https://www.conventionalcommits.org/)
   - 예시:
     - `feat: Add IGDB Extractor with paging support`
     - `fix: Handle empty API response in IgdbExtractor`
     - `refactor: Improve S3 key naming logic in S3JsonLoader`
     - `docs: Update CONTRIBUTING.md with CI pipeline details`

---

## 🏁 전체 워크플로우 요약 (예: Loader 추가)

1. **브랜치 생성:** `git checkout -b feature/s3-json-loader`
2. **(RED)** `tests/pipeline/test_loader.py`에 `test_s3_loader_saves_data` 테스트 작성.
3. **(RED)** `pytest` 실행 -> **실패** 확인.
4. **(RED)** `git commit -m "RED: Add failing test for S3JsonLoader"`
5. **(GREEN)** `src/pipeline/loaders.py`에 `S3JsonLoader` 껍데기 코드 작성.
6. **(GREEN)** `pytest` 실행 -> **성공** 확인.
7. **(GREEN)** `git commit -m "GREEN: Implement minimal S3JsonLoader to pass test"`
8. **(REFACTOR)** `S3JsonLoader`의 `put_object` 호출 로직 리팩토링.
9. **(REFACTOR)** `pytest` 실행 -> **성공** 확인.
10. **(REFACTOR)** `git commit -m "REFACTOR: Clean up S3 key naming logic"`
11. **(반복)** `S3Loader`에 필요한 다른 기능(예: 에러 핸들링)도 TDD 사이클 반복.
12. **로컬 검증:** 푸시 전 로컬에서 CI와 동일한 검증 실행 (선택사항)

    ```bash
    # 린팅 및 포맷팅 검증
    uv run ruff check src tests
    uv run ruff format --check src tests
    uv run mypy src

    # 전체 테스트 실행
    uv run pytest --cov=src
    ```

13. **PR:** `git push origin feature/s3-json-loader` 후 PR 생성.
14. **CI 통과 대기:**
    - `lint` job: Ruff + mypy 통과 확인
    - `test` job: Python 3.11/3.12/3.13 모든 버전에서 테스트 통과 확인
    - 커버리지 리포트 확인 (Codecov 댓글 자동 생성)
15. **병합:** 리뷰 완료 후 "Squash and Merge" 클릭, 커밋 메시지 `feat: Add S3JsonLoader for raw data` 작성 후 병합.
