# 01. 프로젝트와 구조

이 문서는 프로젝트의 구조와 그에 관련된 문제를 다루는 문서입니다. 제가 느낀 문제점과 실제 문제를 어떻게 해결할지에 대한 고민을 중심으로 서술합니다.

## 0. 목차

[1. 왜 만들었는가?](#1-왜-만들었는가)

[2. 시스템 아키텍처](#2-시스템-아키텍처)

## 1. 왜 만들었는가?

제가 이전에 2개월간 개발한 [game-pricing-pipeline](https://github.com/webplusangels/game-pricing-pipeline)은 [WARA:B](https://github.com/100-hours-a-week/9-team-gudokjohayo-warab-be)(백엔드 레포) 프로젝트의 일부로 운영을 위한 데이터 관련 작업을 자동화하는 파이프라인 모듈입니다.

이 파이프라인은 서비스의 요구 사항인 한국어, PC 게임, 할인가 수집 등을 만족하기 위해 Steam API를 포함한 5-6개의 서드 파티 API들에게서 수집한 데이터들을 통합하여 설계한 데이터 ERD에 맞게 정제하고, 업서트 작업을 cron 스케줄링으로 자동화하여 구현하였습니다. 이 과정에서 주요 병목인 수집 과정을 최적화하기 위해 파일 시스템 기반의 기초적인 캐싱 시스템을 활용하고, python 기본 로그 라이브러리를 이용해 로깅하며 에러를 직접 확인하였습니다. 그 결과 실제로 수동 수집 기준 **1/6** 수준의 수집 시간 단축과, 운영 기간 동안의 데이터 수집 오류를 **0%**, 완전히 배제할 수 있었습니다.

그러나 파이프라인 개발이 처음인데다, 구조에 대한 충분한 고민 없이 짧은 시간 구현에 집중하다보니 개발 후에는 많은 한계점들이 발견되었습니다. 문제점들을 요약하자면 다음과 같습니다.

- ❌ 코드의 가독성과 낮은 유지 보수성
- ❌ 긴 실행 시간 (배치 작업 최적화 부족)
- ❌ 테스트 안정성 부재 (트러블슈팅 시간 증가)

사소한 오류가 긴 시간의 트러블 슈팅과 테스트로 이어지는 고통스러운 과정은, 소프트웨어 개발 이론에서만 배웠던 **낮은 결합도, 높은 응집도**의 필요성을 체감하게 했습니다. 이는 오류 해결을 위한 소프트웨어 유지 보수 시도 자체가 차단되는 수준의 심각한 문제점으로 이어지고 있었습니다.

현재와 같은 목표를 세우기 위해 여러 시행착오를 거치며 v2에 대한 구상을 이어나갔습니다. 데이터를 다루기 위해 **Python**, **헥사고날 아키텍처**, **파일 시스템 기반의 캐싱 시스템**, **Redis 캐싱 모듈**, **TDD** 등의 키워드들에 대해 깊게 공부하고, 개발했던 모듈의 근본적인 문제점들을 파악하며 현재 실무에서 사용되고 있는 기술들을 활용한 빠르고 현대적인 파이프라인의 구조에 대해 파악하였습니다. 그 중에서는 복잡성이 높은 아키텍처와 새로운 기술들을 도입하는 것보다는, 기술과 개념 그 자체에 고민할 시간을 가질 수 있는 충분히 단순하고 직관적인 방식을 우선으로 고려하였습니다.

결과적으로 현재 제가 개발하려는 파이프라인은 다음과 같은 목표를 가지고 있습니다.

### 1. 안정적인 진실 공급원

IGDB는 Twitch에서 운영중인 게임 관련 오픈 소스 API입니다. 이미 검증된 데이터베이스인 만큼 규모나 상업적인 이용에도 아무런 문제가 없고, 관련 데이터베이스 중 가장 안정적으로 공급될 가능성이 높은 API입니다.

이전 파이프라인에서는 Steam에서 서비스 목적인 PC 게임만을 데이터로 받았다면, 현재 구상 중인 파이프라인은 ELT, 즉 해당 API가 가지고 있는 풍부한 게임 관련 데이터에서 추출한 모든 데이터를 적재, 변형하는 명시적인 플로우를 통해 L과 T의 의존성을 줄이는 동시에, 데이터를 필요에 맞게 비교적 자유로운 형태로 변형할 수 있습니다.

### 2. 가볍고 현대적인 구조

Python의 추상 클래스를 이용한 인터페이스 기반 설계로 가볍고 현대적인 구조화를 구현하여, 이후 점진적인 개선 작업이 가능하도록 합니다.

이 프로젝트에서 장기적인 목표로 하는 것들은 결국 현재 활발히 사용되고 있는 도구들과의 성공적인 통합이고, 이를 위해 코어 로직과의 결합도가 보다 낮아질 필요가 있습니다. 이를 위해 추상 클래스로 설계도를 먼저 제시하면, 이를 구현하는 방식을 통해 의존성 역전을 실현할 수 있습니다. 이는 TDD의 핵심인 테스트 스위트를 원활하게 정의하는 데도 도움이 됩니다.

### 3. TDD 개발 방식

TDD는 다음과 같은 순서를 ([RED] -> [GREEN] -> [REFACTOR]) 반복하며 기능을 구현하는 개발 방식입니다. 필요한 기능만을 작성할 수 있기 때문에 코드의 단순함을 유지할 수 있고, 안정적인 코드 개선이 가능합니다. 특히 문제가 되었던 코드의 높은 결합도를 낮추는데 도움이 되고, 설계 단계에서 미리 문제점을 확인할 수 있습니다.

테스트의 초기 작성 시간이나 개발 효율, 그리고 러닝 커브와 같은 부분이 단점으로 작용할 수 있지만 제 프로젝트는 최대한 작은 규모를 유지하면서도 안정적인 설계를 필요로 하기 때문에 알맞은 방법이라고 생각했습니다.

### 4. 타입 안정성

Python은 기본적으로 타입을 강제하는 언어는 아니지만, 타입 시스템이 도입되면서 `FastAPI`와 같은 프레임워크에는 표준처럼 쓰이는 상황입니다.

제가 경험한 Python 개발 경험에 따르면, 타입 안정성은 개발 안정성에 큰 장점을 가지며 특히 테스트를 활용한 개발 경험에서 잘 맞는다고 생각했습니다. Meta, JetBrains, Microsoft 등에서 진행한 [설문조사](https://www.linkedin.com/posts/alexandrevs_typed-python-in-2024-well-adopted-yet-usability-activity-7298661426624110592-2vJC)에 따르면 Python 개발자의 88%가 타입 힌트를 자주 사용한다고 밝혔습니다. 제 생각에도 아마 이 부분은 지속적으로 늘어날 것이라고 예상합니다.

### 5. 데이터 기반 점진적 기능 향상

기능 향상은 최대한 데이터를 기반으로 합니다. 파이프라인의 효율성 향상에 대한 아이디어가 현재에도 여러가지가 있지만 이 부분에 대해서는 직접 실험해보고, 다양한 도구들을 사용해 보면서 점진적인 확장을 목표로 합니다. 이를 다룬 [문서](./04_Performance.md)입니다.

### 6. 체계적인 문서화

현재 작성 중인 문서와 같이 프로젝트에 기록을 남기고 현재 생각하고 있는 부분을 문서로 남기는 점은 학습에도 중요합니다. 이 프로젝트를 통해 실제 실습을 통한 추상화된 개념과 그 아래에 존재하는 컴퓨터 과학과 관련된 개념을 학습합니다. 진행 중에 생기는 질문들에 대해 최대한 문서화하여 프로젝트 전체를 쉽게 이해하고 성장에 밑거름이 될 수 있도록 합니다.

## 2. 시스템 아키텍처

### 아키텍처 다이어그램 (11.14.25)

![1114v1](./imgs/Pipeline%20Diagram%201114.png)

### 데이터 플로우

```
┌─────────────────────────────────────────────────────────────┐
│                       IGDB API                              │
│  /games, /platforms, /genres, /game_modes, etc.             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓ (Python EL Pipeline)
┌─────────────────────────────────────────────────────────────┐
│                    Raw Layer (S3)                           │
│  raw/dimensions/{entity}/*.jsonl  (스냅샷 방식)              │
│  raw/games/dt=YYYY-MM-DD/*.jsonl  (증분 방식)                │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓ (dbt + DuckDB)
┌─────────────────────────────────────────────────────────────┐
│                  Staging Layer (dbt)                        │
│  stg_games, stg_platforms, stg_genres, ...                  │
│  Bridge tables: stg_game_platform_bridge, ...               │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓ (dbt Transformations)
┌─────────────────────────────────────────────────────────────┐
│                   Mart Layer (S3 Parquet)                   │
│           dim_games.parquet (최종 분석용 테이블)              │
└─────────────────────────────────────────────────────────────┘
```

### 컴포넌트 배치

#### 개발 환경 (Local)

- **실행 위치**: 로컬
- **Transform 엔진**: DuckDB (임베디드 DB)
- **스토리지**: 로컬 파일 시스템 (`.db`)
- **용도**: TDD 개발, 단위 테스트 실행

#### 프로덕션 환경 (AWS)

- **EL 파이프라인**:
  - GitHub Actions (Ubuntu 러너)
  - Python 3.12 + uv 패키지 매니저
- **데이터 스토리지**:
  - S3 버킷: 원본 데이터 (JSONL), 변환 데이터 (Parquet)
  - CloudFront: S3 앞단 CDN (데이터 전송 비용(DTO) 절감)
- **Transform 엔진**:
  - dbt + DuckDB (GitHub Actions 내에서 실행)
  - Stateless 실행: 매번 S3에서 읽고 쓰기

#### 배포 흐름

```
로컬 개발 → Git Push → GitHub Actions 트리거 → EL 실행 → dbt Transform → S3 저장
```

**비용 최적화 전략**:

- CloudFront를 통해 S3 DTO(Data Transfer Out) 비용 99% 절감
- DuckDB의 경량성으로 별도 DB 인프라와 비용 불필요

## 3. 핵심 컴포넌트

### Extractor

#### 역할

IGDB API로부터 게임 관련 데이터를 추출하는 역할을 합니다.

#### 핵심 설계

- **추상 클래스 기반**: `BaseIgdbExtractor` 추상 클래스로 공통 로직 정의

- **인터페이스 분리**: `Extractor` 인터페이스를 통한 테스트 용이성

- **비동기 스트리밍**: `AsyncGenerator`로 대용량 데이터를 배치 단위로 생성

#### 구현 클래스

| 클래스명                         | 엔티티              | 추출 방식          |
| -------------------------------- | ------------------- | ------------------ |
| `IgdbExtractor`                  | games               | 증분 (Incremental) |
| `IgdbPlatformExtractor`          | platforms           | 전체 (Full)        |
| `IgdbGenreExtractor`             | genres              | 전체 (Full)        |
| `IgdbGameModeExtractor`          | game_modes          | 전체 (Full)        |
| `IgdbThemeExtractor`             | themes              | 전체 (Full)        |
| `IgdbPlayerPerspectiveExtractor` | player_perspectives | 전체 (Full)        |

#### 주요 기능

**1. 페이지네이션 처리**

```python
async def extract(self, is_full_refresh: bool = False, last_updated_at: int | None = None):
    offset = 0
    while True:
        batch = await self._fetch_page(offset)
        if not batch:
            break
        yield batch
        offset += self.limit
```

**2. 증분 추출**

- `games` 엔티티 해당
- `updated_at` 기준 변경된 데이터 추출
- 안전 마진을 적용해 클럭 스큐 방지

**3. 전체 추출**

- Dimension 엔티티 해당
- 데이터 규모가 작아 전체 추출이 효율적
- 매 실행마다 전체 데이터를 처리함

#### 에러 처리 (미완)

- 초당 4회 API Rate Limit을 준수
- HTTP 오류 시 로그 기록 후에 빈 배치를 반환
- 타임아웃 설정

#### 테스트 전략

- Mock API 응답으로 단위 테스트
- Fixture 데이터로 페이지네이션 검증
- 실제 응답 샘플인 `test_data/*.jsonl` 사용

### Loader

#### 역할

Extractor가 생성한 데이터 배치를 S3 버킷에 JSONL 형태로 적재합니다.

#### 핵심 설계

- **추상 클래스/인터페이스 분리**: `Loader` 인터페이스/추상 클래스를 통한 테스트 용이성

- **비동기 S3 클라이언트**: `aioboto3`로 비동기 S3 클라이언트를 사용해 비동기 처리

#### 구현 클래스

| 클래스명   | 용도      |
| ---------- | --------- |
| `S3Loader` | S3에 적재 |

#### 주요 기능

**1. JSONL 변환 및 업로드**

```python
async def load(self, data: list[dict], key: str) -> None:
    jsonl_data = "\n".join(json.dumps(item) for item in data)
    await self._s3_client.put_object(
        Bucket=self._bucket_name,
        Key=key,
        Body=jsonl_data,
        ContentType="application/x-jsonlines",
        Tagging="status=temp",  # 임시 파일 표시
    )
```

**2. 파일 경로 구조 구분**

- **Dimension**: `raw/dimensions/{entity}/batch-{uuid}.jsonl`

- **Fact**: `raw/games/dt=YYYY-MM-DD/batch-{uuid}.jsonl`

**3. 객체 태그 관리**

- `status=temp`: 적재 중인 파일
- `status=final`: 적재 완료 (Manifest에 등록됨)
- `status=outdated`: Full Refresh 시 이전 파일

**4. Manifest 파일**

- 적재된 파일 목록을 JSON으로 관리
- `dbt`에서 읽을 파일 목록 제공
- CloudFront 캐시 무효화에 활용

```json
{
  "files": [
    "raw/dimensions/platforms/batch-abc123.jsonl",
    "raw/dimensions/platforms/batch-def456.jsonl"
  ],
  "updated_at": 1732435200
}
```

#### 주요 특징

- **비동기 I/O**: 네트워크 I/O 시간 최소화
- **배치 단위 저장**: 메모리 효율성 - 50000개 단위
- **테스트 용이성**: S3에 접속하지 않고 단위 테스트 가능

### Transform

#### 역할

S3에 적재된 원본 데이터를 비즈니스 요구 사항에 맞게 변환하는 역할을 맡습니다.

#### 핵심 설계

- **`dbt`**: SQL 기반 데이터 변환 프레임워크를 사용
- **`DuckDB`**: 서버가 불필요한 경량 열 기반 데이터베이스
- 데이터 레이어
  - 1. Staging Layer: CloudFront에서 Manifest 읽고 JSONL 파싱
  - 2. Mart Layer: 비즈니스 로직을 적용하고 증분된 데이터를 기존 데이터와 병합해 최종적으로 S3에 Parquet 저장

#### 구현 전략

- Materialization: 구체화 전략으로 테스트 시에는 view 위주, 실제 프로덕션 환경에서는 table 위주
- Jinja Macro: 차원 데이터와 팩트(게임) 데이터는 파일 목록을 읽는 방식과 증분 방식이 다르기 때문에 각각의 매크로를 사용해 CloudFront 경로를 생성
- 환경 변수 주입: `env_var()`를 통해 동일한 환경 변수를 주입

#### 주요 특징

- **SQL 중심 개발**: Python 스크립트가 아닌 `dbt`의 SQL로 대부분 작성
- **증분 업데이트**: 전체 데이터 재수집-처리 하지 않음
- **버전 관리**: `dbt`는 변환 단계를 저장하기 때문에 스크립트를 `Git`으로 관리
- **테스트 용이**: `dbt`에서 지원하는 테스트를 통해 실행 후 테스트로 데이터 무결성을 확인

### Orchestration

#### 역할

EL 파이프라인과 Transform을 순차적으로 실행하고 상태를 관리합니다.

#### 핵심 설계

**1. 로컬 실행**

1. 상태 조회
2. 추출
3. 적재
4. Manifest 생성/업데이트
5. 상태 저장

**2. GitHub Actions 스케줄링**

- 매일 새벽에 실행
- 매주 월요일 새벽에 Full Refresh 후 기존 파일 Outdated 변경
- Full Refresh 옵션을 포함해서 수동 실행 가능

**3. 실행 순서 제어**

- Dimension 데이터를 먼저 준비한 후에 JOIN을 할 수 있도록 함
- 의존성 순서 보장

**4. 상태 관리**

- `S3StateManager`를 통해 각 엔티티가 업데이트된 시간을 저장
- 업데이트 시에 해당 엔티티의 state를 참고해 이후 데이터를 수집 시도

**5. Full Refresh**

1. 기존 파일 태그 변경 (`status=final` → `status=outdated`)
2. 전체 데이터 재추출
3. 새 파일 적재
4. Manifest 교체
5. S3 Lifecycle Policy로 7일 후 `outdated` 파일 자동 삭제

**6. 에러 핸들링**

- 각 엔티티별 독립 실행
- 실패 시 로그 기록 및 GitHub Actions 실패 표시
- 더 나은 핸들링 로직 필요

**7. 모니터링**

- **로그**: Loguru로 구조화된 로그 출력
- **메트릭**: 추출 레코드와 실행 시간 기록
- **향후**: 모니터링으로 전달되는 로그를 구조화하고, 웹훅 연동 가능

## 디렉토리 구조

```plain
gameboy-pipeline/
├── .github/
│   └── workflows/
│       ├── ci.yml              # CI 파이프라인
│       └── elt_pipeline.yml    # 파이프라인 스케줄링
├── src/
│   ├── pipeline/
│   │   ├── extractors.py       # Extract 레이어
│   │   ├── loaders.py          # Load 레이어
│   │   ├── auth.py             # API 토큰 제공자
│   │   ├── state.py            # 파이프라인 상태 관리자
│   │   └── interfaces.py       # 추상 클래스
│   └── config.py
├── transform/
│   ├── macros/
│   ├── models/
│   │   ├── staging/            # 원본 데이터 정규화
│   │   └── marts/              # 비즈니스 로직
│   ├── profiles.yml
│   └── dbt_project.yml
├── tests/
│   ├── pipeline/               # 파이프라인 테스트 파일
│   ├── test_data/              # 테스트 데이터
│   └── conftest.py             # 테스트 설정
├── scripts/
│   └── run_pipeline.py         # Orchestration
├── docs/
│   └── 00...md                 # 프로젝트 문서화
├── .env                        # 환경 변수
...
```
