# 05. 데이터 모델 명세

이 문서는 프로젝트의 데이터 구조와 각 레이어별 데이터 모델을 설명합니다.

## 📋 목차

1. [데이터 레이어 개요](#데이터-레이어-개요)
2. [Raw Layer (S3 JSONL)](#raw-layer-s3-jsonl)
3. [Staging Layer (dbt Models)](#staging-layer-dbt-models)
4. [Mart Layer (Final Tables)](#mart-layer-final-tables)
5. [데이터 플로우](#데이터-플로우)

---

## 데이터 레이어 개요

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

---

## Raw Layer (S3 JSONL)

### 저장 구조

```
raw/
├── dimensions/             # Dimension 테이블 (스냅샷 방식)
│   ├── platforms/
│   │   ├── batch-0.jsonl   # 최신 전체 스냅샷
│   │   └── _manifest.json  # 파일 목록 메타데이터
│   ├── genres/
│   ├── game_modes/
│   ├── themes/
│   └── player_perspectives/
│
└── games/                   # Fact 테이블 (증분 방식)
    ├── dt=2025-11-15/
    │   ├── batch-0.jsonl    # 해당 날짜 증분 데이터
    │   └── _manifest.json
    └── dt=2025-11-16/
        ├── batch-0.jsonl
        └── _manifest.json
```

### IGDB Endpoint → Raw Layer 매핑

| IGDB Endpoint          | Raw Layer 경로                               | 저장 방식 | 업데이트 주기 |
| ---------------------- | -------------------------------------------- | --------- | ------------- |
| `/games`               | `raw/games/dt=YYYY-MM-DD/*.jsonl`            | 증분      | 일일          |
| `/platforms`           | `raw/dimensions/platforms/*.jsonl`           | 스냅샷    | 주/월         |
| `/genres`              | `raw/dimensions/genres/*.jsonl`              | 스냅샷    | 주/월         |
| `/game_modes`          | `raw/dimensions/game_modes/*.jsonl`          | 스냅샷    | 주/월         |
| `/themes`              | `raw/dimensions/themes/*.jsonl`              | 스냅샷    | 주/월         |
| `/player_perspectives` | `raw/dimensions/player_perspectives/*.jsonl` | 스냅샷    | 주/월         |

### Raw 데이터 스키마 예시

#### Games (raw/games/dt=_/batch-_.jsonl)

```json
{
  "id": 123456,
  "name": "The Legend of Zelda: Breath of the Wild",
  "slug": "the-legend-of-zelda-breath-of-the-wild",
  "summary": "Step into a world of discovery...",
  "game_type": 0,
  "parent_game": null,
  "cover": 87373,
  "url": "https://www.igdb.com/games/the-legend-of-zelda-breath-of-the-wild",
  "checksum": "abcd1234-5678-90ef",
  "genres": [12, 31],
  "platforms": [130, 6],
  "game_modes": [1],
  "player_perspectives": [3],
  "themes": [17, 38],
  "game_engines": [120],
  "keywords": [100, 200],
  "first_release_date": 1488499200,
  "release_dates": [10001, 10002],
  "screenshots": [50001, 50002],
  "websites": [3001, 3002],
  "created_at": 1234567890,
  "updated_at": 1700000000
}
```

#### Platforms (raw/dimensions/platforms/batch-\*.jsonl)

```json
{
  "id": 130,
  "name": "Nintendo Switch",
  "abbreviation": "Switch",
  "alternative_name": "NS",
  "generation": 8,
  "created_at": 1234567890,
  "updated_at": 1700000000
}
```

---

## Staging Layer (dbt Models)

### 모델 구조

```
models/
├── staging/
│   ├── dimensions/
│   │   ├── stg_platforms.sql
│   │   ├── stg_genres.sql
│   │   ├── stg_game_modes.sql
│   │   ├── stg_themes.sql
│   │   └── stg_player_perspectives.sql
│   │
│   ├── facts/
│   │   └── stg_games.sql
│   │
│   └── bridge/
│       ├── stg_game_platform_bridge.sql
│       ├── stg_game_genre_bridge.sql
│       ├── stg_game_mode_bridge.sql
│       ├── stg_game_theme_bridge.sql
│       └── stg_game_perspective_bridge.sql
│
└── marts/
    └── dim_games.sql (최종 테이블)
```

### Staging Models 상세

#### 1. Dimension Staging Models

**특징**:

- Manifest 기반으로 최신 전체 스냅샷 읽기
- 중복 제거 불필요 (이미 전체 데이터)
- `get_dimension_path()` 매크로 사용

**예시: stg_platforms.sql**

```sql
{{ config(materialized='ephemeral') }}

SELECT * FROM read_json_auto(
    {{ get_dimension_path("platforms") }},
    ignore_errors = true
)
```

**컬럼 구조**:
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Platform ID (Primary Key) |
| name | VARCHAR | 플랫폼 이름 (예: "Nintendo Switch") |
| abbreviation | VARCHAR | 약어 (예: "Switch") |
| alternative_name | VARCHAR | 대체 이름 |
| generation | INTEGER | 세대 (예: 8) |
| created_at | BIGINT | 생성 시간 (Unix timestamp) |
| updated_at | BIGINT | 업데이트 시간 (Unix timestamp) |

#### 2. Fact Staging Model

**특징**:

- 오늘 날짜 파티션만 읽기 (증분)
- `get_partition_path()` 매크로 사용
- Manifest 기반 파일 목록 로드

**예시: stg_games.sql**

```sql
{{ config(materialized='table') }}

WITH raw_games AS (
  SELECT * FROM read_json_auto(
    {{ get_partition_path("games") }},
    ignore_errors = true
  )
),

deduplicated_games AS (
  SELECT *
  FROM raw_games
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY updated_at DESC
  ) = 1
)

SELECT
  id, name, slug,
  COALESCE(summary, '') AS summary,
  game_type, parent_game, cover,
  url, checksum,
  created_at, updated_at,
  genres, platforms, game_modes,
  player_perspectives, themes,
  game_engines, keywords,
  first_release_date, release_dates,
  screenshots, websites
FROM deduplicated_games
WHERE name IS NOT NULL
```

**컬럼 구조**:
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Game ID (Primary Key) |
| name | VARCHAR | 게임 제목 |
| slug | VARCHAR | URL 친화적 식별자 |
| summary | VARCHAR | 게임 요약 (빈 문자열 기본값) |
| game_type | INTEGER | 게임 타입 |
| parent_game | INTEGER | 부모 게임 ID (DLC, Expansion 등) |
| cover | INTEGER | 커버 이미지 ID |
| url | VARCHAR | IGDB URL |
| checksum | VARCHAR | 데이터 체크섬 |
| created_at | BIGINT | 생성 시간 (Unix timestamp) |
| updated_at | BIGINT | 업데이트 시간 (Unix timestamp) |
| genres | INTEGER[] | 장르 ID 배열 |
| platforms | INTEGER[] | 플랫폼 ID 배열 |
| game_modes | INTEGER[] | 게임 모드 ID 배열 |
| themes | INTEGER[] | 테마 ID 배열 |
| player_perspectives | INTEGER[] | 플레이어 시점 ID 배열 |
| game_engines | INTEGER[] | 게임 엔진 ID 배열 |
| keywords | INTEGER[] | 키워드 ID 배열 |
| first_release_date | BIGINT | 첫 출시일 (Unix timestamp) |
| release_dates | INTEGER[] | 출시일 ID 배열 |
| screenshots | INTEGER[] | 스크린샷 ID 배열 |
| websites | INTEGER[] | 웹사이트 ID 배열 |

#### 3. Bridge Tables

**목적**: Many-to-Many 관계를 1:N으로 변환

**예시: stg_game_platform_bridge.sql**

```sql
{{ config(materialized='table') }}

WITH raw_games AS (
  SELECT * FROM {{ ref('stg_games') }}
)

SELECT
  id AS game_id,
  UNNEST(platforms) AS platform_id
FROM raw_games
WHERE platforms IS NOT NULL
```

**생성되는 Bridge Tables**:

- `stg_game_platform_bridge`: game_id ↔ platform_id
- `stg_game_genre_bridge`: game_id ↔ genre_id
- `stg_game_mode_bridge`: game_id ↔ game_mode_id
- `stg_game_theme_bridge`: game_id ↔ theme_id
- `stg_game_perspective_bridge`: game_id ↔ perspective_id

---

## Mart Layer (Final Tables)

### dim_games (최종 분석 테이블)

**목적**: 모든 게임 정보 + Dimension 데이터 통합

**생성 방식**:

1. 오늘 증분 데이터 (`stg_games`) + 전체 Dimension 데이터 JOIN
2. CloudFront에서 기존 Parquet 읽기
3. 증분 + 기존 UNION → 중복 제거
4. S3에 Parquet 덮어쓰기

**스키마**:

| Column Name        | Type      | Description                           | Source                       |
| ------------------ | --------- | ------------------------------------- | ---------------------------- |
| game_id            | INTEGER   | 게임 고유 ID (PK)                     | stg_games.id                 |
| game_name          | VARCHAR   | 게임 제목                             | stg_games.name               |
| game_slug          | VARCHAR   | 게임 URL 슬러그                       | stg_games.slug               |
| game_summary       | VARCHAR   | 게임 요약                             | stg_games.summary            |
| platform_names     | VARCHAR[] | 플랫폼 이름 배열                      | Bridge + Dimension JOIN      |
| genre_names        | VARCHAR[] | 장르 이름 배열                        | Bridge + Dimension JOIN      |
| game_mode_names    | VARCHAR[] | 게임 모드 이름 배열                   | Bridge + Dimension JOIN      |
| theme_names        | VARCHAR[] | 테마 이름 배열                        | Bridge + Dimension JOIN      |
| perspective_names  | VARCHAR[] | 시점 이름 배열                        | Bridge + Dimension JOIN      |
| url                | VARCHAR   | IGDB URL                              | stg_games.url                |
| cover              | INTEGER   | 커버 이미지 ID                        | stg_games.cover              |
| first_release_date | BIGINT    | 첫 출시일 (Unix timestamp)            | stg_games.first_release_date |
| created_at         | BIGINT    | 생성 시간 (Unix timestamp)            | stg_games.created_at         |
| updated_at         | BIGINT    | 마지막 업데이트 시간 (Unix timestamp) | stg_games.updated_at         |

**중요 사항**:

- ID 배열은 수집하지 않음 (이름 배열만 저장)
- 중복 제거: `game_id` 기준, `updated_at` DESC로 최신 레코드 선택
- 증분 업데이트: 기존 Parquet + 오늘 증분 UNION → 중복 제거

**저장 위치**: `s3://bucket/marts/dim_games/dim_games.parquet`

**예시 데이터**:

```
game_id: 233
game_name: "Half-Life 2"
game_slug: "half-life-2"
game_summary: "1998. HALF-LIFE sends a shock through..."
platform_names: ["Xbox 360", "PlayStation 3", "PC (Microsoft Windows)", ...]
genre_names: ["Shooter"]
game_mode_names: ["Single player"]
theme_names: ["Action", "Science fiction"]
perspective_names: ["First person"]
url: "https://www.igdb.com/games/half-life-2"
cover: 77288
first_release_date: 1100563200
created_at: 1300349787
updated_at: 1763234106
```

---

## 데이터 플로우

### 증분 업데이트 (Incremental Mode)

```
Day 1:
  Raw: games 340,000개 (전체)
  Staging: stg_games 340,000개
  Mart: dim_games 340,000개

Day 2:
  Raw: games 150개 (증분)
  Staging:
    - stg_games: 150개 (오늘 증분)
    - stg_platforms: 15개 (전체, 변경 없음)
  Mart: dim_games 340,150개 (150 + 340,000 병합)

Day 3:
  Raw: games 100개 (증분)
  Staging:
    - stg_games: 100개 (오늘 증분)
    - stg_platforms: 15개 (전체)
  Mart: dim_games 340,250개 (100 + 340,150 병합)
```

### Full Refresh (주 1회)

```
Saturday:
  1. 기존 파일 태그 변경: status=final → status=outdated
  2. 전체 데이터 재수집
  3. Manifest 교체 (새 파일만 가리킴)
  4. Mart 전체 재생성
  5. 7일 후: status=outdated 파일 자동 삭제
```

---

## 참고 자료

- **IGDB API 문서**: https://api-docs.igdb.com/
- **dbt 모델**: `transform/models/`
- **스키마 정의**: `transform/models/schema.yml`

_최종 업데이트: 2025.11.24_
