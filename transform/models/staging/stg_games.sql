{{
  config(
    materialized = 'table' if target.name == 'prod_s3' else 'view'
  )
}}

-- TDD 환경: 로컬 JSONL 파일에서 읽기
-- Prod 환경: S3 JSONL 파일에서 읽기
WITH raw_games AS (
  {% if target.name == 'dev_local_tdd' %}
  SELECT * FROM read_json_auto('seeds/igdb_games_mock.jsonl')
  {% else %}
  SELECT * FROM read_json_auto({{ get_partition_path("games") }})
  {% endif %}
),

deduplicated_games AS (
  SELECT *
  FROM raw_games
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY id 
      ORDER BY updated_at DESC -- 같은 ID가 있다면 최신 수정일 우선
  ) = 1
)

SELECT
  -- 기본 정보
  id,
  name,
  slug,
  
  -- 설명 (NULL 처리)
  COALESCE(summary, '') AS summary,
  
  -- 게임 타입 및 분류
  game_type,
  parent_game,
  
  -- 시각 자료 ID
  cover,
  
  -- 메타데이터
  url,
  checksum,
  created_at,
  updated_at,
  
  -- 관계형 데이터 (배열 필드 - 나중에 브릿지 테이블로 분리)
  genres,
  platforms,
  game_modes,
  player_perspectives,
  themes,
  game_engines,
  keywords,
  release_dates,
  screenshots,
  websites

FROM deduplicated_games
WHERE name IS NOT NULL  -- 이름 없는 게임 제외
