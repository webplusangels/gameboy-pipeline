-- TDD 환경: 로컬 JSONL 파일에서 읽기
-- Prod 환경: S3 JSONL 파일에서 읽기
{% if target.name == 'dev_local_tdd' %}
WITH raw_games AS (
  SELECT * FROM read_json_auto('seeds/igdb_games_mock.jsonl')
)
{% else %}
WITH raw_games AS (
  SELECT * FROM read_json_auto('s3://{{ env_var("S3_BUCKET_NAME", "placeholder-bucket") }}/raw/games/*.jsonl')
)
{% endif %}

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

FROM raw_games
WHERE name IS NOT NULL  -- 이름 없는 게임 제외
