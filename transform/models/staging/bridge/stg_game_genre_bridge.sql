-- 게임-장르 다대다 관계 브릿지 테이블

-- 배열 필드를 행으로 펼쳐서 관계형 데이터로 변환
{% if target.name == 'dev_local_tdd' %}
WITH raw_games AS (
  SELECT * FROM read_json_auto('seeds/igdb_games_mock.jsonl')
)
{% else %}
WITH raw_games AS (
  SELECT * FROM {{ ref('stg_games') }}
)
{% endif %}

SELECT
  id AS game_id,
  UNNEST(genres) AS genre_id  -- 배열을 행으로 펼치기
FROM raw_games
WHERE genres IS NOT NULL
  AND len(genres) > 0  -- 빈 배열 제외
