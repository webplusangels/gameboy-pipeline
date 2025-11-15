-- 게임-플랫폼 다대다 관계 브릿지 테이블

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
  UNNEST(platforms) AS platform_id
FROM raw_games
WHERE platforms IS NOT NULL
  AND len(platforms) > 0
