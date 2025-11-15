-- 게임-게임모드 다대다 관계 브릿지 테이블

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
  UNNEST(game_modes) AS game_mode_id
FROM raw_games
WHERE game_modes IS NOT NULL
  AND len(game_modes) > 0
