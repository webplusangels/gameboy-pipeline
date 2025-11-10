-- 게임-테마 다대다 관계 브릿지 테이블

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
  id AS game_id,
  UNNEST(themes) AS theme_id
FROM raw_games
WHERE themes IS NOT NULL
  AND len(themes) > 0
