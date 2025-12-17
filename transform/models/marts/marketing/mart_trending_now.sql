{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_trending_now.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- 🔥 지금 가장 핫한 게임 (Engagement Velocity 기반)
-- Playing / Played 비율이 높아 현재 활발하게 플레이되는 게임

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
)

SELECT
    g.game_name,
    p.engagement_velocity,
    p.playing,
    p.played,
    p.igdb_total_engagement,
    p.steam_positive_ratio,
    p.cross_platform_score,
    g.aggregated_rating,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN popularity_metrics p ON g.game_id = p.game_id
WHERE p.engagement_velocity IS NOT NULL
  AND p.played >= 0.0001  -- 최소 임계값 이상 플레이한 게임 (정규화된 점수)
  AND p.engagement_velocity > 0.01  -- 최소 1% 이상의 velocity
ORDER BY p.engagement_velocity DESC, p.playing DESC
LIMIT 100
