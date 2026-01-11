{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_trending_now.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- 🔥 지금 가장 핫한 게임 (Playing Activity 기반)
-- 현재 활발하게 플레이되는 게임 (이미 정규화된 playing 값 활용)

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
    WHERE playing IS NOT NULL
      AND played IS NOT NULL
)

SELECT
    g.game_name,
    p.playing,
    p.played,
    p.playing_percentile,
    p.played_percentile,
    p.igdb_total_engagement,
    p.steam_positive_reviews,
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
WHERE p.playing_percentile >= 50  -- 상위 50% 현재 플레이 중인 게임
  AND p.played_percentile >= 20  -- 최소한의 played 이력
ORDER BY p.playing_percentile DESC, p.playing DESC
LIMIT 100
