{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_rising_stars.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- ⭐ 떠오르는 스타 게임 (멀티플랫폼 + 높은 평가)
-- Cross-platform score 높고 Steam 평가 좋은 신뢰도 높은 게임

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
)

SELECT
    g.game_name,
    p.cross_platform_score,
    p.steam_positive_ratio,
    p.steam_total_reviews,
    p.igdb_total_engagement,
    p.engagement_velocity,
    p.available_metrics_count,
    g.aggregated_rating,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN popularity_metrics p ON g.game_id = p.game_id
WHERE p.cross_platform_score >= 2  -- 최소 2개 플랫폼 이상
  AND p.steam_positive_ratio >= 0.75  -- Steam 긍정률 75% 이상
  AND p.steam_total_reviews >= 50  -- 최소 50개 이상의 리뷰
  AND p.igdb_total_engagement > 0  -- IGDB 참여도 있음
ORDER BY p.cross_platform_score DESC, p.steam_positive_ratio DESC, p.igdb_total_engagement DESC
LIMIT 100
