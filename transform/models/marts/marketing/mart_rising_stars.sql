{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_rising_stars.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- ⭐ 떠오르는 스타 게임 (최근 출시 + 빠른 성장 + 높은 품질)
-- 진정한 "Rising": 최근 2년 내 출시 + engagement velocity 높음 + 멀티플랫폼

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
),

review_percentiles AS (
    SELECT 
        *,
        NTILE(100) OVER (ORDER BY steam_total_reviews) AS review_percentile,
        NTILE(100) OVER (ORDER BY COALESCE(engagement_velocity, 0)) AS velocity_percentile
    FROM popularity_metrics
    WHERE steam_total_reviews IS NOT NULL
)

SELECT
    g.game_name,
    p.cross_platform_score,
    p.steam_positive_ratio,
    p.steam_total_reviews,
    p.igdb_total_engagement,
    p.engagement_velocity,
    p.playing,
    p.played,
    p.available_metrics_count,
    g.aggregated_rating,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN review_percentiles p ON g.game_id = p.game_id
WHERE p.cross_platform_score >= 2  -- 최소 2개 플랫폼 이상
  AND p.steam_positive_ratio >= 0.75  -- Steam 긍정률 75% 이상
  AND p.review_percentile >= 30  -- 상위 70% 리뷰 수 (충분한 검증)
  AND p.igdb_total_engagement > 0  -- IGDB 참여도 있음
  AND (
      -- 조건 1: 최근 2년 내 출시 (진짜 신작)
      (g.first_release_date IS NOT NULL AND to_timestamp(g.first_release_date) >= CURRENT_TIMESTAMP - INTERVAL '2 years')
      OR
      -- 조건 2: engagement_velocity 상위 40% (빠르게 성장 중)
      p.velocity_percentile >= 60
  )
ORDER BY 
    p.velocity_percentile DESC,
    p.cross_platform_score DESC,
    p.steam_positive_ratio DESC
LIMIT 100
