{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_rising_stars.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- ⭐ 떠오르는 스타 게임 (최근 출시 + 빠른 성장 또는 높은 품질)
-- 조건 완화: 더 많은 게임을 포착하도록 개선

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
WHERE 
  -- 조건 1: 품질 기준 (대폭 완화)
  p.steam_positive_ratio >= 0.65  -- 65% 이상 (적당한 품질)
  
  -- 조건 2: 최소 검증 (대폭 완화)
  AND p.review_percentile >= 10  -- 상위 90% (거의 모든 게임)
  
  -- 조건 3: 최근성 또는 성장성 또는 인지도 (대폭 완화)
  AND (
      -- 옵션 A: 최근 5년 출시
      (g.first_release_date IS NOT NULL AND to_timestamp(g.first_release_date) >= CURRENT_TIMESTAMP - INTERVAL '5 years')
      OR
      -- 옵션 B: engagement velocity 상위 60%
      p.velocity_percentile >= 40
      OR
      -- 옵션 C: 멀티플랫폼
      p.cross_platform_score >= 2
      OR
      -- 옵션 D: 높은 평점
      g.aggregated_rating >= 75
  )
ORDER BY 
    -- 정렬 우선순위
    CASE 
        WHEN p.velocity_percentile >= 70 THEN 3  -- 매우 빠른 성장
        WHEN g.first_release_date IS NOT NULL 
             AND to_timestamp(g.first_release_date) >= CURRENT_TIMESTAMP - INTERVAL '1 year' THEN 2  -- 신작
        ELSE 1
    END DESC,
    p.velocity_percentile DESC,
    p.steam_positive_ratio DESC,
    p.cross_platform_score DESC
LIMIT 100
