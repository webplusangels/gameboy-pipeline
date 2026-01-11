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
    WHERE steam_positive_reviews IS NOT NULL
      AND steam_total_reviews IS NOT NULL
)

SELECT
    g.game_name,
    p.cross_platform_score,
    p.steam_positive_reviews,
    p.steam_total_reviews,
    p.positive_reviews_percentile,
    p.total_reviews_percentile,
    p.playing_percentile,
    p.igdb_total_engagement,
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
INNER JOIN popularity_metrics p ON g.game_id = p.game_id
WHERE 
  -- 조건 1: 품질 기준 (positive reviews가 많아야 함)
  p.positive_reviews_percentile >= 50  -- 상위 50% positive reviews
  
  -- 조건 2: 최소 검증
  AND p.total_reviews_percentile >= 10  -- 상위 90% total reviews
  
  -- 조건 3: 최근성 또는 성장성 또는 인지도 (4가지 옵션 중 하나 만족)
  AND (
      -- 옵션 A: 최근 5년 출시
      (g.first_release_date IS NOT NULL AND to_timestamp(g.first_release_date) >= CURRENT_TIMESTAMP - INTERVAL '5 years')
      OR
      -- 옵션 B: 현재 활발히 플레이 중
      p.playing_percentile >= 40  -- 상위 60% playing activity
      OR
      -- 옵션 C: 멀티플랫폼
      p.cross_platform_score >= 2
      OR
      -- 옵션 D: 고평점
      g.aggregated_rating >= 75
  )
ORDER BY 
    -- 정렬 우선순위
    CASE 
        WHEN p.playing_percentile >= 70 THEN 3  -- 매우 활발한 플레이
        WHEN g.first_release_date IS NOT NULL 
             AND to_timestamp(g.first_release_date) >= CURRENT_TIMESTAMP - INTERVAL '1 year' THEN 2  -- 신작
        ELSE 1
    END DESC,
    p.playing_percentile DESC,
    p.positive_reviews_percentile DESC,
    p.cross_platform_score DESC
LIMIT 100
