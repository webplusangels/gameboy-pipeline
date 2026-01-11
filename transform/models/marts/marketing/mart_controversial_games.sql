{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_controversial_games.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- ⚠️ 논란의 게임 (찬반 양립 또는 높은 부정률)
-- 진정한 논란: 40%+ 부정률 OR 찬반 비슷(45-55% 혼합)

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
),

review_percentiles AS (
    SELECT 
        *,
        NTILE(100) OVER (ORDER BY steam_total_reviews) AS review_percentile
    FROM popularity_metrics
    WHERE steam_total_reviews IS NOT NULL
      AND steam_controversy_ratio IS NOT NULL
)

SELECT
    g.game_name,
    p.steam_controversy_ratio,
    p.steam_positive_ratio,
    p.steam_positive_reviews,
    p.steam_negative_reviews,
    p.steam_total_reviews,
    p.igdb_total_engagement,
    g.aggregated_rating,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN review_percentiles p ON g.game_id = p.game_id
WHERE p.review_percentile >= 30  -- 상위 70% 리뷰 수 (조건 완화)
  AND p.steam_controversy_ratio >= 0.25  -- 25%+ 부정률 (논란의 여지)
ORDER BY 
    -- 찬반이 정확히 50:50에 가까울수록 높은 점수
    ABS(p.steam_controversy_ratio - 0.50) ASC,
    p.steam_total_reviews DESC
LIMIT 100
