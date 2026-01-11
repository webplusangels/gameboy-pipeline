{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_controversial_games.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- ⚠️ 논란의 게임 (부정 리뷰가 많은 게임)
-- 이미 정규화된 negative_reviews 값을 기준으로 순위 선정

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
    WHERE steam_negative_reviews IS NOT NULL
      AND steam_total_reviews IS NOT NULL
)

SELECT
    g.game_name,
    p.steam_negative_reviews,
    p.steam_positive_reviews,
    p.steam_total_reviews,
    p.negative_reviews_percentile,
    p.total_reviews_percentile,
    p.igdb_total_engagement,
    g.aggregated_rating,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN popularity_metrics p ON g.game_id = p.game_id
WHERE p.negative_reviews_percentile >= 30  -- 상위 70% negative reviews (논란 많음)
  AND p.total_reviews_percentile >= 20  -- 최소한의 리뷰 수 필요
ORDER BY 
    p.negative_reviews_percentile DESC,  -- 부정 리뷰가 가장 많은 순
    p.steam_total_reviews DESC
LIMIT 100
