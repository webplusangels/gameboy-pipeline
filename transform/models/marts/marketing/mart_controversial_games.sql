{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_controversial_games.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- ⚠️ 논란의 게임 (찬반 양극화 게임)
-- 부정도 많고 긍정도 많은 진짜 "논란작" 선별
-- 단순 망작이 아닌 의견이 갈리는 게임

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
    WHERE steam_negative_reviews IS NOT NULL
      AND steam_total_reviews IS NOT NULL
      AND steam_positive_reviews IS NOT NULL
),

scored_controversial AS (
    SELECT
        *,
        -- Controversy Score: 부정도 많고 긍정도 많을수록 높은 점수 (양극화)
        (
            negative_reviews_percentile * 0.50 +   -- 부정 리뷰 많음 (50%)
            positive_reviews_percentile * 0.30 +   -- 긍정 리뷰도 많음 (30%, 양극화 핵심)
            total_reviews_percentile * 0.20        -- 충분한 샘플 (20%)
        ) AS controversy_score
    FROM popularity_metrics
)

SELECT
    g.game_name,
    p.controversy_score,
    p.steam_negative_reviews,
    p.steam_positive_reviews,
    p.steam_total_reviews,
    p.negative_reviews_percentile,
    p.positive_reviews_percentile,
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
INNER JOIN scored_controversial p ON g.game_id = p.game_id
WHERE p.negative_reviews_percentile >= 40        -- 상위 60% 부정 리뷰
  AND p.positive_reviews_percentile >= 30        -- 상위 70% 긍정 리뷰 (양극화 필수)
  AND p.total_reviews_percentile >= 30           -- 충분한 리뷰 수
ORDER BY 
    p.controversy_score DESC,          -- Controversy score 우선
    p.steam_total_reviews DESC         -- 리뷰 수 많을수록
LIMIT 100
