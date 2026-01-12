{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_hidden_gems.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- 💎 숨은 보석 게임 (낮은 인지도 + 높은 품질)
-- 복합 점수로 진짜 "숨은 명작"을 발굴
-- 품질 점수 + 숨겨진 정도를 종합 평가

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
    WHERE (steam_positive_reviews IS NOT NULL OR igdb_total_engagement IS NOT NULL)
),

scored_gems AS (
    SELECT
        *,
        -- Hidden Score: 품질은 높지만 인지도가 낮을수록 높은 점수
        (
            COALESCE(positive_reviews_percentile, 50) * 0.40 +        -- Steam 유저 품질 (40%)
            COALESCE(g.aggregated_rating, 75) * 0.30 +                -- IGDB 전문가 평가 (30%)
            (100 - COALESCE(engagement_percentile, 50)) * 0.20 +      -- 낮은 IGDB 참여도 = 숨어있음 (20%)
            (100 - COALESCE(total_reviews_percentile, 50)) * 0.10     -- 적은 리뷰 수 = 숨어있음 (10%)
        ) AS hidden_score
    FROM popularity_metrics p
    LEFT JOIN {{ ref('dim_games') }} g ON p.game_id = g.game_id
)

SELECT
    g.game_name,
    p.hidden_score,
    p.steam_positive_reviews,
    p.steam_negative_reviews,
    p.steam_total_reviews,
    p.positive_reviews_percentile,
    p.total_reviews_percentile,
    p.engagement_percentile,
    p.igdb_total_engagement,
    p.cross_platform_score,
    g.aggregated_rating,
    g.aggregated_rating_count,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN scored_gems p ON g.game_id = p.game_id
WHERE (
    COALESCE(p.positive_reviews_percentile, 50) >= 50  -- 최소 품질 기준
    OR g.aggregated_rating >= 75                        -- 또는 전문가 평가 우수
  )
  AND COALESCE(p.engagement_percentile, 50) <= 50      -- 낮은 IGDB 인지도
  AND COALESCE(p.total_reviews_percentile, 50) <= 60  -- 상대적으로 적은 리뷰
  AND COALESCE(p.negative_reviews_percentile, 0) <= 40  -- 논란 없음
ORDER BY 
    p.hidden_score DESC,              -- Hidden score 우선
    g.aggregated_rating DESC,         -- IGDB 평점
    p.positive_reviews_percentile DESC  -- Steam 유저 평가
LIMIT 100
