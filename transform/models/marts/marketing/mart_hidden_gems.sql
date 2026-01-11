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
-- Steam 품질 + IGDB 평론가 점수를 결합하여 차별화

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
    WHERE steam_positive_reviews IS NOT NULL
      AND steam_total_reviews IS NOT NULL
)

SELECT
    g.game_name,
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
    -- 복합 품질 점수: positive reviews percentile + IGDB 평론가 평가
    (
        p.positive_reviews_percentile * 0.6 +  -- Positive reviews percentile (60% 가중치)
        COALESCE(g.aggregated_rating, 75) * 0.4  -- IGDB 평점 (40% 가중치, 없으면 75점 가정)
    ) AS quality_score,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN popularity_metrics p ON g.game_id = p.game_id
WHERE p.positive_reviews_percentile >= 50  -- 상위 50% positive reviews (고품질)
  AND p.total_reviews_percentile <= 50  -- 하위 50% total reviews (숨어있음)
  AND p.engagement_percentile <= 40  -- 하위 40% IGDB 참여도 (낮은 인지도)
  AND COALESCE(p.negative_reviews_percentile, 0) <= 30  -- 하위 70% negative reviews (논란 없음)
ORDER BY 
    quality_score DESC,  -- 복합 품질 점수 우선
    p.total_reviews_percentile ASC,  -- 더 숨어있을수록 우선
    g.aggregated_rating DESC  -- IGDB 평점 높을수록 우선
LIMIT 100
