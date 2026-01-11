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
),

percentiles AS (
    SELECT 
        *,
        NTILE(100) OVER (ORDER BY steam_total_reviews) AS review_percentile,
        NTILE(100) OVER (ORDER BY igdb_total_engagement) AS engagement_percentile
    FROM popularity_metrics
    WHERE steam_total_reviews IS NOT NULL
      AND steam_positive_ratio IS NOT NULL
)

SELECT
    g.game_name,
    p.steam_positive_ratio,
    p.steam_total_reviews,
    p.igdb_total_engagement,
    p.engagement_velocity,
    p.cross_platform_score,
    g.aggregated_rating,
    g.aggregated_rating_count,
    -- 복합 품질 점수: Steam 사용자 평가 + IGDB 평론가 평가
    (
        p.steam_positive_ratio * 100 * 0.6 +  -- Steam 긍정률 (60% 가중치)
        COALESCE(g.aggregated_rating, 75) * 0.4  -- IGDB 평점 (40% 가중치, 없으면 75점 가정)
    ) AS quality_score,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN percentiles p ON g.game_id = p.game_id
WHERE p.steam_positive_ratio >= 0.75  -- 높은 긍정률 (75% 이상으로 완화)
  AND p.review_percentile <= 50  -- 하위 50% 리뷰 수 (숨어있음, 완화)
  AND p.engagement_percentile <= 40  -- 하위 40% IGDB 참여도 (낮은 인지도, 완화)
  AND COALESCE(p.steam_controversy_ratio, 0) < 0.30  -- 낮은 논란도
ORDER BY 
    quality_score DESC,  -- 복합 품질 점수 우선
    p.review_percentile ASC,  -- 더 숨어있을수록 우선
    g.aggregated_rating DESC  -- IGDB 평점 높을수록 우선
LIMIT 100
