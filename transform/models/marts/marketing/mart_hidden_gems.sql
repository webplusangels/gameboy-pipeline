{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_hidden_gems.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- 💎 숨은 보석 게임 (낮은 인지도 + 높은 품질)
-- 상대적으로 덜 알려졌지만 플레이한 사람들의 평가가 매우 좋은 게임

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
)

SELECT
    g.game_name,
    p.steam_positive_ratio,
    p.steam_total_reviews,
    p.igdb_total_engagement,
    p.engagement_velocity,
    p.cross_platform_score,
    g.aggregated_rating,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN popularity_metrics p ON g.game_id = p.game_id
WHERE p.steam_positive_ratio >= 0.85  -- 매우 높은 긍정률
  AND p.steam_total_reviews BETWEEN 0.00005 AND 0.001  -- 적당한 리뷰 수 (숨어있음, 정규화된 점수)
  AND p.igdb_total_engagement < 0.01  -- IGDB에서 낮은 인지도 (정규화된 점수)
  AND p.steam_controversy_ratio < 0.20  -- 낮은 논란도
ORDER BY p.steam_positive_ratio DESC, p.steam_total_reviews ASC
LIMIT 100
