{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_controversial_games.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- ⚠️ 논란의 게임 (높은 Steam 논란도)
-- 부정 리뷰 비율이 높아 찬반이 갈리는 게임

WITH popularity_metrics AS (
    SELECT * FROM {{ ref('fct_game_popularity') }}
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
INNER JOIN popularity_metrics p ON g.game_id = p.game_id
WHERE p.steam_controversy_ratio IS NOT NULL
  AND p.steam_total_reviews >= 100  -- 최소 100개 이상의 리뷰
  AND p.steam_controversy_ratio >= 0.30  -- 부정률 30% 이상
ORDER BY p.steam_controversy_ratio DESC, p.steam_total_reviews DESC
LIMIT 100
