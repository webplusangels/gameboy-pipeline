{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_rising_stars.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- ⭐ 떠오르는 스타 게임 (종합 점수 기반)
-- 현재 인기도 + 성장 속도 + 품질을 종합적으로 평가

WITH popularity_metrics AS (
    SELECT 
        *,
        -- Twitch percentile 계산 (선택적)
        NTILE(100) OVER (ORDER BY COALESCE(twitch_24hr_hours_watched, 0)) AS twitch_percentile,
        
        -- 성장 속도 (Playing / Played 비율)
        CASE 
            WHEN COALESCE(played, 0) > 0 
            THEN COALESCE(playing, 0) / played
            ELSE 0
        END AS velocity_ratio
    FROM {{ ref('fct_game_popularity') }}
    WHERE playing IS NOT NULL  -- playing이 있어야 함 (핵심 지표)
),

velocity_percentiles AS (
    SELECT 
        *,
        NTILE(100) OVER (ORDER BY velocity_ratio) AS velocity_percentile
    FROM popularity_metrics
),

scored_games AS (
    SELECT
        *,
        -- 종합 점수 계산 (가중 평균)
        (
            (playing_percentile * 0.30) +                                      -- 현재 인기 (30%, 35%→30%)
            (velocity_percentile * 0.25) +                                     -- 성장 속도 (25%)
            (COALESCE(engagement_percentile, 0) * 0.05) +                     -- IGDB 참여도 (5%, NEW)
            (COALESCE(positive_reviews_percentile, 0) * 0.15) +               -- Steam 유저 평가 (15%, 20%→15%)
            (COALESCE(twitch_percentile, 0) * 0.10) +                         -- Twitch 인기 (10%, 선택적)
            (COALESCE(g.aggregated_rating, 75) * 0.15)                        -- 전문가 평가 (15%, 10%→15%)
        ) AS rising_score
    FROM velocity_percentiles p
    LEFT JOIN {{ ref('dim_games') }} g ON p.game_id = g.game_id
)

SELECT
    g.game_name,
    p.rising_score,
    p.playing_percentile,
    p.velocity_percentile,
    p.engagement_percentile,
    p.velocity_ratio,
    p.positive_reviews_percentile,
    p.twitch_percentile,
    p.cross_platform_score,
    p.steam_positive_reviews,
    p.steam_total_reviews,
    p.playing,
    p.played,
    p.igdb_total_engagement,
    g.aggregated_rating,
    g.platform_names,
    g.genre_names,
    g.first_release_date,
    g.game_summary,
    g.cover,
    g.url
FROM {{ ref('dim_games') }} g
INNER JOIN scored_games p ON g.game_id = p.game_id
WHERE 
  p.playing_percentile >= 30  -- 최소 활발도 (상위 70%)
ORDER BY 
    p.rising_score DESC,               -- 1차: 종합 점수
    g.first_release_date DESC,         -- 2차: 최신 발매작
    p.playing_percentile DESC          -- 3차: 현재 활발도
LIMIT 100
