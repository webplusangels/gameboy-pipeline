{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_top_rated.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- 🏆 최고 평점 게임 (Bayesian Average 적용)
-- 평가 수가 적은 게임에 패널티를 부여하여 신뢰도 높은 순위 제공

WITH rating_stats AS (
    SELECT
        AVG(aggregated_rating) AS mean_rating,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY aggregated_rating_count) AS min_votes_threshold
    FROM {{ ref('dim_games') }}
    WHERE aggregated_rating IS NOT NULL
      AND aggregated_rating_count > 0
),

weighted_ratings AS (
    SELECT
        g.game_id,
        g.game_name,
        g.aggregated_rating,
        g.aggregated_rating_count,
        g.genre_names,
        g.first_release_date,
        g.cover,
        g.url,
        -- Bayesian Average (IMDB Top 250 방식)
        -- weighted_rating = (v/(v+m)) * R + (m/(v+m)) * C
        -- v = 게임의 평가 수, m = 필요한 최소 평가 수, R = 게임 평점, C = 전체 평균 평점
        (
            (g.aggregated_rating_count::FLOAT / (g.aggregated_rating_count + s.min_votes_threshold)) * g.aggregated_rating +
            (s.min_votes_threshold / (g.aggregated_rating_count + s.min_votes_threshold)) * s.mean_rating
        ) AS weighted_rating
    FROM {{ ref('dim_games') }} g
    CROSS JOIN rating_stats s
    WHERE g.aggregated_rating IS NOT NULL
      AND g.aggregated_rating_count >= 5  -- 최소 5개 평가
)

SELECT
    game_name,
    aggregated_rating,
    aggregated_rating_count,
    weighted_rating,
    genre_names,
    first_release_date,
    cover,
    url
FROM weighted_ratings
ORDER BY weighted_rating DESC, aggregated_rating_count DESC
LIMIT 50
