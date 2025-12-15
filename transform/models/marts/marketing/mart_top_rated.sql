{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_top_rated.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

SELECT
    game_name,
    aggregated_rating,
    aggregated_rating_count,
    genre_names,
    first_release_date,
    cover,
    url
FROM {{ ref('dim_games') }}
WHERE aggregated_rating IS NOT NULL
  AND aggregated_rating_count >= 5 -- 최소 5명 이상의 평가가 있는 게임만
ORDER BY aggregated_rating DESC
LIMIT 50
