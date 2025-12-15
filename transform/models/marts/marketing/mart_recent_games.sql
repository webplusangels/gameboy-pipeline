{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_recent_games.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

SELECT
    game_name,
    first_release_date,
    platform_names,
    genre_names,
    game_summary,
    cover,
    url
FROM {{ ref('dim_games') }}
WHERE first_release_date IS NOT NULL
  AND to_timestamp(first_release_date) <= CURRENT_TIMESTAMP
ORDER BY first_release_date DESC
LIMIT 50
