{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_top_hyped.sql.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

SELECT
    game_name,
    hypes,
    aggregated_rating,
    aggregated_rating_count,
    platform_names,
    genre_names,
    game_summary,
    cover,
    url
FROM {{ ref('dim_games') }}
WHERE hypes IS NOT NULL
  AND hypes > 0
ORDER BY hypes DESC
LIMIT 50
