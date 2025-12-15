{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_top_hyped.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
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
  -- 출시일이 미래이거나 미정인 게임만 (출시 예정작)
  AND (first_release_date IS NULL OR to_timestamp(first_release_date) > CURRENT_TIMESTAMP)
ORDER BY hypes DESC
LIMIT 50
