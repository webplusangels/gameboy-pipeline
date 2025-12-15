{{
    config(
        materialized = 'table' if target.name == 'prod_s3' else 'view',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/marketing/mart_theme_trends.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

WITH unnested_themes AS (
    SELECT
        unnest(theme_names) as theme
    FROM {{ ref('dim_games') }}
    WHERE theme_names IS NOT NULL
)

SELECT
    theme,
    COUNT(*) as count
FROM unnested_themes
GROUP BY theme
ORDER BY count DESC
LIMIT 50
