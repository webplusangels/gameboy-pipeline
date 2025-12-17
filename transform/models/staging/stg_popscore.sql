-- PopScore 데이터와 Popularity Types를 JOIN하여 읽기 쉬운 형태로 변환
-- Long format: 게임별 인기 지표를 row로 유지 (분석 유연성)
WITH popscore AS (
    SELECT 
        id,
        game_id,
        popularity_type,
        value
    {% if target.name == 'dev_local_tdd' %}
    FROM read_json_auto('../logs/sample_popscore.json')
    {% else %}
    FROM read_json_auto({{ get_timeseries_path("popscore") }}, ignore_errors = TRUE)
    {% endif %}
),

popularity_types AS (
    SELECT * FROM {{ ref('stg_popularity_types') }}
)

SELECT
    p.id,
    p.game_id,
    p.popularity_type,
    p.value,
    -- Popularity Type 정보 추가
    pt.name AS popularity_type_name,
    pt.external_popularity_source,
    -- 데이터 소스 분류
    CASE 
        WHEN pt.external_popularity_source = 121 THEN 'IGDB'
        WHEN pt.external_popularity_source = 1 THEN 'Steam'
        WHEN pt.external_popularity_source = 6 THEN 'Twitch'
        ELSE 'Unknown'
    END AS data_source,
    -- 카테고리 분류
    CASE
        WHEN p.popularity_type IN (1, 2, 3, 4) THEN 'User Engagement'
        WHEN p.popularity_type IN (5, 6, 7, 8) THEN 'Steam Metrics'
        WHEN p.popularity_type IN (9, 10) THEN 'Steam Wishlist'
        WHEN p.popularity_type = 34 THEN 'Streaming'
        ELSE 'Other'
    END AS metric_category
FROM popscore p
LEFT JOIN popularity_types pt
    ON p.popularity_type = pt.id
