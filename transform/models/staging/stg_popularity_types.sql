-- Popularity Types Dimension (참조 테이블)
-- 11개의 popularity type 매핑 정보
SELECT 
    id,
    name,
    external_popularity_source,
    popularity_source,
    created_at,
    updated_at,
    checksum
{% if target.name == 'dev_local_tdd' %}
FROM read_json_auto('../logs/sample_popularity_types.json')
{% else %}
FROM read_json_auto({{ get_dimension_path("popularity_types") }}, ignore_errors = TRUE)
{% endif %}
