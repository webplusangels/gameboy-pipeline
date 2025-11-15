-- TDD 환경: 로컬 JSONL 파일에서 읽기
-- Prod 환경: S3 JSONL 파일에서 읽기
SELECT 
    id,
    name,
    slug,
    alternative_name,
    generation,
    platform_logo,
    platform_type,
    url,
    created_at,
    updated_at,
    checksum
{% if target.name == 'dev_local_tdd' %}
FROM read_json_auto('seeds/igdb_platforms_mock.jsonl')
{% else %}
FROM read_json_auto({{ get_dimension_path("platforms") }}, ignore_errors = TRUE)
{% endif %}
