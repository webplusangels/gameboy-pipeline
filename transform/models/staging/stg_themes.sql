-- TDD 환경: 로컬 JSONL 파일에서 읽기
-- Prod 환경: S3 JSONL 파일에서 읽기
{% if target.name == 'dev_local_tdd' %}
SELECT 
    id,
    name,
    slug,
    url,
    created_at,
    updated_at,
    checksum
FROM read_json_auto('seeds/igdb_themes_mock.jsonl')
{% else %}
SELECT 
    id,
    name,
    slug,
    url,
    created_at,
    updated_at,
    checksum
FROM read_json_auto('s3://{{ env_var("S3_BUCKET_NAME") }}/raw/themes/*.jsonl')
{% endif %}
