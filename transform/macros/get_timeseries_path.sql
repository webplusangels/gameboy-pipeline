{% macro get_timeseries_path(entity_name) -%}
{%- set cloudfront_domain = env_var("CLOUDFRONT_DOMAIN", "") -%}

{%- if cloudfront_domain == "" -%}
    {{ exceptions.raise_compiler_error("CLOUDFRONT_DOMAIN 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.") }}
{%- endif -%}

{%- set latest_partition_query = "SELECT DISTINCT regexp_extract(filename, 'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS dt FROM glob('s3://" ~ env_var('S3_BUCKET_NAME') ~ "/raw/" ~ entity_name ~ "/dt=*/*.jsonl') ORDER BY dt DESC LIMIT 1" -%}

{%- if execute -%}
    {%- set result = run_query(latest_partition_query) -%}
    
    {%- if result and result.rows | length > 0 -%}
        {%- set latest_dt = result.rows[0][0] -%}
        {%- set manifest_url = "https://" ~ cloudfront_domain ~ "/raw/" ~ entity_name ~ "/dt=" ~ latest_dt ~ "/_manifest.json" -%}
        
        {%- set manifest_query = "SELECT UNNEST(files) AS file FROM read_json_auto('" ~ manifest_url ~ "')" -%}
        {%- set manifest_result = run_query(manifest_query) -%}
        
        {%- if manifest_result and manifest_result.rows | length > 0 -%}
            {%- set file_urls = [] -%}
            {%- for row in manifest_result.rows -%}
                {%- set _ = file_urls.append("'https://" ~ cloudfront_domain ~ "/" ~ row[0] ~ "'") -%}
            {%- endfor -%}
[{{ file_urls | join(", ") }}]
        {%- else -%}
[]
        {%- endif -%}
    {%- else -%}
[]
    {%- endif -%}
{%- else -%}
[]
{%- endif -%}
{%- endmacro %}
