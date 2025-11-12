{% macro get_latest_partition() -%}
{{ run_started_at.strftime('%Y-%m-%d') }}
{%- endmacro %}

{% macro get_partition_path(entity_name) -%}
{%- set cloudfront_domain = env_var("CLOUDFRONT_DOMAIN", "") -%}
{%- set date = get_latest_partition() -%}

{%- if cloudfront_domain == "" -%}
    {{ exceptions.raise_compiler_error("CLOUDFRONT_DOMAIN 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.") }}
{%- endif -%}

{%- set manifest_url = "https://" ~ cloudfront_domain ~ "/raw/" ~ entity_name ~ "/dt=" ~ date ~ "/_manifest.json" -%}

{%- if execute -%}
    {%- set manifest_query = "SELECT UNNEST(files) AS file FROM read_json_auto('" ~ manifest_url ~ "')" -%}
    {%- set result = run_query(manifest_query) -%}
    
    {%- if result and result.rows | length > 0 -%}
        {%- set file_urls = [] -%}
        {%- for row in result.rows -%}
            {%- set _ = file_urls.append("'https://" ~ cloudfront_domain ~ "/" ~ row[0] ~ "'") -%}
        {%- endfor -%}
[{{ file_urls | join(', ') }}]
    {%- else -%}
[]
    {%- endif -%}
{%- else -%}
[]
{%- endif -%}
{%- endmacro %}

{% macro get_all_partitions_path(entity_name) -%}
{%- set cloudfront_domain = env_var("CLOUDFRONT_DOMAIN", "") -%}

{%- if cloudfront_domain == "" -%}
    {{ exceptions.raise_compiler_error("CLOUDFRONT_DOMAIN 환경 변수가 설정되지 않았습니다.") }}
{%- endif -%}

https://{{ cloudfront_domain }}/raw/{{ entity_name }}/dt=*/*.jsonl
{%- endmacro %}
