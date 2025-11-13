-- 0. 설정
{{
    config(
        materialized = 'view' if target.name == 'dev_local_tdd' else 'table',
        post_hook = [
            "COPY (SELECT * FROM {{ this }}) TO 's3://" ~ env_var('S3_BUCKET_NAME') ~ "/marts/dim_games/dim_games.parquet' (FORMAT PARQUET, COMPRESSION 'snappy', OVERWRITE_OR_IGNORE 1)"
        ] if target.name == 'prod_s3' else []
    )
}}

-- 오늘 들어온 데이터(manifest) 변환
with
games as (
    select * from {{ ref('stg_games') }}
),

platforms as (
    select * from {{ ref('stg_platforms') }}
),

genres as (
    select * from {{ ref('stg_genres') }}
),

game_modes as (
    select * from {{ ref('stg_game_modes') }}
),

game_themes as (
    select * from {{ ref('stg_themes') }}
),

player_perspectives as (
    select * from {{ ref('stg_player_perspectives') }}
),

-- Bridge 테이블: Inner Join으로 유효한 게임 ID만 필터링합니다.
game_platforms_bridge as (
    select b.*
    from {{ ref('stg_game_platform_bridge') }} as b
    inner join games as g on b.game_id = g.id
),

game_genres_bridge as (
    select b.*
    from {{ ref('stg_game_genre_bridge') }} as b
    inner join games as g on b.game_id = g.id
),

game_modes_bridge as (
    select b.*
    from {{ ref('stg_game_mode_bridge') }} as b
    inner join games as g on b.game_id = g.id
),

game_themes_bridge as (
    select b.*
    from {{ ref('stg_game_theme_bridge') }} as b
    inner join games as g on b.game_id = g.id
),

game_perspectives_bridge as (
    select b.*
    from {{ ref('stg_game_perspective_bridge') }} as b
    inner join games as g on b.game_id = g.id
),

-- 집계 테이블들: 각 브릿지 테이블과 명칭 테이블을 JOIN하고, 게임 ID별로 배열(LIST)로 묶습니다.
platforms_agg as (
    select
        b.game_id,
        list(p.name) as platform_names
    from game_platforms_bridge as b
    left join platforms as p
        on b.platform_id = p.id
    group by b.game_id
),

genres_agg as (
    select
        b.game_id,
        list(p.name) as genre_names
    from game_genres_bridge as b
    left join genres as p
        on b.genre_id = p.id
    group by b.game_id
),

game_modes_agg as (
    select
        b.game_id,
        list(p.name) as game_mode_names
    from game_modes_bridge as b
    left join game_modes as p
        on b.game_mode_id = p.id
    group by b.game_id
),

game_themes_agg as (
    select
        b.game_id,
        list(p.name) as theme_names
    from game_themes_bridge as b
    left join game_themes as p
        on b.theme_id = p.id
    group by b.game_id
),

player_perspectives_agg as (
    select
        b.game_id,
        list(p.name) as perspective_names
    from game_perspectives_bridge as b
    left join player_perspectives as p
        on b.perspective_id = p.id
    group by b.game_id
),

-- 새 데이터 최종 형태
new_processed as (
    select
        g.id as game_id,
        g.name as game_name,
        g.slug as game_slug,
        g.summary as game_summary,
        p.platform_names,
        gn.genre_names,
        gm.game_mode_names,
        gt.theme_names,
        pp.perspective_names,
        g.url,
        g.cover,
        g.first_release_date,
        g.created_at,
        g.updated_at
        
    from games as g
    left join platforms_agg as p
        on g.id = p.game_id
    left join genres_agg as gn
        on g.id = gn.game_id
    left join game_modes_agg as gm
        on g.id = gm.game_id
    left join game_themes_agg as gt
        on g.id = gt.game_id
    left join player_perspectives_agg as pp
        on g.id = pp.game_id
),

-- 기존 데이터 로드 (S3 -> CloudFront -> DuckDB)
old_processed as (
    {% if target.name == 'prod_s3' and not flags.FULL_REFRESH %}
    select * from read_parquet('https://{{ env_var("CLOUDFRONT_DOMAIN", "") }}/marts/dim_games/dim_games.parquet')
    {% else %}
    select * from new_processed where 1=0
    {% endif %}
),

-- 병합 및 중복 제거
combined as (
    select * from old_processed
    union all
    select * from new_processed
)

select *
from combined
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY game_id
    ORDER BY updated_at DESC
) = 1
