-- 0. 설정
{{
    config(
        materialized = 'view' if target.name == 'dev_local_tdd' else 'incremental',

        unique_key = 'game_id',

        on_schema_change = 'append_new_columns'
    )
}}

-- 1. TDD로 검증된 Staging(재료) 테이블들을 불러옵니다.
with games as (
    select * from {{ ref('stg_games') }}
    {% if is_incremental() %}
    -- 증분 모드: 마지막 실행 이후 업데이트된 게임만 처리
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
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

game_platforms_bridge as (
    select * from {{ ref('stg_game_platform_bridge') }}
),

game_genres_bridge as (
    select * from {{ ref('stg_game_genre_bridge') }}
),

game_modes_bridge as (
    select * from {{ ref('stg_game_mode_bridge') }}
),

game_themes_bridge as (
    select * from {{ ref('stg_game_theme_bridge') }}
),

game_perspectives_bridge as (
    select * from {{ ref('stg_game_perspective_bridge') }}
),

-- 2. 플랫폼 이름(name)을 JOIN하고, 게임 ID별로 다시 배열(LIST)로 묶습니다.
platforms_agg as (
    select
        b.game_id,
        list(p.name) as platform_names -- ["PC", "PlayStation 4"]
    from game_platforms_bridge as b
    left join platforms as p
        on b.platform_id = p.id
    group by b.game_id
),

-- 3. 장르 이름(name)을 JOIN하고, 게임 ID별로 다시 배열(LIST)로 묶습니다.
genres_agg as (
    select
        b.game_id,
        list(p.name) as genre_names -- ["Simulator", "Strategy"]
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
)

-- 4. 최종: games 테이블에 모든 '배열' 컬럼들을 JOIN합니다.
final as (
    select
        g.id as game_id,
        g.name as game_name,
        g.summary as game_summary,
        p.platform_names, -- (2단계에서 만든 'platform_names' 배열)
        gn.genre_names,   -- (3단계에서 만든 'genre_names' 배열)
        gm.game_mode_names,
        gt.theme_names,
        pp.perspective_names,
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
)

select * from final