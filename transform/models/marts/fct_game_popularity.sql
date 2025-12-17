-- Game Popularity Index (Wide Format)
-- 게임별 모든 인기 지표를 컬럼으로 펼쳐서 한눈에 확인할 수 있는 인덱스
{{
    config(
        materialized='table'
    )
}}

WITH popscore AS (
    SELECT * FROM {{ ref('stg_popscore') }}
)

SELECT
    game_id,
    
    -- IGDB User Engagement Metrics
    MAX(CASE WHEN popularity_type = 1 THEN value END) AS visits,
    MAX(CASE WHEN popularity_type = 2 THEN value END) AS want_to_play,
    MAX(CASE WHEN popularity_type = 3 THEN value END) AS playing,
    MAX(CASE WHEN popularity_type = 4 THEN value END) AS played,
    
    -- Steam Performance Metrics
    MAX(CASE WHEN popularity_type = 5 THEN value END) AS steam_24hr_peak_players,
    MAX(CASE WHEN popularity_type = 6 THEN value END) AS steam_positive_reviews,
    MAX(CASE WHEN popularity_type = 7 THEN value END) AS steam_negative_reviews,
    MAX(CASE WHEN popularity_type = 8 THEN value END) AS steam_total_reviews,
    
    -- Steam Commercial Metrics
    MAX(CASE WHEN popularity_type = 9 THEN value END) AS steam_global_top_sellers,
    MAX(CASE WHEN popularity_type = 10 THEN value END) AS steam_most_wishlisted,
    
    -- Streaming Metrics
    MAX(CASE WHEN popularity_type = 34 THEN value END) AS twitch_24hr_hours_watched,
    
    -- Derived Metrics
    -- Steam 리뷰 긍정률
    CASE 
        WHEN MAX(CASE WHEN popularity_type = 8 THEN value END) > 0 
        THEN MAX(CASE WHEN popularity_type = 6 THEN value END) / MAX(CASE WHEN popularity_type = 8 THEN value END)
        ELSE NULL 
    END AS steam_positive_ratio,
    
    -- IGDB 참여도 합계 (Visits + Want to Play + Playing + Played)
    COALESCE(MAX(CASE WHEN popularity_type = 1 THEN value END), 0) +
    COALESCE(MAX(CASE WHEN popularity_type = 2 THEN value END), 0) +
    COALESCE(MAX(CASE WHEN popularity_type = 3 THEN value END), 0) +
    COALESCE(MAX(CASE WHEN popularity_type = 4 THEN value END), 0) AS igdb_total_engagement,
    
    -- 🔥 현재 활발도 (Playing / Played): 얼마나 지금 활발한 게임인지
    CASE 
        WHEN MAX(CASE WHEN popularity_type = 4 THEN value END) > 0 
        THEN MAX(CASE WHEN popularity_type = 3 THEN value END) / MAX(CASE WHEN popularity_type = 4 THEN value END)
        ELSE NULL 
    END AS engagement_velocity,
    
    -- 🎮 Steam 논란도 (Negative / Total): 부정 리뷰 비율 (낮을수록 좋음)
    CASE 
        WHEN MAX(CASE WHEN popularity_type = 8 THEN value END) > 0 
        THEN MAX(CASE WHEN popularity_type = 7 THEN value END) / MAX(CASE WHEN popularity_type = 8 THEN value END)
        ELSE NULL 
    END AS steam_controversy_ratio,
    
    -- 📊 멀티플랫폼 인기도 (0-3): 여러 플랫폼에서 데이터가 있으면 더 신뢰도 높음
    (CASE WHEN MAX(CASE WHEN popularity_type IN (1, 2, 3, 4) THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END +
     CASE WHEN MAX(CASE WHEN popularity_type IN (5, 6, 7, 8, 9, 10) THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END +
     CASE WHEN MAX(CASE WHEN popularity_type = 34 THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END) AS cross_platform_score,
    
    -- 사용 가능한 지표 개수 (데이터 풍부도)
    COUNT(DISTINCT popularity_type) AS available_metrics_count,
    
    -- 데이터 소스 플래그
    MAX(CASE WHEN popularity_type IN (1, 2, 3, 4) THEN 1 ELSE 0 END) AS has_igdb_data,
    MAX(CASE WHEN popularity_type IN (5, 6, 7, 8, 9, 10) THEN 1 ELSE 0 END) AS has_steam_data,
    MAX(CASE WHEN popularity_type = 34 THEN 1 ELSE 0 END) AS has_twitch_data

FROM popscore
GROUP BY game_id
