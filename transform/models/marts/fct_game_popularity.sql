-- Game Popularity Index (Wide Format)
-- 게임별 모든 인기 지표를 컬럼으로 펼쳐서 한눈에 확인할 수 있는 인덱스
{{
    config(
        materialized='table'
    )
}}

WITH popscore AS (
    SELECT * FROM {{ ref('stg_popscore') }}
),

base_metrics AS (
    SELECT
        game_id,
        
        -- IGDB User Engagement Metrics
        MAX(CASE WHEN popularity_type = 1 THEN value END) AS visits,
        MAX(CASE WHEN popularity_type = 2 THEN value END) AS want_to_play,
        MAX(CASE WHEN popularity_type = 3 THEN value END) AS playing,
        MAX(CASE WHEN popularity_type = 4 THEN value END) AS played,
        
        -- Steam Performance Metrics (normalized values from IGDB API)
        MAX(CASE WHEN popularity_type = 5 THEN value END) AS steam_24hr_peak_players,
        MAX(CASE WHEN popularity_type = 6 THEN value END) AS steam_positive_reviews,
        MAX(CASE WHEN popularity_type = 7 THEN value END) AS steam_negative_reviews,
        MAX(CASE WHEN popularity_type = 8 THEN value END) AS steam_total_reviews,
        
        -- Steam Commercial Metrics
        MAX(CASE WHEN popularity_type = 9 THEN value END) AS steam_global_top_sellers,
        MAX(CASE WHEN popularity_type = 10 THEN value END) AS steam_most_wishlisted,
        
        -- Streaming Metrics
        MAX(CASE WHEN popularity_type = 34 THEN value END) AS twitch_24hr_hours_watched,
        
        -- IGDB 참여도 합계
        COALESCE(MAX(CASE WHEN popularity_type = 1 THEN value END), 0) +
        COALESCE(MAX(CASE WHEN popularity_type = 2 THEN value END), 0) +
        COALESCE(MAX(CASE WHEN popularity_type = 3 THEN value END), 0) +
        COALESCE(MAX(CASE WHEN popularity_type = 4 THEN value END), 0) AS igdb_total_engagement,
        
        -- 데이터 소스 플래그
        MAX(CASE WHEN popularity_type IN (1, 2, 3, 4) THEN 1 ELSE 0 END) AS has_igdb_data,
        MAX(CASE WHEN popularity_type IN (5, 6, 7, 8, 9, 10) THEN 1 ELSE 0 END) AS has_steam_data,
        MAX(CASE WHEN popularity_type = 34 THEN 1 ELSE 0 END) AS has_twitch_data,
        
        -- 사용 가능한 지표 개수
        COUNT(DISTINCT popularity_type) AS available_metrics_count,
        
        -- 📊 멀티플랫폼 인기도 (0-3)
        (CASE WHEN MAX(CASE WHEN popularity_type IN (1, 2, 3, 4) THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END +
         CASE WHEN MAX(CASE WHEN popularity_type IN (5, 6, 7, 8, 9, 10) THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END +
         CASE WHEN MAX(CASE WHEN popularity_type = 34 THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END) AS cross_platform_score
    FROM popscore
    GROUP BY game_id
)

SELECT
    *,
    
    -- Percentile Rankings (이미 정규화된 값을 기준으로 순위화)
    -- Steam positive reviews가 많을수록 높은 percentile
    NTILE(100) OVER (ORDER BY COALESCE(steam_positive_reviews, 0)) AS positive_reviews_percentile,
    
    -- Steam negative reviews가 많을수록 높은 percentile (논란 많음)
    NTILE(100) OVER (ORDER BY COALESCE(steam_negative_reviews, 0)) AS negative_reviews_percentile,
    
    -- Steam total reviews가 많을수록 높은 percentile
    NTILE(100) OVER (ORDER BY COALESCE(steam_total_reviews, 0)) AS total_reviews_percentile,
    
    -- IGDB total engagement가 높을수록 높은 percentile
    NTILE(100) OVER (ORDER BY igdb_total_engagement) AS engagement_percentile,
    
    -- Playing activity가 높을수록 높은 percentile
    NTILE(100) OVER (ORDER BY COALESCE(playing, 0)) AS playing_percentile,
    
    -- Played activity가 높을수록 높은 percentile
    NTILE(100) OVER (ORDER BY COALESCE(played, 0)) AS played_percentile

FROM base_metrics
