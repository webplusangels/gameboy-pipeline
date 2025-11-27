# 실행 순서 정의 (차원 데이터 먼저, 팩트 데이터 나중에)
EXECUTION_ORDER = [
    "platforms",
    "genres",
    "game_modes",
    "themes",
    "player_perspectives",
    "games",  # 마지막
]

DIMENSION_ENTITIES = {
    "platforms",
    "genres",
    "game_modes",
    "themes",
    "player_perspectives",
}

FACT_ENTITIES = {
    "games",
}
