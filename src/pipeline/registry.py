from src.pipeline.extractors import (
    BaseIgdbExtractor,
    IgdbExtractor,
    IgdbGameModeExtractor,
    IgdbGenreExtractor,
    IgdbPlatformExtractor,
    IgdbPlayerPerspectiveExtractor,
    IgdbPopScoreExtractor,
    IgdbPopularityTypesExtractor,
    IgdbThemeExtractor,
)

ALL_ENTITIES: dict[str, type[BaseIgdbExtractor]] = {
    "games": IgdbExtractor,
    "platforms": IgdbPlatformExtractor,
    "genres": IgdbGenreExtractor,
    "game_modes": IgdbGameModeExtractor,
    "player_perspectives": IgdbPlayerPerspectiveExtractor,
    "themes": IgdbThemeExtractor,
    "popscore": IgdbPopScoreExtractor,
    "popularity_types": IgdbPopularityTypesExtractor,
}
