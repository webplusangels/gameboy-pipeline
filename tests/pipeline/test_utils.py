import pytest

from src.pipeline.utils import get_s3_path


@pytest.mark.parametrize(
    "entity_name,expected_prefix",
    [
        ("platforms", "raw/dimensions/platforms"),
        ("genres", "raw/dimensions/genres"),
        ("game_modes", "raw/dimensions/game_modes"),
        ("themes", "raw/dimensions/themes"),
        ("player_perspectives", "raw/dimensions/player_perspectives"),
        ("games", "raw/games/dt=2023-10-01"),
    ],
)
def test_get_s3_path_entities_return_dimension_path(
    entity_name: str, expected_prefix: str
) -> None:
    """
    get_s3_path 함수가 올바른 S3 경로를 반환하는지 테스트합니다.
    """
    dt_partition = "2023-10-01"
    s3_path = get_s3_path(entity_name, dt_partition)
    assert s3_path == expected_prefix


def test_get_s3_path_unknown_entity() -> None:
    """
    get_s3_path 함수가 알 수 없는 엔티티에 대해 올바른 S3 경로(팩트 데이터)를 반환하는지 테스트합니다.
    """
    entity_name = "unknown_entity"
    dt_partition = "2023-10-01"
    s3_path = get_s3_path(entity_name, dt_partition)
    assert s3_path == f"raw/{entity_name}/dt={dt_partition}"
