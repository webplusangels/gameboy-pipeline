from src.pipeline.constants import DIMENSION_ENTITIES

def get_s3_path(entity_name: str, dt_partition: str) -> str:
    """
    엔티티 타입에 따라 S3 경로를 반환합니다.

    Args:
        entity_name (str): 엔티티 이름.
        dt_partition (str): 날짜 파티션 문자열(예: '2023-10-01').
    Returns:
        str: S3 경로 문자열.
    """
    if entity_name in DIMENSION_ENTITIES:
        return f"raw/dimensions/{entity_name}"
    else:
        return f"raw/{entity_name}/dt={dt_partition}"
