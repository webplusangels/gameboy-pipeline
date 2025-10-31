def test_settings_loads_from_env():
    """환경 변수가 제대로 로드되는지 테스트"""
    from src.config import settings

    assert settings.igdb_client_id
    assert settings.igdb_client_secret
