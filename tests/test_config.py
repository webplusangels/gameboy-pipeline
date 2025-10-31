def test_settings_loads_from_env(monkeypatch):
    """환경 변수가 제대로 로드되는지 테스트

    Pydantic `Settings`가 현재는 모듈 레벨에서 인스턴스화되기 때문에,
    테스트가 실행되기 전에 생성되어 오류가 발생할 수 있습니다.
    따라서 환경 변수를 설정한 후에 `Settings` 인스턴스를 새로 생성해야 합니다.
    """
    # 필수 환경 변수 설정
    monkeypatch.setenv("IGDB_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("IGDB_CLIENT_SECRET", "test-client-secret")

    from src.config import Settings

    settings = Settings()

    assert settings.igdb_client_id == "test-client-id"
    assert settings.igdb_client_secret == "test-client-secret"
