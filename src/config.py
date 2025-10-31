"""Configuration management for the Game Boy pipeline.

이 파일은 환경 변수를 타입 안전하게 관리합니다.
Pydantic을 사용해서 자동으로 .env 파일을 읽고 검증합니다.

사용법:
    from src.config import settings

    # settings 객체를 통해 환경 변수 접근
    client_id = settings.igdb_client_id
"""

from typing import Literal

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

# .env 파일에서 환경 변수 로드
load_dotenv()


class Settings(BaseSettings):
    """
    애플리케이션 설정 클래스.

    .env 파일의 환경 변수를 자동으로 로드하고 타입 검증합니다.
    필수 필드는 `...`로 표시되며, 없으면 에러가 발생합니다.
    """

    # IGDB API 자격증명 (필수)
    igdb_client_id: str = Field(..., description="IGDB API Client ID")
    igdb_client_secret: str = Field(..., description="IGDB API Client Secret")

    # 환경 설정 (선택, 기본값 있음)
    environment: Literal["development", "staging", "production"] = "development"
    debug: bool = False
    log_level: str = "INFO"

    # Rate limiting (IGDB API 제한: 초당 4회)
    rate_limit_requests_per_second: int = 4

    # 데이터베이스 (선택)
    database_url: str | None = None

    # Redis 캐시 (선택)
    redis_url: str | None = None

    class ConfigDict:
        """Pydantic 설정."""

        env_file = ".env"  # .env 파일에서 읽기
        case_sensitive = False  # 환경 변수 대소문자 구분 안함


# 전역 settings 인스턴스 (import해서 사용)
settings = Settings()
