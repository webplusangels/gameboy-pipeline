"""Configuration management for the Game Boy pipeline.

이 파일은 환경 변수를 타입 안전하게 관리합니다.
Pydantic을 사용해서 자동으로 .env 파일을 읽고 검증합니다.

사용법:
    from src.config import settings

    # settings 객체를 통해 환경 변수 접근
    client_id = settings.igdb_client_id
"""

from typing import Literal

from pydantic import Field, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):  # type: ignore[misc]
    """
    애플리케이션 설정 클래스.

    .env 파일의 환경 변수를 자동으로 로드하고 타입 검증합니다.
    필수 필드는 `...`로 표시되며, 없으면 에러가 발생합니다.
    """

    # IGDB API 자격증명 (필수)
    igdb_client_id: str = Field(..., description="IGDB API Client ID")
    igdb_client_secret: str | None = Field(
        default=None, description="IGDB API Client Secret"
    )
    igdb_static_token: str = Field(..., description="IGDB Static Access Token")

    # 환경 설정 (선택, 기본값 있음)
    environment: Literal["development", "staging", "production"] = "development"
    debug: bool = False
    log_level: str = "INFO"

    # Rate limiting (IGDB API 제한: 초당 4회)
    rate_limit_requests_per_second: PositiveInt = Field(
        default=4,
        le=4,  # le = less than or equal to (4 이하)
        description="IGDB API rate limit (max 4 per second)",
    )

    # 데이터베이스 (선택)
    # database_url: str | None = None

    # Redis 캐시 (선택)
    # redis_url: str | None = None

    # AWS 파이프라인 설정
    aws_default_region: str = Field(
        default="ap-northeast-2", description="AWS Default Region"
    )
    aws_access_key_id: str | None = Field(default=None, description="AWS Access Key ID")
    aws_secret_access_key: str | None = Field(
        default=None, description="AWS Secret Access Key"
    )
    s3_bucket_name: str | None = Field(
        default=None, description="S3 Bucket Name for Data Lake"
    )
    cloudfront_domain: str | None = Field(default=None, description="CloudFront Domain")
    cloudfront_distribution_id: str | None = Field(
        default=None, description="CloudFront Distribution ID"
    )

    batch_size: PositiveInt = Field(
        default=50000, description="Batch size for data processing"
    )

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False
    )


# 전역 settings 인스턴스 (import해서 사용)
settings = Settings()
