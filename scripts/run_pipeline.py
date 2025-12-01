import argparse
import asyncio
import sys

from loguru import logger

from src.config import settings
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.loaders import S3Loader
from src.pipeline.orchestrator import PipelineOrchestrator
from src.pipeline.registry import ALL_ENTITIES
from src.pipeline.s3_ops import create_clients
from src.pipeline.state import S3StateManager


def setup_logging() -> None:
    """
    로깅 설정을 초기화합니다.
    """
    logger.remove()
    log_level = settings.log_level.upper()
    logger.add(sys.stderr, level=log_level)

    logger.add(
        "logs/pipeline_{time:YYYY-MM-DD-HH-mm-ss}.log",
        rotation="10 MB",
        compression="zip",
        level=log_level,
    )


async def main(full_refresh: bool = False, target_date: str | None = None) -> None:
    """
    EL 파이프라인의 실행 진입점입니다.
    """
    setup_logging()

    logger.info("=== 데이터 레이크 적재 파이프라인 시작 ===")

    client_id = settings.igdb_client_id
    static_token = settings.igdb_static_token
    bucket_name = settings.s3_bucket_name

    if not all([client_id, static_token, bucket_name]):
        logger.error("필수 환경 변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return

    assert client_id is not None
    assert static_token is not None
    assert bucket_name is not None

    auth_provider = StaticAuthProvider(token=static_token)

    async with create_clients() as (http_client, s3_client, cloudfront_client):
        extractors = {
            entity_name: entity_extractor(
                client=http_client, auth_provider=auth_provider, client_id=client_id
            )
            for entity_name, entity_extractor in ALL_ENTITIES.items()
        }

        orchestrator = PipelineOrchestrator(
            s3_client=s3_client,
            cloudfront_client=cloudfront_client,
            extractors=extractors,
            loader=S3Loader(client=s3_client, bucket_name=bucket_name),
            state_manager=S3StateManager(client=s3_client, bucket_name=bucket_name),
            bucket_name=bucket_name,
            cloudfront_distribution_id=settings.cloudfront_distribution_id,
        )

        results = await orchestrator.run(
            full_refresh=full_refresh,
            target_date=target_date,
        )

        total_records = sum(r.record_count for r in results)
        total_time = sum(r.elapsed_seconds for r in results)
        logger.success("=== 데이터 레이크 적재 파이프라인 완료 ===")
        logger.success(f"총 처리된 레코드 수: {total_records}")
        logger.success(f"총 소요 시간: {total_time:.2f}초")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="적재 파이프라인 실행")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="전체 로드를 수행합니다. 지정하지 않으면 증분 업데이트가 수행됩니다.",
    )
    parser.add_argument(
        "--date",
        type=str,
        help="데이터를 적재할 날짜 파티션을 지정합니다 (형식: YYYY-MM-DD). 지정하지 않으면 현재 날짜가 사용됩니다.",
    )
    args = parser.parse_args()
    asyncio.run(main(full_refresh=args.full_refresh, target_date=args.date))
