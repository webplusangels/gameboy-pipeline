"""
Extract 성능 벤치마크 스크립트.

순차 추출의 베이스라인 성능을 측정합니다.
S3 적재 없이 순수 API 추출 시간만 측정합니다.

사용법:
    # 전체 엔티티 측정
    uv run scripts/benchmark_extract.py

    # 특정 엔티티만 측정
    uv run scripts/benchmark_extract.py --entity games

    # 반복 횟수 지정
    uv run scripts/benchmark_extract.py --entity platforms --runs 3
"""

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter

import httpx
from loguru import logger

from src.config import settings
from src.pipeline.auth import StaticAuthProvider
from src.pipeline.registry import ALL_ENTITIES


@dataclass
class BenchmarkResult:
    """벤치마크 결과 데이터 클래스."""

    entity_name: str
    record_count: int
    elapsed_seconds: float
    records_per_second: float
    api_calls: int
    timestamp: str


async def benchmark_entity(
    entity_name: str,
    extractor_cls: type,
    http_client: httpx.AsyncClient,
    auth_provider: StaticAuthProvider,
    client_id: str,
) -> BenchmarkResult:
    """
    단일 엔티티의 추출 성능을 측정합니다.

    Args:
        entity_name: 엔티티 이름
        extractor_cls: Extractor 클래스
        http_client: HTTP 클라이언트
        auth_provider: 인증 제공자
        client_id: IGDB 클라이언트 ID

    Returns:
        BenchmarkResult: 벤치마크 결과
    """
    extractor = extractor_cls(
        client=http_client,
        auth_provider=auth_provider,
        client_id=client_id,
    )

    logger.info(f"[{entity_name}] 추출 시작...")
    start_time = perf_counter()

    record_count = 0
    api_calls = 0

    async for _ in extractor.extract():
        record_count += 1
        # 페이지 경계에서 API 호출 횟수 추정
        if record_count % extractor.limit == 1:
            api_calls += 1

    elapsed = perf_counter() - start_time
    records_per_second = record_count / elapsed if elapsed > 0 else 0

    result = BenchmarkResult(
        entity_name=entity_name,
        record_count=record_count,
        elapsed_seconds=round(elapsed, 2),
        records_per_second=round(records_per_second, 2),
        api_calls=api_calls,
        timestamp=datetime.now().isoformat(),
    )

    logger.success(
        f"[{entity_name}] 완료: {record_count:,}개 / {elapsed:.2f}초 "
        f"({records_per_second:.2f} rec/sec, {api_calls} API calls)"
    )

    return result


async def run_benchmark(
    entities: list[str] | None = None,
    runs: int = 1,
) -> list[BenchmarkResult]:
    """
    벤치마크를 실행합니다.

    Args:
        entities: 측정할 엔티티 목록 (None이면 전체)
        runs: 반복 횟수

    Returns:
        list[BenchmarkResult]: 벤치마크 결과 목록
    """
    client_id = settings.igdb_client_id
    static_token = settings.igdb_static_token

    if not client_id or not static_token:
        logger.error("IGDB_CLIENT_ID 또는 IGDB_STATIC_TOKEN이 설정되지 않았습니다.")
        sys.exit(1)

    auth_provider = StaticAuthProvider(token=static_token)

    target_entities = entities or list(ALL_ENTITIES.keys())
    all_results: list[BenchmarkResult] = []

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        for run_num in range(1, runs + 1):
            if runs > 1:
                logger.info(f"=== Run {run_num}/{runs} ===")

            for entity_name in target_entities:
                if entity_name not in ALL_ENTITIES:
                    logger.warning(f"알 수 없는 엔티티: {entity_name}")
                    continue

                extractor_cls = ALL_ENTITIES[entity_name]
                result = await benchmark_entity(
                    entity_name=entity_name,
                    extractor_cls=extractor_cls,
                    http_client=http_client,
                    auth_provider=auth_provider,
                    client_id=client_id,
                )
                all_results.append(result)

    return all_results


def save_results(results: list[BenchmarkResult], output_path: Path) -> None:
    """결과를 JSON 파일로 저장합니다."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "benchmark_type": "extract_sequential",
        "timestamp": datetime.now().isoformat(),
        "environment": {
            "python_version": sys.version,
            "platform": sys.platform,
        },
        "results": [asdict(r) for r in results],
        "summary": {
            "total_records": sum(r.record_count for r in results),
            "total_elapsed_seconds": round(sum(r.elapsed_seconds for r in results), 2),
            "total_api_calls": sum(r.api_calls for r in results),
        },
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"결과 저장: {output_path}")


def print_summary(results: list[BenchmarkResult]) -> None:
    """결과 요약을 출력합니다."""
    print("\n" + "=" * 60)
    print("📊 Extract 벤치마크 결과 (순차 처리)")
    print("=" * 60)
    print(f"{'엔티티':<25} {'레코드':>10} {'시간(초)':>10} {'rec/sec':>10}")
    print("-" * 60)

    for r in results:
        print(
            f"{r.entity_name:<25} {r.record_count:>10,} {r.elapsed_seconds:>10.2f} {r.records_per_second:>10.2f}"
        )

    print("-" * 60)
    total_records = sum(r.record_count for r in results)
    total_time = sum(r.elapsed_seconds for r in results)
    avg_rps = total_records / total_time if total_time > 0 else 0

    print(f"{'총계':<25} {total_records:>10,} {total_time:>10.2f} {avg_rps:>10.2f}")
    print("=" * 60)


def main() -> None:
    """메인 함수."""
    parser = argparse.ArgumentParser(description="Extract 성능 벤치마크")
    parser.add_argument(
        "--entity",
        type=str,
        help="측정할 엔티티 이름 (예: games, platforms). 미지정시 전체 측정",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="반복 횟수 (기본: 1)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="결과 저장 경로 (예: .benchmarks/results/extract_baseline.json)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="상세 로그 숨기기",
    )
    args = parser.parse_args()

    # 로깅 설정
    logger.remove()
    if not args.quiet:
        logger.add(sys.stderr, level="INFO")
    else:
        logger.add(sys.stderr, level="WARNING")

    # 엔티티 목록
    entities = [args.entity] if args.entity else None

    # 벤치마크 실행
    logger.info("=== Extract 벤치마크 시작 ===")
    results = asyncio.run(run_benchmark(entities=entities, runs=args.runs))

    # 결과 출력
    print_summary(results)

    # 결과 저장
    if args.output:
        save_results(results, Path(args.output))
    else:
        # 기본 경로에 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_path = Path(
            f"docs/refactoring/benchmarks/data/extract_sequential_{timestamp}.json"
        )
        save_results(results, default_path)


if __name__ == "__main__":
    main()
