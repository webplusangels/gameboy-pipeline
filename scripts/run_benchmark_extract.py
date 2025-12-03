"""
Extract 성능 벤치마크 스크립트.

순차 추출과 병렬 추출의 성능을 비교 측정합니다.
S3 적재 없이 순수 API 추출 시간만 측정합니다.

사용법:
    # 전체 엔티티 측정 (순차 + 병렬 비교)
    uv run python scripts/benchmark_extract.py

    # 특정 엔티티만 측정
    uv run python scripts/benchmark_extract.py --entity platforms

    # 순차만 측정
    uv run python scripts/benchmark_extract.py --mode sequential

    # 병렬만 측정
    uv run python scripts/benchmark_extract.py --mode concurrent

    # 반복 횟수 지정
    uv run python scripts/benchmark_extract.py --entity platforms --runs 3

    # 병렬 배치 크기 지정
    uv run python scripts/benchmark_extract.py --batch-size 8
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
from src.pipeline.rate_limiter import IgdbRateLimiter
from src.pipeline.registry import ALL_ENTITIES


@dataclass
class BenchmarkResult:
    """벤치마크 결과 데이터 클래스."""

    entity_name: str
    mode: str  # "sequential" or "concurrent"
    record_count: int
    elapsed_seconds: float
    records_per_second: float
    api_calls: int
    batch_size: int | None  # concurrent 모드에서만 사용
    timestamp: str


@dataclass
class ComparisonResult:
    """순차/병렬 비교 결과 데이터 클래스."""

    entity_name: str
    sequential_elapsed: float
    concurrent_elapsed: float
    speedup: float
    record_count: int


async def benchmark_entity_sequential(
    entity_name: str,
    extractor_cls: type,
    http_client: httpx.AsyncClient,
    auth_provider: StaticAuthProvider,
    client_id: str,
) -> BenchmarkResult:
    """
    단일 엔티티의 순차 추출 성능을 측정합니다.

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

    logger.info(f"[{entity_name}] 순차 추출 시작...")
    start_time = perf_counter()

    record_count = 0
    api_calls = 0

    async for _ in extractor.extract():
        record_count += 1
        if record_count % extractor.limit == 1:
            api_calls += 1

    elapsed = perf_counter() - start_time
    records_per_second = record_count / elapsed if elapsed > 0 else 0

    result = BenchmarkResult(
        entity_name=entity_name,
        mode="sequential",
        record_count=record_count,
        elapsed_seconds=round(elapsed, 2),
        records_per_second=round(records_per_second, 2),
        api_calls=api_calls,
        batch_size=None,
        timestamp=datetime.now().isoformat(),
    )

    logger.success(
        f"[{entity_name}] 순차 완료: {record_count:,}개 / {elapsed:.2f}초 "
        f"({records_per_second:.2f} rec/sec, {api_calls} API calls)"
    )

    return result


async def benchmark_entity_concurrent(
    entity_name: str,
    extractor_cls: type,
    http_client: httpx.AsyncClient,
    auth_provider: StaticAuthProvider,
    client_id: str,
    rate_limiter: IgdbRateLimiter,
    batch_size: int = 4,
) -> BenchmarkResult:
    """
    단일 엔티티의 병렬 추출 성능을 측정합니다.

    Args:
        entity_name: 엔티티 이름
        extractor_cls: Extractor 클래스
        http_client: HTTP 클라이언트
        auth_provider: 인증 제공자
        client_id: IGDB 클라이언트 ID
        rate_limiter: API 호출 속도 제한기
        batch_size: 동시 요청 페이지 수

    Returns:
        BenchmarkResult: 벤치마크 결과
    """
    extractor = extractor_cls(
        client=http_client,
        auth_provider=auth_provider,
        client_id=client_id,
        rate_limiter=rate_limiter,
    )

    logger.info(f"[{entity_name}] 병렬 추출 시작 (batch_size={batch_size})...")
    start_time = perf_counter()

    record_count = 0
    api_calls = 0

    async for _ in extractor.extract_concurrent(batch_size=batch_size):
        record_count += 1
        if record_count % extractor.limit == 1:
            api_calls += 1

    elapsed = perf_counter() - start_time
    records_per_second = record_count / elapsed if elapsed > 0 else 0

    result = BenchmarkResult(
        entity_name=entity_name,
        mode="concurrent",
        record_count=record_count,
        elapsed_seconds=round(elapsed, 2),
        records_per_second=round(records_per_second, 2),
        api_calls=api_calls,
        batch_size=batch_size,
        timestamp=datetime.now().isoformat(),
    )

    logger.success(
        f"[{entity_name}] 병렬 완료: {record_count:,}개 / {elapsed:.2f}초 "
        f"({records_per_second:.2f} rec/sec, {api_calls} API calls)"
    )

    return result


async def run_benchmark(
    entities: list[str] | None = None,
    mode: str = "both",
    runs: int = 1,
    batch_size: int = 4,
) -> tuple[list[BenchmarkResult], list[ComparisonResult]]:
    """
    벤치마크를 실행합니다.

    Args:
        entities: 측정할 엔티티 목록 (None이면 전체)
        mode: "sequential", "concurrent", "both"
        runs: 반복 횟수
        batch_size: 병렬 추출 시 동시 요청 페이지 수

    Returns:
        tuple: (벤치마크 결과 목록, 비교 결과 목록)
    """
    client_id = settings.igdb_client_id
    static_token = settings.igdb_static_token

    if not client_id or not static_token:
        logger.error("IGDB_CLIENT_ID 또는 IGDB_STATIC_TOKEN이 설정되지 않았습니다.")
        sys.exit(1)

    auth_provider = StaticAuthProvider(token=static_token)
    rate_limiter = IgdbRateLimiter(max_concurrency=4, requests_per_second=4)

    target_entities = entities or list(ALL_ENTITIES.keys())
    all_results: list[BenchmarkResult] = []
    comparisons: list[ComparisonResult] = []

    async with httpx.AsyncClient(timeout=120.0) as http_client:
        for run_num in range(1, runs + 1):
            if runs > 1:
                logger.info(f"=== Run {run_num}/{runs} ===")

            for entity_name in target_entities:
                if entity_name not in ALL_ENTITIES:
                    logger.warning(f"알 수 없는 엔티티: {entity_name}")
                    continue

                extractor_cls = ALL_ENTITIES[entity_name]
                seq_result = None
                con_result = None

                # 순차 추출
                if mode in ("sequential", "both"):
                    seq_result = await benchmark_entity_sequential(
                        entity_name=entity_name,
                        extractor_cls=extractor_cls,
                        http_client=http_client,
                        auth_provider=auth_provider,
                        client_id=client_id,
                    )
                    all_results.append(seq_result)

                # 병렬 추출
                if mode in ("concurrent", "both"):
                    con_result = await benchmark_entity_concurrent(
                        entity_name=entity_name,
                        extractor_cls=extractor_cls,
                        http_client=http_client,
                        auth_provider=auth_provider,
                        client_id=client_id,
                        rate_limiter=rate_limiter,
                        batch_size=batch_size,
                    )
                    all_results.append(con_result)

                # 비교 결과 생성
                if seq_result and con_result:
                    speedup = (
                        seq_result.elapsed_seconds / con_result.elapsed_seconds
                        if con_result.elapsed_seconds > 0
                        else 0
                    )
                    comparisons.append(
                        ComparisonResult(
                            entity_name=entity_name,
                            sequential_elapsed=seq_result.elapsed_seconds,
                            concurrent_elapsed=con_result.elapsed_seconds,
                            speedup=round(speedup, 2),
                            record_count=seq_result.record_count,
                        )
                    )

    return all_results, comparisons


def save_results(
    results: list[BenchmarkResult],
    comparisons: list[ComparisonResult],
    output_path: Path,
    mode: str,
    batch_size: int,
) -> None:
    """결과를 JSON 파일로 저장합니다."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 모드별 결과 분리
    seq_results = [r for r in results if r.mode == "sequential"]
    con_results = [r for r in results if r.mode == "concurrent"]

    data = {
        "benchmark_type": f"extract_{mode}",
        "timestamp": datetime.now().isoformat(),
        "environment": {
            "python_version": sys.version,
            "platform": sys.platform,
        },
        "config": {
            "mode": mode,
            "batch_size": batch_size,
            "rate_limit": {
                "max_concurrency": 4,
                "requests_per_second": 4,
            },
        },
        "results": {
            "sequential": [asdict(r) for r in seq_results] if seq_results else None,
            "concurrent": [asdict(r) for r in con_results] if con_results else None,
        },
        "comparisons": [asdict(c) for c in comparisons] if comparisons else None,
        "summary": {
            "sequential": {
                "total_records": sum(r.record_count for r in seq_results),
                "total_elapsed_seconds": round(
                    sum(r.elapsed_seconds for r in seq_results), 2
                ),
                "total_api_calls": sum(r.api_calls for r in seq_results),
            }
            if seq_results
            else None,
            "concurrent": {
                "total_records": sum(r.record_count for r in con_results),
                "total_elapsed_seconds": round(
                    sum(r.elapsed_seconds for r in con_results), 2
                ),
                "total_api_calls": sum(r.api_calls for r in con_results),
            }
            if con_results
            else None,
            "average_speedup": (
                round(sum(c.speedup for c in comparisons) / len(comparisons), 2)
                if comparisons
                else None
            ),
        },
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"결과 저장: {output_path}")


def print_summary(
    results: list[BenchmarkResult], comparisons: list[ComparisonResult], mode: str
) -> None:
    """결과 요약을 출력합니다."""
    seq_results = [r for r in results if r.mode == "sequential"]
    con_results = [r for r in results if r.mode == "concurrent"]

    print("\n" + "=" * 80)
    if mode == "both":
        print("📊 Extract 벤치마크 결과 (순차 vs 병렬 비교)")
    elif mode == "sequential":
        print("📊 Extract 벤치마크 결과 (순차 처리)")
    else:
        print("📊 Extract 벤치마크 결과 (병렬 처리)")
    print("=" * 80)

    if comparisons:
        # 비교 모드
        print(
            f"{'엔티티':<25} {'레코드':>10} {'순차(초)':>10} {'병렬(초)':>10} {'Speedup':>10}"
        )
        print("-" * 80)

        for c in comparisons:
            print(
                f"{c.entity_name:<25} {c.record_count:>10,} "
                f"{c.sequential_elapsed:>10.2f} {c.concurrent_elapsed:>10.2f} "
                f"{c.speedup:>9.2f}x"
            )

        print("-" * 80)

        total_records = sum(c.record_count for c in comparisons)
        total_seq = sum(c.sequential_elapsed for c in comparisons)
        total_con = sum(c.concurrent_elapsed for c in comparisons)
        total_speedup = total_seq / total_con if total_con > 0 else 0

        print(
            f"{'총계':<25} {total_records:>10,} "
            f"{total_seq:>10.2f} {total_con:>10.2f} "
            f"{total_speedup:>9.2f}x"
        )

    else:
        # 단일 모드
        target_results = seq_results or con_results
        print(f"{'엔티티':<25} {'레코드':>10} {'시간(초)':>10} {'rec/sec':>10}")
        print("-" * 80)

        for r in target_results:
            print(
                f"{r.entity_name:<25} {r.record_count:>10,} "
                f"{r.elapsed_seconds:>10.2f} {r.records_per_second:>10.2f}"
            )

        print("-" * 80)
        total_records = sum(r.record_count for r in target_results)
        total_time = sum(r.elapsed_seconds for r in target_results)
        avg_rps = total_records / total_time if total_time > 0 else 0

        print(f"{'총계':<25} {total_records:>10,} {total_time:>10.2f} {avg_rps:>10.2f}")

    print("=" * 80)


def main() -> None:
    """메인 함수."""
    parser = argparse.ArgumentParser(description="Extract 성능 벤치마크")
    parser.add_argument(
        "--entity",
        type=str,
        help="측정할 엔티티 이름 (예: games, platforms). 미지정시 전체 측정",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["sequential", "concurrent", "both"],
        default="both",
        help="측정 모드: sequential(순차), concurrent(병렬), both(비교) (기본: both)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=4,
        help="병렬 추출 시 동시 요청 페이지 수 (기본: 4)",
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
        help="결과 저장 경로 (예: docs/refactoring/benchmarks/data/result.json)",
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
    logger.info(f"=== Extract 벤치마크 시작 (mode={args.mode}) ===")
    results, comparisons = asyncio.run(
        run_benchmark(
            entities=entities,
            mode=args.mode,
            runs=args.runs,
            batch_size=args.batch_size,
        )
    )

    # 결과 출력
    print_summary(results, comparisons, args.mode)

    # 결과 저장
    if args.output:
        output_path = Path(args.output)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(
            f"docs/refactoring/benchmarks/data/extract_{args.mode}_{timestamp}.json"
        )

    save_results(results, comparisons, output_path, args.mode, args.batch_size)


if __name__ == "__main__":
    main()
