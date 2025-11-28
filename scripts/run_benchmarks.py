#!/usr/bin/env python3
"""
리팩토링 전후 코드 품질 벤치마크 측정 및 리포트 생성.

이 스크립트는 테스트 커버리지, 코드 복잡도, 린트 이슈 등을 측정하고
JSON(기계용) + Markdown(사람용) 형식으로 저장합니다.

Usage:
    uv run scripts/run_benchmarks.py baseline          # 리팩토링 전 기준선 캡처
    uv run scripts/run_benchmarks.py snapshot phase1   # Phase별 스냅샷
    uv run scripts/run_benchmarks.py compare baseline phase1  # 비교 리포트
    uv run scripts/run_benchmarks.py --help            # 도움말
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

# =============================================================================
# 경로 설정
# =============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
BENCHMARKS_DIR = PROJECT_ROOT / "docs" / "refactoring" / "benchmarks"
DATA_DIR = BENCHMARKS_DIR / "data"
REPORTS_DIR = BENCHMARKS_DIR / "reports"

SRC_DIR = PROJECT_ROOT / "src"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
TESTS_DIR = PROJECT_ROOT / "tests"


# =============================================================================
# 데이터 모델
# =============================================================================


@dataclass
class FileComplexity:
    """파일별 복잡도 정보."""

    file_path: str
    line_count: int
    average_cc: float
    max_cc: float
    max_cc_function: str


@dataclass
class BenchmarkMetrics:
    """벤치마크 측정 결과."""

    timestamp: str
    tag: str

    # 테스트 메트릭 (src/만 측정, scripts/ 제외)
    test_count: int
    coverage_src: float

    # 코드 품질
    mypy_errors: int
    ruff_issues: int

    # 복잡도
    complexity: list[FileComplexity] = field(default_factory=list)

    # 모듈 구조
    src_file_count: int = 0
    avg_file_lines: float = 0.0
    total_src_lines: int = 0

    # 리팩토링 지표
    max_function_lines: int = 0
    functions_over_50_lines: int = 0
    max_function_cc: int = 0
    maintainability_index: float = 0.0

    # 실행 시간
    test_duration_seconds: float = 0.0


# =============================================================================
# 측정 함수들
# =============================================================================


def run_command(cmd: list[str], cwd: Path | None = None) -> tuple[int, str, str]:
    """명령어 실행 후 결과 반환.

    Args:
        cmd: 실행할 명령어 리스트.
        cwd: 작업 디렉토리.

    Returns:
        (return_code, stdout, stderr) 튜플.
    """
    result = subprocess.run(
        cmd,
        cwd=cwd or PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",  # 디코딩 실패 시 대체 문자 사용
    )
    return result.returncode, result.stdout, result.stderr


def get_test_metrics() -> tuple[int, float, float]:
    """pytest 실행하여 테스트 수, 커버리지, 실행 시간 측정.

    Note:
        scripts/는 진입점/글루 코드이므로 커버리지 측정에서 제외합니다.
        E2E 테스트로 검증하는 것이 적절합니다.

    Returns:
        (테스트 수, src 커버리지, 실행 시간)
    """
    # pytest-cov로 src/만 커버리지 측정 (scripts/ 제외)
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "--cov=src",
        "--cov-report=term",
        "-q",
        str(TESTS_DIR),
    ]

    _, stdout, _ = run_command(cmd)

    # 테스트 수 파싱 (예: "42 passed in 1.23s")
    test_count = 0
    duration = 0.0
    match = re.search(r"(\d+) passed", stdout)
    if match:
        test_count = int(match.group(1))

    match = re.search(r"in ([\d.]+)s", stdout)
    if match:
        duration = float(match.group(1))

    # src/ 커버리지 (TOTAL 라인에서 파싱)
    coverage_src = 0.0
    match = re.search(r"TOTAL\s+\d+\s+\d+\s+(\d+)%", stdout)
    if match:
        coverage_src = float(match.group(1))

    return test_count, coverage_src, duration


def get_mypy_errors() -> int:
    """mypy 실행하여 에러 수 카운트.

    Returns:
        mypy 에러 수.
    """
    cmd = [sys.executable, "-m", "mypy", str(SRC_DIR), str(SCRIPTS_DIR)]
    returncode, stdout, _ = run_command(cmd)

    if returncode == 0:
        return 0

    # 에러 라인 카운트 (예: "Found 3 errors in 2 files")
    match = re.search(r"Found (\d+) error", stdout)
    if match:
        return int(match.group(1))

    # 개별 에러 라인 카운트
    error_lines = [line for line in stdout.split("\n") if ": error:" in line]
    return len(error_lines)


def get_ruff_issues() -> int:
    """ruff 실행하여 린트 이슈 수 카운트.

    Returns:
        ruff 이슈 수.
    """
    cmd = [
        sys.executable,
        "-m",
        "ruff",
        "check",
        str(SRC_DIR),
        str(SCRIPTS_DIR),
        "--output-format=json",
    ]
    returncode, stdout, _ = run_command(cmd)

    if returncode == 0:
        return 0

    # stdout이 None이거나 빈 문자열인 경우 처리
    if not stdout:
        return 0

    try:
        issues = json.loads(stdout)
        return len(issues)
    except (json.JSONDecodeError, TypeError):
        # JSON 파싱 실패 시 라인 수로 카운트
        return len([line for line in stdout.split("\n") if line.strip()])


def get_complexity_report() -> list[FileComplexity]:
    """radon으로 Cyclomatic Complexity 측정.

    Note:
        run_benchmarks.py는 평가 도구이므로 분석 대상에서 제외됩니다.

    Returns:
        파일별 복잡도 리스트.
    """
    results: list[FileComplexity] = []

    # src/ 및 scripts/ 파일들 분석 (run_benchmarks.py 제외)
    target_files = list(SRC_DIR.rglob("*.py")) + [
        f for f in SCRIPTS_DIR.rglob("*.py") if f.name != "run_benchmarks.py"
    ]

    for file_path in target_files:
        if "__pycache__" in str(file_path):
            continue

        # radon cc 실행
        cmd = [sys.executable, "-m", "radon", "cc", str(file_path), "-s", "-j"]
        _, stdout, _ = run_command(cmd)

        try:
            data = json.loads(stdout)
            if not data:
                continue

            file_data = data.get(str(file_path), [])
            if not file_data:
                continue

            # 복잡도 계산
            complexities = [item.get("complexity", 0) for item in file_data]
            avg_cc = sum(complexities) / len(complexities) if complexities else 0
            max_cc = max(complexities) if complexities else 0

            # 최고 복잡도 함수 찾기
            max_item = max(file_data, key=lambda x: x.get("complexity", 0))
            max_func = max_item.get("name", "unknown")

            # 라인 수 계산
            line_count = len(file_path.read_text(encoding="utf-8").splitlines())

            results.append(
                FileComplexity(
                    file_path=str(file_path.relative_to(PROJECT_ROOT)),
                    line_count=line_count,
                    average_cc=round(avg_cc, 2),
                    max_cc=max_cc,
                    max_cc_function=f"{max_func} ({max_cc})",
                )
            )
        except (json.JSONDecodeError, KeyError):
            # 파싱 실패 시 기본값
            line_count = len(file_path.read_text(encoding="utf-8").splitlines())
            results.append(
                FileComplexity(
                    file_path=str(file_path.relative_to(PROJECT_ROOT)),
                    line_count=line_count,
                    average_cc=0.0,
                    max_cc=0.0,
                    max_cc_function="N/A",
                )
            )

    # 라인 수 기준 내림차순 정렬
    results.sort(key=lambda x: x.line_count, reverse=True)
    return results


def get_module_stats() -> tuple[int, float, int]:
    """src/pipeline/ 모듈 통계.

    Returns:
        (파일 수, 평균 라인 수, 총 라인 수)
    """
    pipeline_dir = SRC_DIR / "pipeline"
    if not pipeline_dir.exists():
        return 0, 0.0, 0

    py_files = [f for f in pipeline_dir.glob("*.py") if f.name != "__init__.py"]
    if not py_files:
        return 0, 0.0, 0

    total_lines = sum(
        len(f.read_text(encoding="utf-8").splitlines()) for f in py_files
    )
    return len(py_files), round(total_lines / len(py_files), 1), total_lines


def get_maintainability_index() -> float:
    """radon으로 Maintainability Index 측정.

    MI 점수 해석:
        - 100-20: 유지보수 용이
        - 19-10: 보통
        - 9-0: 유지보수 어려움

    Returns:
        평균 Maintainability Index (0-100).
    """
    cmd = [sys.executable, "-m", "radon", "mi", str(SRC_DIR), "-s", "-j"]
    _, stdout, _ = run_command(cmd)

    if not stdout:
        return 0.0

    try:
        data = json.loads(stdout)
        if not data:
            return 0.0

        # 모든 파일의 MI 평균 계산
        mi_values = []
        for _, info in data.items():
            if isinstance(info, dict) and "mi" in info:
                mi_values.append(info["mi"])

        if not mi_values:
            return 0.0

        return round(sum(mi_values) / len(mi_values), 1)
    except (json.JSONDecodeError, KeyError):
        return 0.0


def get_function_stats(complexity_data: list[FileComplexity]) -> tuple[int, int, int]:
    """함수 관련 통계 계산.

    Args:
        complexity_data: 파일별 복잡도 정보 리스트.

    Returns:
        (최대 함수 라인 수, 50줄 초과 함수 수, 최대 CC)
    """
    max_lines = 0
    over_50_count = 0
    max_cc = 0

    # src/ 파일들만 분석 (scripts/ 제외)
    for file_path in SRC_DIR.rglob("*.py"):
        if "__pycache__" in str(file_path):
            continue

        # radon raw로 함수별 라인 수 측정
        cmd = [sys.executable, "-m", "radon", "raw", str(file_path), "-j"]
        _, stdout, _ = run_command(cmd)

        try:
            data = json.loads(stdout)
            if not data:
                continue

            file_data = data.get(str(file_path), {})
            # LOC (Lines of Code) 사용
            loc = file_data.get("loc", 0)
            if loc > max_lines:
                max_lines = loc
        except (json.JSONDecodeError, KeyError):
            pass

    # 복잡도 데이터에서 최대 CC 및 50줄 초과 함수 계산
    for fc in complexity_data:
        # src/ 파일만 (scripts/ 제외)
        if fc.file_path.startswith("src"):
            if fc.line_count > 50:
                over_50_count += 1
            if fc.max_cc > max_cc:
                max_cc = int(fc.max_cc)

    return max_lines, over_50_count, max_cc


# =============================================================================
# 메트릭 수집
# =============================================================================


def collect_metrics(tag: str) -> BenchmarkMetrics:
    """모든 메트릭 수집.

    Args:
        tag: 벤치마크 태그 (예: "baseline", "phase1").

    Returns:
        수집된 메트릭.
    """
    print(f"📊 메트릭 수집 중... (tag: {tag})")

    print("  ├─ 테스트 실행 중...")
    test_count, cov_src, duration = get_test_metrics()

    print("  ├─ mypy 검사 중...")
    mypy_errors = get_mypy_errors()

    print("  ├─ ruff 검사 중...")
    ruff_issues = get_ruff_issues()

    print("  ├─ 복잡도 분석 중...")
    complexity = get_complexity_report()

    print("  ├─ 유지보수성 분석 중...")
    mi = get_maintainability_index()

    print("  ├─ 함수 통계 분석 중...")
    max_func_lines, over_50_count, max_cc = get_function_stats(complexity)

    print("  └─ 모듈 통계 수집 중...")
    file_count, avg_lines, total_lines = get_module_stats()

    return BenchmarkMetrics(
        timestamp=datetime.now().isoformat(),
        tag=tag,
        test_count=test_count,
        coverage_src=cov_src,
        mypy_errors=mypy_errors,
        ruff_issues=ruff_issues,
        complexity=complexity,
        src_file_count=file_count,
        avg_file_lines=avg_lines,
        total_src_lines=total_lines,
        max_function_lines=max_func_lines,
        functions_over_50_lines=over_50_count,
        max_function_cc=max_cc,
        maintainability_index=mi,
        test_duration_seconds=duration,
    )


# =============================================================================
# 저장 및 렌더링
# =============================================================================


def save_json(metrics: BenchmarkMetrics) -> Path:
    """메트릭을 JSON으로 저장.

    Args:
        metrics: 벤치마크 메트릭.

    Returns:
        저장된 파일 경로.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    file_path = DATA_DIR / f"{date_str}_{metrics.tag}.json"

    # dataclass를 dict로 변환
    data = asdict(metrics)

    file_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return file_path


def render_markdown(metrics: BenchmarkMetrics) -> str:
    """메트릭을 Markdown으로 렌더링.

    Args:
        metrics: 벤치마크 메트릭.

    Returns:
        Markdown 문자열.
    """
    # 복잡도 테이블 생성
    complexity_rows = ""
    for fc in metrics.complexity[:10]:  # 상위 10개만
        complexity_rows += (
            f"| {fc.file_path} | {fc.line_count} | "
            f"{fc.average_cc} | {fc.max_cc_function} |\n"
        )

    # MI 등급 계산
    mi = metrics.maintainability_index
    if mi >= 20:
        mi_grade = "A (유지보수 용이)"
    elif mi >= 10:
        mi_grade = "B (보통)"
    else:
        mi_grade = "C (유지보수 어려움)"

    return f"""# Benchmark Report - {metrics.tag}

> 생성 시간: {metrics.timestamp}

## 1. 테스트 메트릭

- 총 테스트 수: **{metrics.test_count}개**
- src/ 커버리지: **{metrics.coverage_src:.1f}%**
- 테스트 실행 시간: **{metrics.test_duration_seconds:.2f}초**

## 2. 코드 품질 메트릭

- mypy 에러: **{metrics.mypy_errors}개**
- ruff 이슈: **{metrics.ruff_issues}개**
- Maintainability Index: **{mi:.1f}** ({mi_grade})

## 3. 코드 복잡도

| 파일 | 라인 수 | 평균 CC | 최고 CC 함수 |
| ---- | ------- | ------- | ------------ |
{complexity_rows}

## 4. 리팩토링 지표

| 지표 | 값 | 목표 |
| ---- | -- | ---- |
| 최대 함수 CC | {metrics.max_function_cc} | < 10 |
| 50줄 초과 파일 수 | {metrics.functions_over_50_lines} | 0 |
| src/ 총 라인 수 | {metrics.total_src_lines} | - |

## 5. 모듈 구조

- src/pipeline/ 파일 수: **{metrics.src_file_count}개**
- 평균 파일 크기: **{metrics.avg_file_lines}줄**

## 6. 요약

| 지표 | 값 | 상태 |
| ---- | -- | ---- |
| 테스트 수 | {metrics.test_count} | - |
| 커버리지 (src/) | {metrics.coverage_src}% | {'✅' if metrics.coverage_src >= 80 else '⚠️'} |
| mypy 에러 | {metrics.mypy_errors} | {'✅' if metrics.mypy_errors == 0 else '⚠️'} |
| ruff 이슈 | {metrics.ruff_issues} | {'✅' if metrics.ruff_issues == 0 else '⚠️'} |
| Maintainability Index | {mi:.1f} | {'✅' if mi >= 20 else '⚠️'} |
| 가장 큰 파일 | {metrics.complexity[0].file_path if metrics.complexity else 'N/A'} ({metrics.complexity[0].line_count if metrics.complexity else 0}줄) | - |
"""


def save_markdown(metrics: BenchmarkMetrics) -> Path:
    """메트릭을 Markdown으로 저장.

    Args:
        metrics: 벤치마크 메트릭.

    Returns:
        저장된 파일 경로.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    file_path = REPORTS_DIR / f"{date_str}_{metrics.tag}.md"

    content = render_markdown(metrics)
    file_path.write_text(content, encoding="utf-8")
    return file_path


# =============================================================================
# 비교 기능
# =============================================================================


def load_metrics(tag: str) -> BenchmarkMetrics | None:
    """저장된 메트릭 로드 (가장 최근 파일).

    Args:
        tag: 벤치마크 태그.

    Returns:
        메트릭 또는 None.
    """
    pattern = f"*_{tag}.json"
    files = sorted(DATA_DIR.glob(pattern), reverse=True)

    if not files:
        print(f"⚠️  '{tag}' 태그의 벤치마크를 찾을 수 없습니다.")
        return None

    data = json.loads(files[0].read_text(encoding="utf-8"))

    # complexity 필드를 FileComplexity 객체로 변환
    complexity_list = [FileComplexity(**c) for c in data.get("complexity", [])]

    return BenchmarkMetrics(
        timestamp=data["timestamp"],
        tag=data["tag"],
        test_count=data["test_count"],
        coverage_src=data.get("coverage_src", data.get("coverage_total", 0.0)),
        mypy_errors=data["mypy_errors"],
        ruff_issues=data["ruff_issues"],
        complexity=complexity_list,
        src_file_count=data.get("src_file_count", 0),
        avg_file_lines=data.get("avg_file_lines", 0.0),
        total_src_lines=data.get("total_src_lines", 0),
        max_function_lines=data.get("max_function_lines", 0),
        functions_over_50_lines=data.get("functions_over_50_lines", 0),
        max_function_cc=data.get("max_function_cc", 0),
        maintainability_index=data.get("maintainability_index", 0.0),
        test_duration_seconds=data.get("test_duration_seconds", 0.0),
    )


def compare_metrics(before_tag: str, after_tag: str) -> None:
    """두 벤치마크 비교 리포트 생성.

    Args:
        before_tag: 이전 벤치마크 태그.
        after_tag: 이후 벤치마크 태그.
    """
    before = load_metrics(before_tag)
    after = load_metrics(after_tag)

    if not before or not after:
        return

    def delta(b: float, a: float) -> str:
        diff = a - b
        if diff > 0:
            return f"+{diff:.1f}"
        elif diff < 0:
            return f"{diff:.1f}"
        return "0"

    def delta_int(b: int, a: int) -> str:
        diff = a - b
        if diff > 0:
            return f"+{diff}"
        elif diff < 0:
            return f"{diff}"
        return "0"

    report = f"""# Comparison Report: {before_tag} → {after_tag}

> 생성 시간: {datetime.now().isoformat()}

## 메트릭 변화

| 지표 | {before_tag} | {after_tag} | 변화 |
| ---- | ------------ | ----------- | ---- |
| 테스트 수 | {before.test_count} | {after.test_count} | {delta_int(before.test_count, after.test_count)} |
| 커버리지 (src/) | {before.coverage_src}% | {after.coverage_src}% | {delta(before.coverage_src, after.coverage_src)}% |
| mypy 에러 | {before.mypy_errors} | {after.mypy_errors} | {delta_int(before.mypy_errors, after.mypy_errors)} |
| ruff 이슈 | {before.ruff_issues} | {after.ruff_issues} | {delta_int(before.ruff_issues, after.ruff_issues)} |
| Maintainability Index | {before.maintainability_index} | {after.maintainability_index} | {delta(before.maintainability_index, after.maintainability_index)} |
| 최대 함수 CC | {before.max_function_cc} | {after.max_function_cc} | {delta_int(before.max_function_cc, after.max_function_cc)} |
| 50줄 초과 파일 | {before.functions_over_50_lines} | {after.functions_over_50_lines} | {delta_int(before.functions_over_50_lines, after.functions_over_50_lines)} |
| src 파일 수 | {before.src_file_count} | {after.src_file_count} | {delta_int(before.src_file_count, after.src_file_count)} |
| 평균 파일 크기 | {before.avg_file_lines}줄 | {after.avg_file_lines}줄 | {delta(before.avg_file_lines, after.avg_file_lines)}줄 |

## 개선 여부

"""

    # 개선 판단
    improvements = []
    regressions = []

    if after.coverage_src > before.coverage_src:
        improvements.append(
            f"✅ 커버리지 향상: {before.coverage_src}% → {after.coverage_src}%"
        )
    elif after.coverage_src < before.coverage_src:
        regressions.append(
            f"⚠️ 커버리지 감소: {before.coverage_src}% → {after.coverage_src}%"
        )

    if after.maintainability_index > before.maintainability_index:
        improvements.append(
            f"✅ 유지보수성 향상: {before.maintainability_index} → {after.maintainability_index}"
        )

    if after.max_function_cc < before.max_function_cc:
        improvements.append(
            f"✅ 최대 복잡도 감소: {before.max_function_cc} → {after.max_function_cc}"
        )

    if after.functions_over_50_lines < before.functions_over_50_lines:
        improvements.append(
            f"✅ 긴 파일 감소: {before.functions_over_50_lines} → {after.functions_over_50_lines}"
        )

    if after.mypy_errors < before.mypy_errors:
        improvements.append(
            f"✅ mypy 에러 감소: {before.mypy_errors} → {after.mypy_errors}"
        )
    elif after.mypy_errors > before.mypy_errors:
        regressions.append(
            f"⚠️ mypy 에러 증가: {before.mypy_errors} → {after.mypy_errors}"
        )

    if after.ruff_issues < before.ruff_issues:
        improvements.append(
            f"✅ ruff 이슈 감소: {before.ruff_issues} → {after.ruff_issues}"
        )
    elif after.ruff_issues > before.ruff_issues:
        regressions.append(
            f"⚠️ ruff 이슈 증가: {before.ruff_issues} → {after.ruff_issues}"
        )

    if improvements:
        report += (
            "### 개선된 항목\n\n" + "\n".join(f"- {i}" for i in improvements) + "\n\n"
        )

    if regressions:
        report += (
            "### 주의 필요 항목\n\n" + "\n".join(f"- {r}" for r in regressions) + "\n\n"
        )

    if not improvements and not regressions:
        report += "변화 없음.\n"

    # 저장
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_path = REPORTS_DIR / f"{date_str}_compare_{before_tag}_vs_{after_tag}.md"
    file_path.write_text(report, encoding="utf-8")

    print(f"✅ 비교 리포트 생성: {file_path}")


# =============================================================================
# CLI
# =============================================================================


def create_parser() -> argparse.ArgumentParser:
    """CLI 파서 생성."""
    parser = argparse.ArgumentParser(
        description="리팩토링 벤치마크 측정 도구",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run scripts/run_benchmarks.py baseline
  uv run scripts/run_benchmarks.py snapshot phase1
  uv run scripts/run_benchmarks.py compare baseline phase1
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="명령어")

    # baseline 명령어
    subparsers.add_parser("baseline", help="리팩토링 전 기준선 캡처")

    # snapshot 명령어
    snapshot_parser = subparsers.add_parser("snapshot", help="현재 상태 스냅샷")
    snapshot_parser.add_argument("tag", help="스냅샷 태그 (예: phase1, final)")

    # compare 명령어
    compare_parser = subparsers.add_parser("compare", help="두 벤치마크 비교")
    compare_parser.add_argument("before", help="이전 벤치마크 태그")
    compare_parser.add_argument("after", help="이후 벤치마크 태그")

    return parser


def main() -> int:
    """메인 진입점.

    Returns:
        종료 코드.
    """
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "baseline":
        metrics = collect_metrics("baseline")
        json_path = save_json(metrics)
        md_path = save_markdown(metrics)
        print("\n✅ Baseline 저장 완료:")
        print(f"   - JSON: {json_path}")
        print(f"   - Markdown: {md_path}")

    elif args.command == "snapshot":
        metrics = collect_metrics(args.tag)
        json_path = save_json(metrics)
        md_path = save_markdown(metrics)
        print(f"\n✅ 스냅샷 저장 완료 (tag: {args.tag}):")
        print(f"   - JSON: {json_path}")
        print(f"   - Markdown: {md_path}")

    elif args.command == "compare":
        compare_metrics(args.before, args.after)

    return 0


if __name__ == "__main__":
    sys.exit(main())
