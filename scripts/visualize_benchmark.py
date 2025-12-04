"""
벤치마크 결과 시각화 스크립트.

순차 vs 병렬 추출 성능 비교 그래프를 생성합니다.

사용법:
    uv run python scripts/visualize_benchmark.py
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

# 한글 폰트 설정 (Windows)
plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

# 벤치마크 데이터 (실제 측정 결과)
BENCHMARK_DATA = {
    "games": {"sequential": 813.22, "concurrent": 374.28, "records": 344880},
    "platforms": {"sequential": 0.58, "concurrent": 0.18, "records": 220},
    "genres": {"sequential": 0.82, "concurrent": 1.24, "records": 23},
    "game_modes": {"sequential": 0.56, "concurrent": 0.99, "records": 6},
    "player_perspectives": {"sequential": 0.81, "concurrent": 1.00, "records": 7},
    "themes": {"sequential": 0.81, "concurrent": 1.01, "records": 22},
}

OUTPUT_DIR = Path("docs/refactoring/benchmarks/reports")


def create_comparison_bar_chart():
    """순차 vs 병렬 비교 막대 그래프 생성."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # === 1. games 엔티티 (대용량) ===
    ax1 = axes[0]
    games = BENCHMARK_DATA["games"]
    categories = ["Sequential", "Concurrent"]
    times = [games["sequential"], games["concurrent"]]
    colors = ["#FF6B6B", "#4ECDC4"]

    bars = ax1.bar(categories, times, color=colors, edgecolor="black", linewidth=1.2)
    ax1.set_ylabel("Time (seconds)", fontsize=12)
    ax1.set_title(
        f"Games Entity ({games['records']:,} records)\n"
        f"Speedup: {games['sequential'] / games['concurrent']:.2f}x",
        fontsize=14,
        fontweight="bold",
    )

    # 값 표시
    for bar, time in zip(bars, times, strict=False):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 10,
            f"{time:.1f}s",
            ha="center",
            va="bottom",
            fontsize=12,
            fontweight="bold",
        )

    ax1.set_ylim(0, max(times) * 1.15)
    ax1.grid(axis="y", alpha=0.3)

    # === 2. 전체 엔티티 비교 ===
    ax2 = axes[1]
    entities = list(BENCHMARK_DATA.keys())
    sequential_times = [BENCHMARK_DATA[e]["sequential"] for e in entities]
    concurrent_times = [BENCHMARK_DATA[e]["concurrent"] for e in entities]

    x = np.arange(len(entities))
    width = 0.35

    _ = ax2.bar(
        x - width / 2,
        sequential_times,
        width,
        label="Sequential",
        color="#FF6B6B",
        edgecolor="black",
    )
    _ = ax2.bar(
        x + width / 2,
        concurrent_times,
        width,
        label="Concurrent",
        color="#4ECDC4",
        edgecolor="black",
    )

    ax2.set_ylabel("Time (seconds)", fontsize=12)
    ax2.set_title("All Entities Comparison", fontsize=14, fontweight="bold")
    ax2.set_xticks(x)
    ax2.set_xticklabels(entities, rotation=45, ha="right")
    ax2.legend()
    ax2.set_yscale("log")  # 로그 스케일 (games가 너무 큼)
    ax2.grid(axis="y", alpha=0.3)

    plt.tight_layout()

    # 저장
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "benchmark_comparison.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"✅ 저장됨: {output_path}")

    plt.show()


def create_speedup_chart():
    """Speedup 비교 차트 생성."""
    fig, ax = plt.subplots(figsize=(10, 6))

    entities = list(BENCHMARK_DATA.keys())
    speedups = []
    for e in entities:
        seq = BENCHMARK_DATA[e]["sequential"]
        con = BENCHMARK_DATA[e]["concurrent"]
        speedups.append(seq / con if con > 0 else 0)

    colors = ["#4ECDC4" if s >= 1 else "#FF6B6B" for s in speedups]

    bars = ax.barh(entities, speedups, color=colors, edgecolor="black", linewidth=1.2)

    # 1x 기준선
    ax.axvline(x=1, color="gray", linestyle="--", linewidth=2, label="1x (no change)")

    # 값 표시
    for bar, speedup in zip(bars, speedups, strict=False):
        ax.text(
            bar.get_width() + 0.05,
            bar.get_y() + bar.get_height() / 2,
            f"{speedup:.2f}x",
            va="center",
            fontsize=11,
            fontweight="bold",
        )

    ax.set_xlabel("Speedup (Sequential / Concurrent)", fontsize=12)
    ax.set_title(
        "Concurrent Extraction Speedup by Entity\n(Green: Improved, Red: Slower)",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlim(0, max(speedups) * 1.2)
    ax.grid(axis="x", alpha=0.3)

    plt.tight_layout()

    output_path = OUTPUT_DIR / "benchmark_speedup.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"✅ 저장됨: {output_path}")

    plt.show()


def create_summary_dashboard():
    """종합 대시보드 생성."""
    fig = plt.figure(figsize=(16, 10))

    # 레이아웃 설정
    gs = fig.add_gridspec(2, 3, hspace=0.3, wspace=0.3)

    # === 1. 헤드라인 메트릭 (상단 전체) ===
    ax_headline = fig.add_subplot(gs[0, :])
    ax_headline.axis("off")

    games = BENCHMARK_DATA["games"]
    speedup = games["sequential"] / games["concurrent"]
    time_saved = games["sequential"] - games["concurrent"]

    headline_text = (
        f"Performance Improvement Summary\n\n"
        f"Games Entity (344,880 records)\n"
        f"{'=' * 55}\n"
        f"Sequential: {games['sequential']:.1f}s  ->  Concurrent: {games['concurrent']:.1f}s\n"
        f"Speedup: {speedup:.2f}x  |  Time Saved: {time_saved:.1f}s ({time_saved / 60:.1f} min)"
    )

    ax_headline.text(
        0.5,
        0.5,
        headline_text,
        transform=ax_headline.transAxes,
        fontsize=14,
        verticalalignment="center",
        horizontalalignment="center",
        fontfamily="monospace",
        bbox={"boxstyle": "round,pad=0.5", "facecolor": "#f0f0f0", "edgecolor": "gray"},
    )

    # === 2. 시간 비교 막대 그래프 ===
    ax_bars = fig.add_subplot(gs[1, 0])
    categories = ["Sequential", "Concurrent"]
    times = [games["sequential"], games["concurrent"]]
    colors = ["#FF6B6B", "#4ECDC4"]

    bars = ax_bars.bar(categories, times, color=colors, edgecolor="black")
    ax_bars.set_ylabel("Time (seconds)")
    ax_bars.set_title("Games Extraction Time", fontweight="bold")

    for bar, time in zip(bars, times, strict=False):
        ax_bars.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 10,
            f"{time:.0f}s",
            ha="center",
            fontweight="bold",
        )

    # === 3. Speedup 비교 ===
    ax_speedup = fig.add_subplot(gs[1, 1])
    entities = list(BENCHMARK_DATA.keys())
    speedups = [
        BENCHMARK_DATA[e]["sequential"] / BENCHMARK_DATA[e]["concurrent"]
        for e in entities
    ]
    colors = ["#4ECDC4" if s >= 1 else "#FF6B6B" for s in speedups]

    ax_speedup.barh(entities, speedups, color=colors, edgecolor="black")
    ax_speedup.axvline(x=1, color="gray", linestyle="--", linewidth=2)
    ax_speedup.set_xlabel("Speedup")
    ax_speedup.set_title("Speedup by Entity", fontweight="bold")

    # === 4. 처리량 비교 ===
    ax_throughput = fig.add_subplot(gs[1, 2])
    seq_rps = games["records"] / games["sequential"]
    con_rps = games["records"] / games["concurrent"]

    bars = ax_throughput.bar(
        ["Sequential", "Concurrent"],
        [seq_rps, con_rps],
        color=["#FF6B6B", "#4ECDC4"],
        edgecolor="black",
    )
    ax_throughput.set_ylabel("Records/Second")
    ax_throughput.set_title("Throughput (Games)", fontweight="bold")

    for bar, rps in zip(bars, [seq_rps, con_rps], strict=False):
        ax_throughput.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 10,
            f"{rps:.0f}",
            ha="center",
            fontweight="bold",
        )

    plt.suptitle(
        "Extract Pipeline Performance Benchmark",
        fontsize=16,
        fontweight="bold",
        y=0.98,
    )

    output_path = OUTPUT_DIR / "benchmark_dashboard.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"✅ 저장됨: {output_path}")

    plt.show()


def main():
    """메인 함수."""
    print("📊 벤치마크 결과 시각화 시작...\n")

    create_comparison_bar_chart()
    create_speedup_chart()
    create_summary_dashboard()

    print("\n✅ 모든 그래프 생성 완료!")
    print(f"📁 저장 위치: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
