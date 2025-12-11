import os
import sys
from datetime import datetime

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from loguru import logger


# --- 1. 초기 설정 (Logging & Page Config) ---
def setup_logging():
    logger.remove()
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.add(sys.stderr, level=log_level)
    # 로컬 디버깅용 로그 파일 (운영 환경에서는 stdout 권장)
    # logger.add("logs/dashboard_{time:YYYY-MM-DD}.log", rotation="10 MB", level=log_level)


setup_logging()

st.set_page_config(
    page_title="Gameboy Dashboard",
    page_icon=":video_game:",
    layout="wide",
    initial_sidebar_state="expanded",
)


# --- 2. Configuration Class (설정 중앙화) ---
class AppConfig:
    def __init__(self):
        self.bucket_name = self._get_bucket_name()
        self.data_path = f"s3://{self.bucket_name}/marts/dim_games/*.parquet"
        self.aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")

    def _get_bucket_name(self) -> str:
        # 1. 환경변수 확인
        bucket = os.getenv("S3_BUCKET_NAME")
        # 2. streamlit secrets 확인
        if not bucket and "S3_BUCKET_NAME" in st.secrets:
            bucket = st.secrets["S3_BUCKET_NAME"]

        # 3. 로컬 config 모듈 확인 (Optional)
        if not bucket:
            try:
                from src.config import settings

                bucket = settings.s3_bucket_name
            except ImportError:
                pass

        if not bucket:
            st.error("🚨 Critical Error: S3_BUCKET_NAME is missing.")
            st.stop()
        return bucket


# Config 객체 생성 (싱글톤처럼 사용)
config = AppConfig()


# --- 3. Data Layer (DuckDB) ---
@st.cache_resource
def get_db_connection():
    """
    DuckDB 연결 및 AWS 자격 증명 설정 (Resource Caching)
    """
    try:
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL aws; LOAD aws;")

        # Region 설정 명시
        con.execute(f"SET s3_region='{config.aws_region}';")

        # Credential Chain 로드
        con.execute("CALL load_aws_credentials();")

        logger.info("DuckDB connection established.")
        return con
    except Exception:
        logger.exception("Failed to connect to DuckDB")  # Traceback 포함 로그
        st.error("데이터베이스 연결 실패. 관리자에게 문의하세요.")
        st.stop()


@st.cache_data(ttl=3600)
def fetch_summary_metrics(_con, source_path) -> tuple[int, datetime | None]:
    query = f"""
        SELECT
            COUNT(*) as total_games,
            MAX(to_timestamp(updated_at)) as last_updated
        FROM read_parquet('{source_path}')
    """
    try:
        df = _con.execute(query).df()
        if df.empty:
            return 0, None

        # 결과가 NULL인 경우 방어
        last_updated = df["last_updated"][0]
        if pd.isna(last_updated):
            last_updated = None

        return df["total_games"][0], last_updated
    except Exception as e:
        logger.error(f"Summary query failed: {e}")
        return 0, None


@st.cache_data(ttl=3600)
def fetch_year_range(_con, source_path) -> tuple[int, int]:
    """데이터셋의 최소/최대 연도 조회"""
    query = f"""
        SELECT
            MIN(year(to_timestamp(first_release_date))) as min_year,
            MAX(year(to_timestamp(first_release_date))) as max_year
        FROM read_parquet('{source_path}')
        WHERE first_release_date IS NOT NULL
    """
    try:
        df = _con.execute(query).df()
        if df.empty or pd.isna(df["min_year"][0]):
            return 2000, 2025  # 기본값
        return int(df["min_year"][0]), int(df["max_year"][0])
    except Exception as e:
        logger.error(f"Year range query failed: {e}")
        return 2000, 2025


@st.cache_data(ttl=3600)
def fetch_games_by_year(
    _con, source_path, start_year: int, end_year: int
) -> pd.DataFrame:
    # 파라미터 바인딩 사용 (?)
    query = f"""
        SELECT
            year(to_timestamp(first_release_date)) as release_year,
            COUNT(*) as game_count
        FROM read_parquet('{source_path}')
        WHERE year(to_timestamp(first_release_date)) BETWEEN ? AND ?
        GROUP BY release_year
        ORDER BY release_year
    """
    try:
        return _con.execute(query, [start_year, end_year]).df()
    except Exception as e:
        logger.error(f"Year trend query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def fetch_top_hyped_games(_con, source_path, limit: int = 10) -> pd.DataFrame:
    """Hype 점수가 높은 TOP 게임 조회"""
    query = f"""
        SELECT
            game_name,
            hypes,
            platform_names,
            genre_names,
            game_summary
        FROM read_parquet('{source_path}')
        WHERE hypes IS NOT NULL
          AND hypes > 0
        ORDER BY hypes DESC
        LIMIT ?
    """
    try:
        return _con.execute(query, [limit]).df()
    except Exception as e:
        logger.error(f"Top hyped games query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def fetch_recent_games(_con, source_path, limit: int = 10) -> pd.DataFrame:
    """최근 출시된 게임 조회 (현재 시점 기준 이미 출시된 게임만)"""
    query = f"""
        SELECT
            game_name,
            to_timestamp(first_release_date) as release_date,
            year(to_timestamp(first_release_date)) as release_year,
            platform_names,
            genre_names,
            game_summary
        FROM read_parquet('{source_path}')
        WHERE first_release_date IS NOT NULL
          AND to_timestamp(first_release_date) <= CURRENT_TIMESTAMP
        ORDER BY first_release_date DESC
        LIMIT ?
    """
    try:
        return _con.execute(query, [limit]).df()
    except Exception as e:
        logger.error(f"Recent games query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)  # 검색은 자주 바뀔 수 있으므로 TTL 짧게
def search_games(_con, source_path, search_term: str, limit: int = 100) -> pd.DataFrame:
    if not search_term:
        return pd.DataFrame()

    # [수정] 과도한 정규식 제거. SQL Injection은 바인딩으로 해결됨.
    # 양쪽 공백만 제거
    clean_term = search_term.strip()

    query = f"""
        SELECT
            game_name,
            year(to_timestamp(first_release_date)) as release_year,
            platform_names,
            genre_names,
            game_summary
        FROM read_parquet('{source_path}')
        WHERE game_name ILIKE ?
        ORDER BY first_release_date DESC
        LIMIT ?
    """
    try:
        logger.info(f"Searching for: {clean_term}")
        # ILIKE %keyword% 패턴 적용
        return _con.execute(query, [f"%{clean_term}%", limit]).df()
    except Exception as e:
        logger.error(f"Search query failed: {e}")
        return pd.DataFrame()


# --- 4. UI Layer (Main) ---
def main():
    st.title("🕹️ Gameboy Dashboard")
    st.markdown("### Global Game Metrics Monitor")

    con = get_db_connection()

    # --- Sidebar ---
    st.sidebar.header("🔍 필터 옵션")

    # 데이터셋의 연도 범위 가져오기
    min_year, max_year = fetch_year_range(con, config.data_path)

    # [UX 개선] Form을 사용하여 엔터/버튼 클릭 시에만 검색 (리소스 절약)
    with st.sidebar.form(key="search_form"):
        search_term = st.text_input(
            "게임 검색",
            placeholder="예: Mario, Zelda",
        )
        search_submit = st.form_submit_button("검색")

    col_year1, col_year2 = st.sidebar.columns(2)
    with col_year1:
        start_year = st.number_input("시작 연도", min_year, max_year, min_year)
    with col_year2:
        end_year = st.number_input("종료 연도", min_year, max_year, max_year)

    if start_year > end_year:
        st.sidebar.error("⚠️ 시작 연도가 종료 연도보다 클 수 없습니다.")
        start_year, end_year = end_year, start_year

    # --- KPI Section ---
    with st.spinner("Fetching data..."):
        total_games, last_updated = fetch_summary_metrics(con, config.data_path)

    st.divider()
    m_col1, m_col2, m_col3 = st.columns(3)

    with m_col1:
        st.metric("Total Games", f"{total_games:,}개")
    with m_col2:
        updated_str = last_updated.strftime("%Y-%m-%d %H:%M") if last_updated else "N/A"
        st.metric("Last Updated", updated_str)
    with m_col3:
        st.metric("Source", "IGDB API")

    # --- Chart Section ---
    st.divider()
    st.subheader(f"📈 연도별 출시 추이 ({start_year}~{end_year})")

    year_df = fetch_games_by_year(con, config.data_path, start_year, end_year)

    if not year_df.empty:
        fig = px.bar(
            year_df,
            x="release_year",
            y="game_count",
            labels={"release_year": "Year", "game_count": "Games"},
            color="game_count",
            color_continuous_scale="Blues",
        )
        # 차트 높이 조정 및 모바일 대응
        fig.update_layout(height=350, margin={"l": 20, "r": 20, "t": 30, "b": 20})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("해당 기간의 데이터가 없습니다.")

    # --- TOP Hyped Games Section ---
    st.divider()
    st.subheader("🔥 현재 기대작 TOP 10")

    hyped_df = fetch_top_hyped_games(con, config.data_path, 10)

    if not hyped_df.empty:
        fig_hype = px.bar(
            hyped_df,
            x="hypes",
            y="game_name",
            orientation="h",
            labels={"hypes": "Hype Score", "game_name": "Game"},
            color="hypes",
            color_continuous_scale="Reds",
        )
        fig_hype.update_layout(
            height=450,
            margin={"l": 20, "r": 20, "t": 30, "b": 20},
            yaxis={"categoryorder": "total ascending"},
        )
        st.plotly_chart(fig_hype, use_container_width=True)
    else:
        st.info("Hype 데이터가 없습니다. (출시 예정 게임이 없거나 데이터 미수집)")

    # --- Recent Games Section ---
    st.divider()
    st.subheader("🆕 최근 출시 게임 TOP 10")

    recent_df = fetch_recent_games(con, config.data_path, 10)

    if not recent_df.empty:
        # 날짜 포맷 변환
        recent_df["release_date"] = pd.to_datetime(
            recent_df["release_date"]
        ).dt.strftime("%Y-%m-%d")

        st.dataframe(
            recent_df,
            column_config={
                "game_name": st.column_config.TextColumn("게임명", width="medium"),
                "release_date": st.column_config.TextColumn("출시일", width="small"),
                "release_year": st.column_config.NumberColumn("연도", format="%d"),
                "platform_names": st.column_config.ListColumn("플랫폼"),
                "genre_names": st.column_config.ListColumn("장르"),
                "game_summary": st.column_config.TextColumn("설명", width="large"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("출시일 데이터가 없습니다.")

    # --- Search Result Section ---
    # Form 제출 버튼이 눌렸거나, 검색어가 있을 때 실행
    if search_submit and search_term:
        st.divider()
        st.subheader(f"🔎 '{search_term}' 검색 결과")

        with st.spinner("Searching..."):
            search_df = search_games(con, config.data_path, search_term)

        if not search_df.empty:
            st.dataframe(
                search_df,
                column_config={
                    "game_name": st.column_config.TextColumn("게임명", width="medium"),
                    "release_year": st.column_config.NumberColumn(
                        "출시년도", format="%d"
                    ),
                    "platform_names": st.column_config.ListColumn("플랫폼"),
                    "genre_names": st.column_config.ListColumn("장르"),
                    "game_summary": st.column_config.TextColumn("설명", width="large"),
                },
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.warning("검색 결과가 없습니다.")


if __name__ == "__main__":
    main()
