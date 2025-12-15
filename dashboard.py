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
        self.aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")

        # S3 Paths (특화 마트 경로 추가)
        self.path_dim_games = f"s3://{self.bucket_name}/marts/dim_games/*.parquet"
        self.path_mart_recent = (
            f"s3://{self.bucket_name}/marts/marketing/mart_recent_games.parquet"
        )
        self.path_mart_hyped = (
            f"s3://{self.bucket_name}/marts/marketing/mart_top_hyped.parquet"
        )
        self.path_mart_rated = (
            f"s3://{self.bucket_name}/marts/marketing/mart_top_rated.parquet"
        )
        self.path_mart_themes = (
            f"s3://{self.bucket_name}/marts/marketing/mart_theme_trends.parquet"
        )

    def _get_bucket_name(self) -> str:
        bucket = os.getenv("S3_BUCKET_NAME")
        if not bucket and "S3_BUCKET_NAME" in st.secrets:
            bucket = st.secrets["S3_BUCKET_NAME"]
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


config = AppConfig()


# --- 3. Data Layer (DuckDB) ---
@st.cache_resource
def get_db_connection():
    try:
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL aws; LOAD aws;")
        con.execute(f"SET s3_region='{config.aws_region}';")
        con.execute("CALL load_aws_credentials();")
        logger.info("DuckDB connection established.")
        return con
    except Exception:
        logger.exception("Failed to connect to DuckDB")
        st.error("데이터베이스 연결 실패. 관리자에게 문의하세요.")
        st.stop()


@st.cache_data(ttl=3600)
def fetch_summary_metrics(_con, source_path) -> tuple[int, datetime | None]:
    """전체 게임 수 및 마지막 업데이트 시간 조회 (dim_games 사용)"""
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
        last_updated = df["last_updated"][0]
        if pd.isna(last_updated):
            last_updated = None
        return df["total_games"][0], last_updated
    except Exception as e:
        logger.error(f"Summary query failed: {e}")
        return 0, None


@st.cache_data(ttl=3600)
def fetch_mart_data(_con, source_path, limit: int = 10) -> pd.DataFrame:
    """특화 마트 데이터 조회 (단순 SELECT)"""
    query = f"SELECT * FROM read_parquet('{source_path}') LIMIT ?"
    try:
        return _con.execute(query, [limit]).df()
    except Exception as e:
        logger.warning(f"Mart query failed for {source_path}: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def search_games(_con, source_path, search_term: str, limit: int = 100) -> pd.DataFrame:
    """게임 검색 (dim_games 사용)"""
    if not search_term:
        return pd.DataFrame()
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
    st.sidebar.header("🔍 게임 검색")
    with st.sidebar.form(key="search_form"):
        search_term = st.text_input("게임명 검색", placeholder="예: Mario, Zelda")
        search_submit = st.form_submit_button("검색")

    # --- KPI Section ---
    with st.spinner("Fetching summary..."):
        total_games, last_updated = fetch_summary_metrics(con, config.path_dim_games)

    st.divider()
    m_col1, m_col2, m_col3 = st.columns(3)
    with m_col1:
        st.metric("Total Games", f"{total_games:,}개")
    with m_col2:
        updated_str = last_updated.strftime("%Y-%m-%d %H:%M") if last_updated else "N/A"
        st.metric("Last Updated", updated_str)
    with m_col3:
        st.metric("Source", "IGDB API")

    # --- 1. Recent Games (Mart) ---
    st.divider()
    st.subheader("🆕 최근 출시 게임 TOP 10")
    recent_df = fetch_mart_data(con, config.path_mart_recent, 10)

    if not recent_df.empty:
        # 날짜 포맷팅
        if "first_release_date" in recent_df.columns:
            recent_df["release_date"] = pd.to_datetime(
                recent_df["first_release_date"], unit="s", errors="coerce"
            ).dt.strftime("%Y-%m-%d")

        st.dataframe(
            recent_df,
            column_config={
                "game_name": st.column_config.TextColumn("게임명", width="medium"),
                "release_date": st.column_config.TextColumn("출시일", width="small"),
                "platform_names": st.column_config.ListColumn("플랫폼"),
                "genre_names": st.column_config.ListColumn("장르"),
                "game_summary": st.column_config.TextColumn("설명", width="large"),
                "url": st.column_config.LinkColumn("링크"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("최근 출시 게임 데이터가 없습니다.")

    # --- 2. Top Hyped Games (Mart) ---
    st.divider()
    st.subheader("🔥 현재 기대작 TOP 10")
    hyped_df = fetch_mart_data(con, config.path_mart_hyped, 10)

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
        fig_hype.update_layout(height=400, yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_hype, use_container_width=True)
    else:
        st.info("기대작 데이터가 없습니다.")

    # --- 3. Top Rated & Theme Trends (Mart) ---
    st.divider()
    st.subheader("🏆 명예의 전당 & 트렌드")

    col_rate, col_theme = st.columns(2)

    with col_rate:
        st.markdown("#### ⭐ Top Rated Games")
        rated_df = fetch_mart_data(con, config.path_mart_rated, 10)
        if not rated_df.empty:
            st.dataframe(
                rated_df,
                column_config={
                    "game_name": "게임명",
                    "aggregated_rating": st.column_config.ProgressColumn(
                        "평점", min_value=0, max_value=100, format="%.1f"
                    ),
                    "aggregated_rating_count": "평가 수",
                },
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("평점 데이터가 없습니다.")

    with col_theme:
        st.markdown("#### ☁️ 인기 테마 트렌드")
        theme_df = fetch_mart_data(con, config.path_mart_themes, 50)
        if not theme_df.empty:
            fig_tree = px.treemap(
                theme_df,
                path=["theme"],
                values="count",
                color="count",
                color_continuous_scale="Viridis",
            )
            st.plotly_chart(fig_tree, use_container_width=True)
        else:
            st.info("테마 데이터가 없습니다.")

    # --- Search Result Section ---
    if search_submit and search_term:
        st.divider()
        st.subheader(f"🔎 '{search_term}' 검색 결과")
        with st.spinner("Searching..."):
            search_df = search_games(con, config.path_dim_games, search_term)

        if not search_df.empty:
            st.dataframe(
                search_df,
                column_config={
                    "game_name": "게임명",
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
