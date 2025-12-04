import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

try:
    from src.config import settings

    S3_BUCKET_NAME = settings.s3_bucket_name
except ImportError:
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

st.set_page_config(
    page_title="Gameboy Pipeline Dashboard", page_icon=":video_game:", layout="wide"
)

st.title("🕹️ Gameboy Pipeline Dashboard")

# 환경 변수 체크
if not S3_BUCKET_NAME:
    # 로컬 개발용 fallback
    # S3_BUCKET_NAME = "my-gameboy-bucket"
    st.error("S3_BUCKET_NAME 환경 변수가 설정되지 않았습니다.")
    st.stop()


# --- DuckDB 연결 및 설정 ---
@st.cache_resource
def get_db_connection():
    """
    DuckDB 연결을 생성하고 S3 접근 권한을 설정합니다.
    IAM Instance Profile을 사용하므로 별도의 Key 입력이 필요 없습니다.
    """
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL aws; LOAD aws;")

    # EC2 IAM Role의 자격 증명을 자동으로 로드합니다.
    con.execute("CALL load_aws_credentials();")

    return con


con = get_db_connection()

DATA_PATH = f"s3://{S3_BUCKET_NAME}/marts/dim_games/*.parquet"

# --- 데이터 조회 (SQL) ---
# 1. 요약 메트릭 조회
try:
    summary_df = con.execute(f"""
        SELECT
            COUNT(*) as total_games,
            MAX(to_timestamp(updated_at)) as last_updated
        FROM '{DATA_PATH}'
    """).df()

    total_games = summary_df["total_games"][0]
    last_updated = summary_df["last_updated"][0]

except Exception as e:
    st.error(f"S3 데이터 접근 실패: {e}")
    st.info("EC2 IAM Role 권한이나 S3 버킷 경로를 확인해주세요.")
    st.stop()

# --- 대시보드 레이아웃 ---
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("총 게임 수", f"{total_games:,} 개")
with col2:
    if pd.notnull(last_updated):
        st.metric("마지막 업데이트", last_updated.strftime("%Y-%m-%d %H:%M"))
    else:
        st.metric("마지막 업데이트", "-")
with col3:
    st.metric("데이터 소스", "S3 Data Lake (DuckDB Engine)")

st.divider()

# --- 사이드바 필터 ---
st.sidebar.header("🔍 필터 옵션")

# 연도 범위 동적 조회
year_range = con.execute(f"""
    SELECT
        MIN(year(to_timestamp(first_release_date))) as min_year,
        MAX(year(to_timestamp(first_release_date))) as max_year
    FROM '{DATA_PATH}'
    WHERE first_release_date IS NOT NULL
""").df()

min_year = (
    int(year_range["min_year"][0]) if pd.notnull(year_range["min_year"][0]) else 2000
)
max_year = (
    int(year_range["max_year"][0]) if pd.notnull(year_range["max_year"][0]) else 2025
)

selected_years = st.sidebar.slider(
    "출시 연도 범위", min_year, max_year, (min_year, max_year)
)

search_term = st.sidebar.text_input("게임 이름 검색")

# --- 메인 데이터 쿼리 ---
query = f"""
    SELECT
        name as game_name,
        year(to_timestamp(first_release_date)) as release_year,
        platform_names,
        genre_names
    FROM '{DATA_PATH}'
    WHERE release_year BETWEEN {selected_years[0]} AND {selected_years[1]}
"""

if search_term:
    query += f" AND name ILIKE '%{search_term}%'"

# 결과 정렬 및 제한
query += " ORDER BY release_year DESC LIMIT 1000"

with st.spinner("데이터 조회 중..."):
    filtered_df = con.execute(query).df()

if filtered_df.empty:
    st.warning("조건에 맞는 게임이 없습니다.")
else:
    # --- 차트 및 테이블 ---
    col_chart, col_table = st.columns([2, 1])

    with col_chart:
        st.subheader("📊 연도별 출시 현황")
        release_counts = (
            filtered_df["release_year"].value_counts().sort_index().reset_index()
        )
        release_counts.columns = ["Year", "Count"]

        fig = px.bar(
            release_counts,
            x="Year",
            y="Count",
            title="연도별 출시 게임 수",
            color="Count",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_table:
        st.subheader("📋 게임 목록 (Top 1000)")
        st.dataframe(
            filtered_df,
            column_config={
                "game_name": "게임 이름",
                "release_year": "출시 연도",
                "platform_names": "플랫폼",
                "genre_names": "장르",
            },
            hide_index=True,
            use_container_width=True,
        )
