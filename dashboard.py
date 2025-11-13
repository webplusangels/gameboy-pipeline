import streamlit as st
import pandas as pd
import os
import plotly.express as px

# 설정 및 초기화
st.set_page_config(
    page_title="Gameboy Pipeline Dashboard",
    page_icon=":video_game:",
    layout="wide"
)

st.title(" Gameboy Pipeline Dashboard ")

# 환경 변수 로드
try:
    CLOUDFRONT_DOMAIN = st.secrets["CLOUDFRONT_DOMAIN"]
except (KeyError, FileNotFoundError):
    CLOUDFRONT_DOMAIN = os.getenv("CLOUDFRONT_DOMAIN")

if not CLOUDFRONT_DOMAIN:
    st.error("CLOUDFRONT_DOMAIN 환경 변수가 설정되지 않았습니다.")
    st.stop()

# 데이터 로드 (캐싱 포함)
DATA_DIR = f"https://{CLOUDFRONT_DOMAIN}/marts/dim_games/dim_games.parquet"

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    try:
        df = pd.read_parquet(DATA_DIR)

        # 날짜 컬럼 변환
        if 'first_release_date' in df.columns:
            df['release_date'] = pd.to_datetime(df['first_release_date'], unit='s', errors='coerce')
            df['release_year'] = df['release_date'].dt.year

        return df
    except Exception as e:
        st.error(f"데이터를 로드하는 중 오류가 발생했습니다: {e}")
        return None
    
with st.spinner("데이터를 로드하는 중..."):
    df = load_data()

if df is None or df.empty:
    st.warning("데이터가 없습니다.")
    st.stop()

# 대시보드 레이아웃
col1, col2, col3 = st.columns(3)
with col1:
    total_games = len(df)
    st.metric("총 게임 수", f"{total_games:,} 개")
with col2:
    if 'updated_at' in df.columns:
        last_updated = pd.to_datetime(df['updated_at'].max(), unit='s', errors='coerce')
        st.metric("마지막 업데이트", last_updated.strftime("%Y-%m-%d %H:%M"))
with col3:
    st.metric("데이터 소스", "IGDB API")

st.divider()

st.sidebar.header("필터 옵션")

# 출시 연도 필터
min_year = int(df['release_year'].min()) if 'release_year' in df.columns else 2000
max_year = int(df['release_year'].max()) if 'release_year' in df.columns else 2030
selected_years = st.sidebar.slider("출시 연도 범위 선택", min_year, max_year, (2000, 2025))

# 이름 검색
search_term = st.sidebar.text_input("게임 이름 검색")

# 필터 적용
filtered_df = df.copy()
if 'release_year' in filtered_df.columns:
    filtered_df = filtered_df[
        (filtered_df['release_year'] >= selected_years[0]) &
        (filtered_df['release_year'] <= selected_years[1])
    ]

if search_term:
    filtered_df = filtered_df[
        filtered_df['game_name'].str.contains(search_term, case=False, na=False)
    ]

# 메인 차트 및 테이블
col_chart, col_table = st.columns([2, 1])

with col_chart:
    st.subheader("연도별 게임 출시 현황")
    if 'release_year' in filtered_df.columns:
        release_counts = (
            filtered_df['release_year']
            .value_counts()
            .sort_index()
            .reset_index()
        )
        release_counts.columns = ['Year', 'Count']

        fig = px.bar(release_counts, x='Year', y='Count', color='Count', title='연도별 출시된 게임 수')
        st.plotly_chart(fig, width='stretch')

with col_table:
    st.subheader("게임 목록")
    st.dataframe(
        filtered_df[['game_name', 'release_year', 'platform_names', 'genre_names']].rename(
            columns={
                'game_name': '게임 이름',
                'release_year': '출시 연도',
                'platform_names': '플랫폼',
                'genre_names': '장르'
            }
        ).sort_values(by='출시 연도', ascending=False).reset_index(drop=True),
        width='stretch',
        hide_index=True
    )

# 디버깅
# with st.expander("원본 데이터 미리보기"):
#     st.write(df.head())
#     st.write(f"URL: {DATA_DIR}")
#     st.write(f"데이터 행 수: {len(df)}, 열 수: {len(df.columns)}")
#     st.write("데이터 컬럼:", df.columns.tolist())