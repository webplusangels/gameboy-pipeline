import streamlit as st
import duckdb
import pandas as pd
import os

# 결과물 경로
DB_FILE_PATH = "transform/prod_warehouse.db"

@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    """
    DuckDB 데이터베이스에서 데이터를 로드합니다.

    Returns:
        pd.DataFrame: 로드된 데이터프레임
    """
    if not os.path.exists(DB_FILE_PATH):
        st.error("데이터베이스 파일을 찾을 수 없습니다.")
        return pd.DataFrame()
    
    try:
        conn = duckdb.connect(DB_FILE_PATH, read_only=True)
        query = "SELECT * FROM main_marts.dim_games;"  # 예시 쿼리, 필요에 따라 수정
        df = conn.execute(query).df()
        conn.close()
        return df
    
    except Exception as e:
        st.error(f"데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()
    
st.set_page_config(page_title="게임 대시보드", layout="wide")
st.title("IGDB 데이터 마트")
st.write("`dim_games` 테이블 데이터")

df_games = load_data()

if not df_games.empty:
    st.subheader(f"게임 목록 (총 {len(df_games)})")
    st.dataframe(
        df_games[
            ["game_name", "game_summary", "platform_names", "genre_names"]
        ].head(50)
    )

    st.subheader("플랫폼별 게임 수")
    st.write(
        df_games.explode("platform_names")["platform_names"].value_counts()
    )

    df_platforms = df_games.explode("platform_names")
    platform_counts = df_platforms["platform_names"].value_counts().head(10)

    st.bar_chart(platform_counts)

else:
    st.warning("표시할 데이터가 없습니다.")