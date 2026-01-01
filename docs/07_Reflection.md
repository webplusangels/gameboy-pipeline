1. 아키텍처 결정 기록 (ADR, Architecture Decision Record):

   - 왜 Airflow 대신 GitHub Actions를 썼는가? -> 비용 0원, 관리 포인트 최소화, 하루 1회 배치에는 충분.
   - 왜 RDBMS 대신 DuckDB + S3인가? -> 서버리스 데이터 레이크 구현, OLAP 분석 쿼리에 최적화.
   - 왜 Synchronous 대신 Asyncio인가? -> 네트워크 I/O 대기 시간 최소화로 수집 속도 1,700배 개선 (벤치마크 결과 첨부).

2. 데이터 흐름도 (Data Lineage):

   - IGDB API -> Extract (Python) -> S3 (Raw JSON) -> dbt (DuckDB) -> Streamlit

3. 트러블 슈팅 & 배운 점 (Learning Log):

   - 비동기 처리 시 Rate Limit 걸렸던 경험과 해결책 (tenacity 도입 배경).
   - DuckDB가 S3 파일을 읽을 때의 메모리 이슈나 성능 최적화 경험.
