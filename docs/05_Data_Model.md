# 05. 데이터 모델 명세

이 문서는 데이터들이 정확히 어떻게 구성되어 있는지 설명하기 위해 작성되었습니다.

## 데이터 소스 매핑

| IGDB Endpoint | Raw Layer (S3)          | Staging Model   | Mart Model  |
| ------------- | ----------------------- | --------------- | ----------- |
| `/games`      | `raw/games/*.jsonl`     | `stg_games`     | `dim_games` |
| `/platforms`  | `raw/platforms/*.jsonl` | `stg_platforms` | (bridge)    |
| `/genres`     | ...                     | ...             | ...         |

## 최종 스키마: dim_games

| Column Name | Type   | Description          | Source                   |
| ----------- | ------ | -------------------- | ------------------------ |
| game_id     | INT    | Primary key          | stg_games.id             |
| game_name   | STRING | 게임 제목            | stg_games.name           |
| genres      | ARRAY  | 장르 목록 (unnested) | stg_game_genre_bridge    |
| platforms   | ARRAY  | 플랫폼 목록          | stg_game_platform_bridge |
| ...         | ...    | ...                  | ...                      |
