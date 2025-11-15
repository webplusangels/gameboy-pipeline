# 프로젝트 요약

**작고 빠른 게임 데이터 파이프라인**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![uv](https://img.shields.io/badge/uv-latest-purple.svg)](https://github.com/astral-sh/uv)
[![dbt](https://img.shields.io/badge/dbt-DuckDB-orange.svg)](https://www.getdbt.com/)

[![CI Status](https://github.com/webplusangels/gameboy-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/webplusangels/gameboy-pipeline/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/webplusangels/gameboy-pipeline/graph/badge.svg)](https://codecov.io/gh/webplusangels/gameboy-pipeline)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://gameboy-pipeline-f9xnvfsfs54eapgwteldez.streamlit.app/)

## 라이브 데모

## 아키텍처 다이어그램

> 서버리스(Serverless)와 저비용을 목표로 설계된 모델입니다.

![Pipeline_v1](./docs/imgs/Pipeline%20Diagram%201114.png)

<details>

<summary>`dbt` 리니지 그래프</summary>

**2025.11.10 - ELT 구현 직후**

![2025.11.10](./docs/imgs/202511101913-Lineage-Graph.png)

**2025.11.13 - 배포 모델**

![2025.11.13](./docs/imgs/202511132008-Lineage-Graph.png)

</details>

## 왜 만들었는가

> **문제:** 기존 [game-pricing-pipeline](링크)에서 경험한 유지보수의 어려움  
> **해결:** TDD와 타입 안정성을 바탕으로 한 현대적인 재설계

[game-pricing-pipeline](https://github.com/webplusangels/game-pricing-pipeline)은 인기 게임들의 정보와 특가 정보를 제공하고, 간단하지만 편리한 팀 파인드 서비스를 제공하는 프로젝트인 [WARA:B](https://github.com/100-hours-a-week/9-team-gudokjohayo-warab-be)(백엔드 레포)의 일부로, 운영을 위한 데이터 수집, 업데이트 등 프로젝트에 필요한 데이터 관련 작업을 자동화하기 위해 개발된 파이프라인 모듈입니다.

초기 개발에서 집중한 기능 완성과는 별개로, 실제 운영 과정에서는 다음과 같은 **문제점**들이 발견되었습니다.

- ❌ 코드의 가독성과 낮은 유지 보수성
- ❌ 긴 실행 시간 (배치 작업 최적화 부족)
- ❌ 테스트 안정성 부재 (트러블슈팅 시간 증가)

이 파이프라인은 다음과 같은 **목표**를 가지고 개발되었습니다.

- ✅ 하나의 안정적인 진실 공급원(IGDB API)
- ✅ 가볍고 현대적인 구조 (Interface 기반 설계)
- ✅ TDD 개발 방식 (RED-GREEN-REFACTOR)
- ✅ 타입 안정성 (mypy strict mode)
- ✅ 데이터 기반 점진적 성능 향상
- ✅ 체계적인 문서화

자세한 설명은 문서를 [**참조**](./docs/01_Project.md)해주세요.

## 성능 요약

## Quick Start

## 링크

- [01. 프로젝트와 구조](./docs/01_Project.md)
- [02. 기술 스택](./docs/02_Tech_Stacks.md)
- [03. 학습 기록](./docs/03_Learning_Log.md)
- [04. 성능 측정](./docs/04_Performance.md)
- [05. 데이터 모델](./docs/05_Data_Model.md)
- [개발 방식](./CONTRIBUTING.md)

## 타임라인

- **2025.11.01**: [IGDB API](https://www.igdb.com/)를 활용한 ELT 파이프라인 개발 시작
- **2025.11.03**: Extractor 모듈
- **2025.11.04**: Loader 모듈
- **2025.11.05**: 차원 데이터를 위한 Extractor로 확장
- **2025.11.07**: `dbt` Transform 레이어
- **2025.11.08**: E2E 테스트
- **2025.11.10**: 벤치마크 테스트
- **2025.11.11**: 증분 업데이트
- **2025.11.11**: 오케스트레이션 스크립트 작성
- **2025.11.13**: `Streamlit` [배포](https://gameboy-pipeline-f9xnvfsfs54eapgwteldez.streamlit.app/)
