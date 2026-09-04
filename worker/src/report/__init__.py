"""리포트 워커 — 독립 앱으로 배포되는 도메인 패키지.

실행: `faststream run src.report.main:app` (이력서 워커 `src.main:app` 과 별개 프로세스).
운영·로컬 모두 프로세스를 분리해 장애·배포를 격리한다 — 공유는 계약(contract)·
AI 제공자(ai)·설정(config) 같은 라이브러리 계층뿐이다.
"""
