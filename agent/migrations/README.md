# agent 마이그레이션

transcript 소유권(쓰기 권한 경계 = 소유권 경계)에 따라 `interview_transcript`의
DDL·마이그레이션은 agent가 소유한다 — docs/prd/interview-end.md §4.

- 파일은 번호 순서대로 적용하는 멱등 SQL(`CREATE TABLE IF NOT EXISTS` 등)이다.
- 적용은 배포 시 1회 수행한다: `uv run python scripts/apply_migrations.py`
  (접속 정보는 `KKORI_AGENT_DATABASE_URL`). 잡 시작마다 실행하지 않는다.
- Spring 소유 테이블(`interview_session` 등)은 여기서 다루지 않는다.
