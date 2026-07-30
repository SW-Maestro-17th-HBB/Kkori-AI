"""agent 소유 마이그레이션 적용 — 배포 시 1회 실행. migrations/README.md 참조.

사용법: cd agent && uv run python scripts/apply_migrations.py
접속 정보는 KKORI_AGENT_DATABASE_URL 환경 변수를 쓴다.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg

DATABASE_URL_ENV = "KKORI_AGENT_DATABASE_URL"
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


def apply_migrations(url: str) -> list[str]:
    """번호 순서대로 멱등 SQL을 적용하고 적용한 파일명을 반환한다."""
    applied: list[str] = []
    with psycopg.connect(url) as conn:
        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            conn.execute(sql_file.read_text(encoding="utf-8"))
            applied.append(sql_file.name)
    return applied


def main() -> int:
    url = os.getenv(DATABASE_URL_ENV)
    if not url:
        print(f"{DATABASE_URL_ENV} 미설정 — 적용할 대상 DB가 없다", file=sys.stderr)
        return 1
    applied = apply_migrations(url)
    for name in applied:
        print(f"applied: {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
