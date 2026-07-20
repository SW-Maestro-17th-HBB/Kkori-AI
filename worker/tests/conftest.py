"""테스트 공용 픽스처 — 실제 PostgreSQL(pgvector) 대상 DB 픽스처.

실 데이터(kkori DB)를 건드리지 않도록 전용 DB(kkori_worker_test)를 만들어 쓴다.
백엔드 소유 테이블(resumes, resume_analysis_status)은 테스트용 최소 형태로 흉내낸다.
로컬 Postgres 가 없으면(예: CI) DB 사용 테스트를 건너뛴다.
"""

import psycopg
import pytest
import pytest_asyncio

from src.config import Settings
from src.contract import AnalysisStatus
from src.storage.repository import connect, ensure_schema

ADMIN_DSN = "postgresql://kkori:kkori@localhost:5432/kkori"
TEST_DB = "kkori_worker_test"
TEST_DSN = f"postgresql://kkori:kkori@localhost:5432/{TEST_DB}"
DIM = 8  # 테스트는 작은 차원으로 충분


def postgres_available() -> bool:
    try:
        with psycopg.connect(ADMIN_DSN, connect_timeout=2):
            return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not postgres_available(), reason="로컬 PostgreSQL(5432) 없음 — DB 테스트 건너뜀"
)


@pytest.fixture(scope="session")
def test_db():
    """전용 테스트 DB 생성 + 백엔드 소유 테이블 흉내."""
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        exists = admin.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,)
        ).fetchone()
        if not exists:
            admin.execute(f"CREATE DATABASE {TEST_DB}")
    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        # 백엔드 소유 테이블의 테스트용 최소 형태 (실환경에선 Spring 이 만든다)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS resumes (
                id BIGSERIAL PRIMARY KEY,
                structured_data JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS resume_analysis_status (
                id BIGSERIAL PRIMARY KEY,
                resume_id BIGINT NOT NULL UNIQUE,
                parse_status VARCHAR(30) NOT NULL,
                parser_version VARCHAR(50),
                error_message TEXT,
                retry_count INT NOT NULL DEFAULT 0,
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                failed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    yield TEST_DSN


@pytest_asyncio.fixture
async def conn(test_db):
    settings = Settings(postgres_dsn=test_db)
    c = await connect(settings)
    await ensure_schema(c, dim=DIM)
    # 테스트 간 격리 — 시작 시 치운다
    await c.execute("TRUNCATE resume_chunks, resume_analysis_status, resumes")
    yield c
    await c.close()


async def seed_resume(conn, status: AnalysisStatus, structured_data=None) -> int:
    """테스트용 이력서 + 상태 레코드 생성. structured_data 는 dict 또는 None."""
    from psycopg.types.json import Jsonb

    cur = await conn.execute(
        "INSERT INTO resumes (structured_data) VALUES (%s) RETURNING id",
        (Jsonb(structured_data) if structured_data is not None else None,),
    )
    rid = (await cur.fetchone())["id"]
    await conn.execute(
        "INSERT INTO resume_analysis_status (resume_id, parse_status) VALUES (%s, %s)",
        (rid, status.value),
    )
    return rid
