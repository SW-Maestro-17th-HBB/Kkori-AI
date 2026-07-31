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
from src.report import repository as report_repository
from src.storage.repository import connect, ensure_schema

ADMIN_DSN = "postgresql://kkori:kkori@localhost:5432/kkori"
TEST_DB = "kkori_worker_test"
TEST_DSN = f"postgresql://kkori:kkori@localhost:5432/{TEST_DB}"
DIM = 8  # 테스트는 작은 차원으로 충분


def postgres_available() -> bool:
    """Postgres 접속 + pgvector 확장 사용 가능 여부까지 확인 (일반 PG 는 스킵되게)."""
    try:
        with psycopg.connect(ADMIN_DSN, connect_timeout=2) as conn:
            row = conn.execute(
                "SELECT 1 FROM pg_available_extensions WHERE name = 'vector'"
            ).fetchone()
            return row is not None
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
        # 리포트 스냅샷 재료 — 기존 테스트 DB 에도 반영되도록 ALTER 로 멱등 추가
        conn.execute(
            "ALTER TABLE resumes ADD COLUMN IF NOT EXISTS original_file_name VARCHAR(255)"
        )
        # 리포트 도메인 — 백엔드 소유 3종 (스키마 원천: Spring report/domain 엔티티)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                interview_session_id BIGINT NOT NULL,
                resume_id BIGINT,
                status VARCHAR(20) NOT NULL,
                overall_score INT,
                delivery_score INT,
                summary TEXT,
                resume_file_name_snapshot VARCHAR(255) NOT NULL,
                weakness_tag_summary JSONB,
                failed_reason TEXT,
                text_analyzed_at TIMESTAMPTZ,
                audio_analyzed_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                deleted_at TIMESTAMPTZ,
                CONSTRAINT uk_reports_interview_session_id UNIQUE (interview_session_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_scores (
                id BIGSERIAL PRIMARY KEY,
                report_id BIGINT NOT NULL,
                logic_score INT NOT NULL,
                specificity_score INT NOT NULL,
                technical_accuracy_score INT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                deleted_at TIMESTAMPTZ,
                CONSTRAINT uk_report_scores_report_id UNIQUE (report_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_feedbacks (
                id BIGSERIAL PRIMARY KEY,
                report_id BIGINT NOT NULL,
                question_number INT NOT NULL,
                logic_score INT NOT NULL,
                specificity_score INT NOT NULL,
                technical_accuracy_score INT NOT NULL,
                feedback TEXT NOT NULL,
                weakness_tags JSONB,
                improvement_tasks JSONB,
                resume_context JSONB,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                deleted_at TIMESTAMPTZ,
                CONSTRAINT uk_report_feedbacks_report_id_question_number
                    UNIQUE (report_id, question_number)
            )
            """
        )
        # 면접 도메인 소유 — 워커는 읽기 전용. interview_session(단수)은 백엔드 실물 엔티티의
        # 최소 형태(워커가 읽는 컬럼만), interview_transcripts 는 에이전트 구현(HBB1-287) 전 잠정
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS interview_session (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                resume_id BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS interview_transcripts (
                id BIGSERIAL PRIMARY KEY,
                interview_session_id BIGINT NOT NULL UNIQUE,
                utterances JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    yield TEST_DSN


@pytest_asyncio.fixture
async def conn(test_db):
    settings = Settings(postgres_dsn=test_db)
    c = await connect(settings)
    await ensure_schema(c, dim=DIM)
    await report_repository.ensure_schema(c)
    # 테스트 간 격리 — 시작 시 치운다
    await c.execute(
        "TRUNCATE resume_chunks, resume_analysis_status, resumes, "
        "reports, report_scores, report_feedbacks, report_generation_jobs, "
        "interview_session, interview_transcripts"
    )
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


async def seed_session(conn, user_id: int = 1, file_name: str = "이력서.pdf") -> tuple[int, int]:
    """테스트용 면접 세션 + 이력서 생성 → (session_id, resume_id)."""
    cur = await conn.execute(
        "INSERT INTO resumes (original_file_name) VALUES (%s) RETURNING id", (file_name,)
    )
    resume_id = (await cur.fetchone())["id"]
    cur = await conn.execute(
        "INSERT INTO interview_session (user_id, resume_id) VALUES (%s, %s) RETURNING id",
        (user_id, resume_id),
    )
    return (await cur.fetchone())["id"], resume_id


async def seed_transcript(conn, session_id: int, utterances: list[dict]) -> None:
    """테스트용 대본(세션당 1행 jsonb) 생성."""
    from psycopg.types.json import Jsonb

    await conn.execute(
        "INSERT INTO interview_transcripts (interview_session_id, utterances) VALUES (%s, %s)",
        (session_id, Jsonb(utterances)),
    )
