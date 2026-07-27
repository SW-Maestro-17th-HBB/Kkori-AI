"""transcript DB flush 테스트 — 로컬 Postgres 없으면 실쿼리 테스트만 skip (worker 패턴).

CI에서는 agent-ci.yml의 Postgres 서비스 컨테이너로 전부 실행된다.
DDL은 agent 소유 마이그레이션(migrations/)을 그대로 적용해 검증한다.
"""

import asyncio
import json
import random
from datetime import datetime, timezone

import pytest

from src.interview.conversation_log import ConversationLog, QuestionType
from src.interview.transcript_store import DATABASE_URL_ENV, flush_transcript

LOCAL_URL = "postgresql://postgres:postgres@localhost:5432/postgres"
NOW = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)


def _pg_available() -> bool:
    import psycopg

    try:
        with psycopg.connect(LOCAL_URL, connect_timeout=2):
            return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(not _pg_available(), reason="로컬 Postgres(5432) 없음")


def _sample_content() -> list[dict]:
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.INITIAL, content="자기소개 부탁드립니다.", spoken_at=NOW,
    )
    log.append_answer("백엔드 지망입니다.", NOW)
    log.append_closing("오늘 면접은 여기까지입니다. 수고 많으셨습니다.", NOW)
    return [u.to_json_dict() for u in log.utterances]


def _fetch_content(session_id: int):
    import psycopg

    with psycopg.connect(LOCAL_URL) as conn:
        row = conn.execute(
            "SELECT content FROM interview_transcript WHERE session_id = %s",
            (session_id,),
        ).fetchone()
    return row[0] if row else None


def _cleanup(session_id: int) -> None:
    import psycopg

    with psycopg.connect(LOCAL_URL) as conn:
        conn.execute(
            "DELETE FROM interview_transcript WHERE session_id = %s", (session_id,)
        )


@pytest.fixture
def migrated_db(monkeypatch):
    """agent 소유 마이그레이션을 적용한 DB — 테스트가 스키마 정의를 그대로 검증한다."""
    import sys
    from pathlib import Path

    monkeypatch.setenv(DATABASE_URL_ENV, LOCAL_URL)
    scripts_dir = str(Path(__file__).resolve().parent.parent / "scripts")
    monkeypatch.syspath_prepend(scripts_dir)
    apply_migrations = __import__("apply_migrations")
    applied = apply_migrations.apply_migrations(LOCAL_URL)
    assert "001_interview_transcript.sql" in applied
    sys.modules.pop("apply_migrations", None)


@requires_pg
def test_flush_writes_single_jsonb_row_matching_memory_log(migrated_db):
    session_id = random.randint(10**9, 10**10)
    content = _sample_content()
    try:
        assert asyncio.run(flush_transcript(str(session_id), content)) is True
        stored = _fetch_content(session_id)
        assert stored == json.loads(json.dumps(content))  # 순서·스키마 그대로 (무변환)
    finally:
        _cleanup(session_id)


@requires_pg
def test_duplicate_flush_is_noop_and_preserves_first_row(migrated_db):
    session_id = random.randint(10**9, 10**10)
    first = _sample_content()
    try:
        assert asyncio.run(flush_transcript(str(session_id), first)) is True
        # 중복 flush(종료 신호 중복·재시도) — 기존 행이 보존된다
        assert asyncio.run(flush_transcript(str(session_id), [])) is True
        assert _fetch_content(session_id) == json.loads(json.dumps(first))
    finally:
        _cleanup(session_id)


def test_flush_skipped_without_database_config(monkeypatch):
    monkeypatch.delenv(DATABASE_URL_ENV, raising=False)
    assert asyncio.run(flush_transcript("123", _sample_content())) is False


def test_flush_rejects_non_numeric_session_id(monkeypatch):
    monkeypatch.setenv(DATABASE_URL_ENV, LOCAL_URL)
    assert asyncio.run(flush_transcript("console-test", _sample_content())) is False


# --- 재시도 계약 (실패 시 1회 재시도 — PRD §4) ---

class _FakeConn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, sql, params):
        return None


def _fake_connect(monkeypatch, *, fail_first=False, fail_all=False, hang_first=False):
    """psycopg.AsyncConnection.connect를 대체해 호출 횟수를 기록한다."""
    import src.interview.transcript_store as store

    calls = {"n": 0}

    class _FakeConnection:
        @staticmethod
        async def connect(url, **kwargs):
            calls["n"] += 1
            if hang_first and calls["n"] == 1:
                await asyncio.sleep(3600)
            if fail_all or (fail_first and calls["n"] == 1):
                raise RuntimeError("transient")
            return _FakeConn()

    monkeypatch.setattr(store.psycopg, "AsyncConnection", _FakeConnection)
    monkeypatch.setenv(DATABASE_URL_ENV, "postgresql://irrelevant:5432/db")
    return calls


def test_flush_retries_once_then_succeeds(monkeypatch):
    calls = _fake_connect(monkeypatch, fail_first=True)
    assert asyncio.run(flush_transcript("123", [])) is True
    assert calls["n"] == 2  # 1차 실패 → 정확히 1회 재시도


def test_flush_exhausts_after_exactly_two_attempts(monkeypatch):
    calls = _fake_connect(monkeypatch, fail_all=True)
    assert asyncio.run(flush_transcript("123", [])) is False
    assert calls["n"] == 2  # 재시도 소진 — 3회 이상 시도하지 않는다


def test_hanging_attempt_is_bounded_and_retried(monkeypatch):
    import src.interview.transcript_store as store

    # 시도당 상한 — connect_timeout이 못 잡는 hang도 재시도 계약을 죽이지 않는다
    monkeypatch.setattr(store, "_ATTEMPT_TIMEOUT_SECONDS", 0.01)
    calls = _fake_connect(monkeypatch, hang_first=True)
    assert asyncio.run(flush_transcript("123", [])) is True
    assert calls["n"] == 2  # hang한 1차 시도가 취소되고 2차가 성공한다
