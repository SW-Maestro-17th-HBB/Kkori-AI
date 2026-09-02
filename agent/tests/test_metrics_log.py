"""메트릭 수집·flush 테스트 — 로컬 Postgres 없으면 실쿼리 테스트만 skip (worker 패턴).

수집기는 실제 livekit 메트릭 클래스(pydantic)로 검증한다 — 세션 이벤트 래핑
(MetricsCollectedEvent)과 인스턴스 원본 두 형태 모두. DDL은 agent 소유
마이그레이션(migrations/)을 그대로 적용해 검증한다.
"""

import asyncio
import random
from datetime import datetime, timezone

import pytest
from livekit.agents.metrics import EOUMetrics, LLMMetrics
from livekit.agents.voice.events import MetricsCollectedEvent

import src.interview.metrics_log as metrics_log_module
from src.interview.metrics_log import DATABASE_URL_ENV, MetricsLog, flush_metrics

LOCAL_URL = "postgresql://postgres:postgres@localhost:5432/postgres"
EPOCH = 1725270000.0  # 2024-09-02T09:40:00Z


def _pg_available() -> bool:
    import psycopg

    try:
        with psycopg.connect(LOCAL_URL, connect_timeout=2):
            return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(not _pg_available(), reason="로컬 Postgres(5432) 없음")


def _llm_metrics(ts: float = EPOCH) -> LLMMetrics:
    return LLMMetrics(
        label="aws.LLM",
        request_id="req-1",
        timestamp=ts,
        duration=1.2,
        ttft=0.4,
        cancelled=False,
        completion_tokens=10,
        prompt_tokens=100,
        prompt_cached_tokens=0,
        total_tokens=110,
        tokens_per_second=8.3,
    )


def _eou_event(ts: float = EPOCH) -> MetricsCollectedEvent:
    return MetricsCollectedEvent(
        metrics=EOUMetrics(
            timestamp=ts,
            end_of_utterance_delay=0.5,
            transcription_delay=0.1,
            on_user_turn_completed_delay=0.01,
        )
    )


# --- 수집기 ---


def test_record_unwraps_session_event():
    log = MetricsLog()
    log.handler()(_eou_event())
    [(ts, kind, payload)] = log.rows
    assert kind == "eou_metrics"
    assert ts == datetime.fromtimestamp(EPOCH, tz=timezone.utc)
    assert payload["end_of_utterance_delay"] == 0.5
    assert "source" not in payload


def test_record_instance_metrics_with_source_tag():
    log = MetricsLog()
    log.handler("orchestrator")(_llm_metrics())
    [(_, kind, payload)] = log.rows
    assert kind == "llm_metrics"
    assert payload["source"] == "orchestrator"
    assert payload["prompt_tokens"] == 100


def test_record_swallows_serialization_failure():
    log = MetricsLog()
    log.record(object())  # model_dump 없음 — 폐기되고 예외는 삼켜진다
    assert log.rows == []


def test_record_caps_rows(monkeypatch):
    monkeypatch.setattr(metrics_log_module, "_MAX_ROWS", 2)
    log = MetricsLog()
    for _ in range(5):
        log.record(_llm_metrics())
    assert len(log.rows) == 2


# --- flush ---


def _fetch_rows(session_id: int):
    import psycopg

    with psycopg.connect(LOCAL_URL) as conn:
        return conn.execute(
            "SELECT kind, payload FROM interview_metrics "
            "WHERE session_id = %s ORDER BY id",
            (session_id,),
        ).fetchall()


def _cleanup(session_id: int) -> None:
    import psycopg

    with psycopg.connect(LOCAL_URL) as conn:
        conn.execute(
            "DELETE FROM interview_metrics WHERE session_id = %s", (session_id,)
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
    assert "002_interview_metrics.sql" in applied
    sys.modules.pop("apply_migrations", None)


@requires_pg
def test_flush_writes_one_row_per_event(migrated_db):
    session_id = random.randint(10**9, 10**10)
    log = MetricsLog()
    log.handler("orchestrator")(_llm_metrics())
    log.handler()(_eou_event(EPOCH + 1))
    try:
        assert asyncio.run(flush_metrics(str(session_id), log.rows)) is True
        rows = _fetch_rows(session_id)
        assert [kind for kind, _ in rows] == ["llm_metrics", "eou_metrics"]
        assert rows[0][1]["source"] == "orchestrator"
        assert rows[1][1]["transcription_delay"] == 0.1
    finally:
        _cleanup(session_id)


def test_flush_empty_rows_is_noop_success(monkeypatch):
    monkeypatch.delenv(DATABASE_URL_ENV, raising=False)
    assert asyncio.run(flush_metrics("123", [])) is True


def test_flush_skipped_without_database_config(monkeypatch):
    monkeypatch.delenv(DATABASE_URL_ENV, raising=False)
    log = MetricsLog()
    log.record(_llm_metrics())
    assert asyncio.run(flush_metrics("123", log.rows)) is False


def test_flush_rejects_non_numeric_session_id(monkeypatch):
    monkeypatch.setenv(DATABASE_URL_ENV, LOCAL_URL)
    log = MetricsLog()
    log.record(_llm_metrics())
    assert asyncio.run(flush_metrics("console-test", log.rows)) is False


# --- 재시도 계약 (실패 시 1회 재시도 — transcript_store와 동일) ---


def _fake_connect(monkeypatch, *, fail_first=False, fail_all=False):
    """psycopg.AsyncConnection.connect를 대체해 호출 횟수를 기록한다."""
    calls = {"n": 0}

    class _FakeCursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def executemany(self, sql, params):
            return None

    class _FakeConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def cursor(self):
            return _FakeCursor()

    class _FakeConnection:
        @staticmethod
        async def connect(url, **kwargs):
            calls["n"] += 1
            if fail_all or (fail_first and calls["n"] == 1):
                raise OSError("connect refused")
            return _FakeConn()

    monkeypatch.setattr(
        metrics_log_module.psycopg, "AsyncConnection", _FakeConnection
    )
    monkeypatch.setenv(DATABASE_URL_ENV, "postgresql://fake")
    return calls


def test_flush_retries_once_then_succeeds(monkeypatch):
    calls = _fake_connect(monkeypatch, fail_first=True)
    log = MetricsLog()
    log.record(_llm_metrics())
    assert asyncio.run(flush_metrics("123", log.rows)) is True
    assert calls["n"] == 2


def test_flush_gives_up_after_retry_exhaustion(monkeypatch):
    calls = _fake_connect(monkeypatch, fail_all=True)
    log = MetricsLog()
    log.record(_llm_metrics())
    assert asyncio.run(flush_metrics("123", log.rows)) is False
    assert calls["n"] == 2
