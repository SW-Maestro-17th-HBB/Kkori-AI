"""리포트 생성 요청 발행 테스트 — 로컬 Redis 없으면 발행 검증만 skip (worker 패턴)."""

import asyncio
import uuid

import pytest

from src.config import REPORT_REQUEST_STREAM_KEY
from src.interview.redis_sink import REDIS_URL_ENV
from src.interview.report_request import publish_report_request

LOCAL_URL = "redis://localhost:6379"


def _redis_available() -> bool:
    import redis as sync_redis

    try:
        sync_redis.Redis(host="localhost", port=6379, socket_connect_timeout=2).ping()
        return True
    except Exception:
        return False


requires_redis = pytest.mark.skipif(not _redis_available(), reason="로컬 Redis(6379) 없음")


@requires_redis
def test_publish_appends_pointer_message_to_stream(monkeypatch):
    import redis as sync_redis

    monkeypatch.setenv(REDIS_URL_ENV, LOCAL_URL)
    session_id = f"{uuid.uuid4().int % 10**10}"
    client = sync_redis.Redis.from_url(LOCAL_URL, decode_responses=True)
    try:
        last_id = "$"
        info = client.xinfo_stream(REPORT_REQUEST_STREAM_KEY) if client.exists(
            REPORT_REQUEST_STREAM_KEY
        ) else None
        if info:
            last_id = info["last-generated-id"]

        assert asyncio.run(publish_report_request(session_id)) is True

        entries = client.xrange(REPORT_REQUEST_STREAM_KEY, min=f"({last_id}" if last_id != "$" else "-")
        fields = entries[-1][1]
        assert fields["sessionId"] == session_id
        assert fields["requestedAt"].endswith("Z")
        assert set(fields) == {"sessionId", "requestedAt"}  # 포인터 — 전사 본문 없음
    finally:
        client.close()


@requires_redis
def test_duplicate_publish_appends_again_consumer_dedupes(monkeypatch):
    """재시도로 인한 중복 발행은 허용된다 — 멱등은 소비 측(worker) 계약이다."""
    import redis as sync_redis

    monkeypatch.setenv(REDIS_URL_ENV, LOCAL_URL)
    session_id = f"{uuid.uuid4().int % 10**10}"
    client = sync_redis.Redis.from_url(LOCAL_URL, decode_responses=True)
    try:
        assert asyncio.run(publish_report_request(session_id)) is True
        assert asyncio.run(publish_report_request(session_id)) is True
        entries = client.xrange(REPORT_REQUEST_STREAM_KEY)
        matching = [e for e in entries if e[1].get("sessionId") == session_id]
        assert len(matching) == 2
    finally:
        client.close()


def test_publish_skipped_without_redis_config(monkeypatch):
    monkeypatch.delenv(REDIS_URL_ENV, raising=False)
    assert asyncio.run(publish_report_request("123")) is False


def test_malformed_url_returns_false_instead_of_raising(monkeypatch):
    # 클라이언트 생성 실패(ValueError)도 재시도·식별 로그 경로를 타고 False로 끝난다
    monkeypatch.setenv(REDIS_URL_ENV, "not-a-url")
    assert asyncio.run(publish_report_request("123")) is False


def test_first_failure_then_retry_succeeds(monkeypatch):
    import src.interview.report_request as report_request

    monkeypatch.setenv(REDIS_URL_ENV, "redis://irrelevant:6379")
    attempts = {"n": 0}

    class _FlakyRedis:
        async def xadd(self, key, fields):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("transient")

        async def aclose(self):
            pass

    class _Factory:
        @staticmethod
        def from_url(url, **kwargs):
            return _FlakyRedis()

    monkeypatch.setattr(report_request, "Redis", _Factory)
    assert asyncio.run(publish_report_request("123")) is True
    assert attempts["n"] == 2  # 1차 실패 → 재시도 성공
