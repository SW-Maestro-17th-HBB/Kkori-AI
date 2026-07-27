"""종료 표식 테스트 — 로컬 Redis 없으면 write 검증만 skip (worker 패턴)."""

import asyncio
import json
import uuid

import pytest

from src.interview.redis_sink import REDIS_URL_ENV, write_termination_marker

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
def test_marker_written_with_cause_and_ttl(monkeypatch):
    import redis as sync_redis

    monkeypatch.setenv(REDIS_URL_ENV, LOCAL_URL)
    session_id = f"test-{uuid.uuid4()}"
    key = f"interview:{session_id}:termination"

    assert asyncio.run(write_termination_marker(session_id, "USER_REQUEST")) is True

    client = sync_redis.Redis.from_url(LOCAL_URL)
    try:
        payload = json.loads(client.get(key))
        assert payload["cause"] == "USER_REQUEST"
        assert payload["markedAt"].endswith("Z")
        assert client.ttl(key) > 0  # TTL 없는 개인정보 키가 남지 않는다
    finally:
        client.delete(key)
        client.close()


def test_marker_skipped_without_redis_config(monkeypatch):
    monkeypatch.delenv(REDIS_URL_ENV, raising=False)
    assert asyncio.run(write_termination_marker("123", "USER_REQUEST")) is False


# --- Redis 사본 정리 (flush 성공 후 DEL — docs/prd/interview-end.md §4) ---

@requires_redis
def test_purge_deletes_transcript_copy(monkeypatch):
    import redis as sync_redis

    from src.interview.redis_sink import purge_transcript_copy

    monkeypatch.setenv(REDIS_URL_ENV, LOCAL_URL)
    session_id = f"test-{uuid.uuid4()}"
    key = f"interview:{session_id}:transcript"
    client = sync_redis.Redis.from_url(LOCAL_URL)
    try:
        client.rpush(key, "{}")
        assert asyncio.run(purge_transcript_copy(session_id)) is True
        assert client.exists(key) == 0  # TTL 만료를 기다리지 않고 즉시 정리
    finally:
        client.delete(key)
        client.close()


def test_purge_skipped_without_redis_config(monkeypatch):
    from src.interview.redis_sink import purge_transcript_copy

    monkeypatch.delenv(REDIS_URL_ENV, raising=False)
    assert asyncio.run(purge_transcript_copy("123")) is False
