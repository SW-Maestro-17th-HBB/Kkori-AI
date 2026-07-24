"""Redis write-through writer 테스트 — 로컬 Redis 없으면 해당 테스트만 skip (worker 패턴).

CI에서는 agent-ci.yml의 Redis 서비스 컨테이너로 전부 실행된다.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone

import pytest

import src.interview.redis_sink as redis_sink
from src.interview.conversation_log import ConversationLog, QuestionType
from src.interview.redis_sink import RedisTranscriptWriter, create_transcript_writer

NOW = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)
LOCAL_URL = "redis://localhost:6379"


def _redis_available() -> bool:
    import redis as sync_redis

    try:
        sync_redis.Redis(host="localhost", port=6379, socket_connect_timeout=2).ping()
        return True
    except Exception:
        return False


requires_redis = pytest.mark.skipif(not _redis_available(), reason="로컬 Redis(6379) 없음")


def _sample_utterances() -> list[dict]:
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.INITIAL, content="자기소개 부탁드립니다.", spoken_at=NOW,
    )
    log.append_answer("백엔드 지망입니다.", NOW)
    log.append_answer("아, 그리고 인프라에도 관심이 있습니다.", NOW)
    return [u.to_json_dict() for u in log.utterances]


# --- 팩토리 폴백 (Redis 불필요) ---

def test_factory_without_url_falls_back_to_memory_only(monkeypatch):
    monkeypatch.delenv(redis_sink.REDIS_URL_ENV, raising=False)
    assert create_transcript_writer("123") is None


def test_factory_without_session_id_falls_back_to_memory_only(monkeypatch):
    monkeypatch.setenv(redis_sink.REDIS_URL_ENV, LOCAL_URL)
    assert create_transcript_writer(None) is None


# --- enqueue 논블로킹 (Redis 불필요 — writer task를 시작하지 않아 큐가 채워짐) ---

def test_enqueue_never_blocks_or_raises_when_queue_full(monkeypatch):
    monkeypatch.setattr(redis_sink, "_QUEUE_MAX_SIZE", 2)

    async def scenario():
        writer = RedisTranscriptWriter(url=LOCAL_URL, session_id="q", ttl_seconds=60)
        for data in _sample_utterances():  # 3건 > maxsize 2 — 초과분은 드롭
            writer.enqueue(data)
        assert writer._queue.qsize() == 2
        writer._closed = True  # 미시작 task·연결 정리 없이 종료 처리

    asyncio.run(scenario())


def test_enqueue_after_close_is_dropped():
    async def scenario():
        writer = RedisTranscriptWriter(url=LOCAL_URL, session_id="c", ttl_seconds=60)
        writer._closed = True
        writer.enqueue(_sample_utterances()[0])
        assert writer._queue.qsize() == 0

    asyncio.run(scenario())


# --- 실제 Redis 왕복 ---

@requires_redis
def test_write_through_preserves_schema_order_and_ttl():
    session_id = f"test-{uuid.uuid4().hex[:8]}"
    key = f"interview:{session_id}:transcript"
    utterances = _sample_utterances()

    async def scenario() -> tuple[list[dict], int, int]:
        writer = RedisTranscriptWriter(
            url=LOCAL_URL, session_id=session_id, ttl_seconds=86400
        )
        writer.start()
        for data in utterances:
            writer.enqueue(data)
        await writer.wait_for_drain()
        ttl_after_first_batch = await writer._redis.ttl(key)

        # TTL 갱신 확인 — 짧게 줄여둔 뒤 append하면 다시 늘어나야 한다
        await writer._redis.expire(key, 100)
        writer.enqueue(utterances[0])
        await writer.wait_for_drain()
        ttl_after_refresh = await writer._redis.ttl(key)

        stored = [json.loads(raw) for raw in await writer._redis.lrange(key, 0, -1)]
        await writer._redis.delete(key)
        await writer.aclose()
        return stored, ttl_after_first_batch, ttl_after_refresh

    stored, ttl_first, ttl_refreshed = asyncio.run(scenario())
    assert stored[:3] == utterances  # 메모리 순서 = Redis 순서, 스키마 그대로
    assert ttl_first > 0  # 첫 적재 직후 TTL 설정 (RPUSH+EXPIRE 원자)
    assert ttl_refreshed > 100  # append마다 TTL 갱신


@requires_redis
def test_aclose_drains_pending_items_and_is_idempotent():
    session_id = f"test-{uuid.uuid4().hex[:8]}"
    key = f"interview:{session_id}:transcript"

    async def scenario() -> int:
        writer = RedisTranscriptWriter(
            url=LOCAL_URL, session_id=session_id, ttl_seconds=60
        )
        writer.start()
        for data in _sample_utterances():
            writer.enqueue(data)
        await writer.aclose()  # drain 후 종료 — 적재 완료 보장
        await writer.aclose()  # 멱등

        import redis as sync_redis

        client = sync_redis.Redis(host="localhost", port=6379)
        count = client.llen(key)
        client.delete(key)
        return count

    assert asyncio.run(scenario()) == 3
