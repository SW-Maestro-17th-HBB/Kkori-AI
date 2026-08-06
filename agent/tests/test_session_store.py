"""세션 메타·owner·복원 상태 저장소 테스트 — 로컬 Redis 없으면 해당 테스트만 skip.

docs/prd/interview-recovery.md §2. CI에서는 Redis 서비스 컨테이너로 전부 실행된다.
"""

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from src.interview.redis_sink import REDIS_URL_ENV
from src.interview.session_store import (
    claim_owner,
    clear_reconnect_deadline,
    init_session_meta,
    owner_allows,
    purge_session_state,
    read_restore_state,
    record_reconnect_deadline,
    release_owner,
)

LOCAL_URL = "redis://localhost:6379"
STARTED = datetime(2026, 8, 6, 9, 0, 0, tzinfo=timezone.utc)
DEADLINE = STARTED + timedelta(minutes=25)


def _redis_available() -> bool:
    import redis as sync_redis

    try:
        sync_redis.Redis(host="localhost", port=6379, socket_connect_timeout=2).ping()
        return True
    except Exception:
        return False


requires_redis = pytest.mark.skipif(not _redis_available(), reason="로컬 Redis(6379) 없음")


@pytest.fixture
def session_id(monkeypatch):
    monkeypatch.setenv(REDIS_URL_ENV, LOCAL_URL)
    sid = f"test-{uuid.uuid4()}"
    yield sid
    # 테스트 키 정리 — owner 포함
    import redis as sync_redis

    client = sync_redis.Redis(host="localhost", port=6379)
    client.delete(
        f"interview:{sid}:meta",
        f"interview:{sid}:owner",
        f"interview:{sid}:transcript",
        f"interview:{sid}:termination",
    )


# --- Redis 미구성 폴백 (Redis 불필요) ---

def test_writes_without_url_are_noop(monkeypatch):
    monkeypatch.delenv(REDIS_URL_ENV, raising=False)
    assert asyncio.run(init_session_meta("s", started_at=STARTED, candidate_identity="u")) is False
    assert asyncio.run(record_reconnect_deadline("s", DEADLINE)) is False
    assert asyncio.run(claim_owner("s", "job-1")) is False
    assert asyncio.run(owner_allows("s", "job-1")) is True  # 조회 불가 — 통과


def test_read_restore_state_without_url_is_empty(monkeypatch):
    monkeypatch.delenv(REDIS_URL_ENV, raising=False)
    state = asyncio.run(read_restore_state("s"))
    assert state.restorable is False
    assert state.terminated is False


# --- 메타 원자 초기화·NX 보존 ---

@requires_redis
def test_init_is_atomic_and_nx_preserves_original(session_id):
    async def scenario():
        assert await init_session_meta(
            session_id, started_at=STARTED, candidate_identity="user-a"
        )
        # 재디스패치·중복 기록 — 원래 값을 덮어쓰지 못한다(HSETNX)
        await init_session_meta(
            session_id,
            started_at=STARTED + timedelta(minutes=5),
            candidate_identity="user-b",
        )
        return await read_restore_state(session_id)

    state = asyncio.run(scenario())
    assert state.started_at == STARTED
    assert state.candidate_identity == "user-a"

    # 원자 초기화 — 두 필드 + TTL이 함께 존재한다(부분 상태 없음)
    import redis as sync_redis

    client = sync_redis.Redis(host="localhost", port=6379)
    assert client.ttl(f"interview:{session_id}:meta") > 0


@requires_redis
def test_deadline_roundtrip_and_clear(session_id):
    async def scenario():
        await init_session_meta(session_id, started_at=STARTED, candidate_identity="u")
        await record_reconnect_deadline(session_id, DEADLINE)
        with_deadline = await read_restore_state(session_id)
        await clear_reconnect_deadline(session_id)
        cleared = await read_restore_state(session_id)
        return with_deadline, cleared

    with_deadline, cleared = asyncio.run(scenario())
    assert with_deadline.reconnect_deadline == DEADLINE  # 절대 시각 — 창은 재부여되지 않는다
    assert cleared.reconnect_deadline is None


# --- owner 가드 (완화 계층) ---

@requires_redis
def test_owner_last_wins_and_mismatch_blocks(session_id):
    async def scenario():
        await claim_owner(session_id, "job-old")
        await claim_owner(session_id, "job-new")  # last-wins — 후발 잡이 주인
        return (
            await owner_allows(session_id, "job-new"),
            await owner_allows(session_id, "job-old"),
        )

    new_ok, old_ok = asyncio.run(scenario())
    assert new_ok is True
    assert old_ok is False  # 다른 잡의 식별자 관측 — 종결 단계 생략


@requires_redis
def test_owner_absent_passes_and_release_only_own(session_id):
    async def scenario():
        absent = await owner_allows(session_id, "job-1")  # 부재 = 인수 관측 없음 — 통과
        await claim_owner(session_id, "job-1")
        await release_owner(session_id, "job-2")  # 남의 소유 — DEL 안 함
        still = await owner_allows(session_id, "job-2")
        await release_owner(session_id, "job-1")  # 자기 소유 — DEL
        released = await owner_allows(session_id, "job-2")
        return absent, still, released

    absent, still, released = asyncio.run(scenario())
    assert absent is True
    assert still is False
    assert released is True


# --- 복원 상태 조회·purge 집합 ---

@requires_redis
def test_restore_state_reads_marker_meta_and_transcript(session_id):
    import redis as sync_redis

    client = sync_redis.Redis(host="localhost", port=6379)
    client.rpush(
        f"interview:{session_id}:transcript",
        json.dumps({"speaker": "INTERVIEWER", "content": "질문"}),
        "깨진 JSON{",
    )
    client.set(f"interview:{session_id}:termination", "{}")

    async def scenario():
        await init_session_meta(session_id, started_at=STARTED, candidate_identity="u")
        return await read_restore_state(session_id)

    state = asyncio.run(scenario())
    assert state.terminated is True  # 표식 존재 — 방어적 즉시 종료 재료
    assert state.restorable is True
    assert len(state.utterances) == 1 and state.dropped == 1  # 파싱 불가만 드롭


@requires_redis
def test_purge_removes_transcript_and_meta_but_not_owner(session_id):
    import redis as sync_redis

    client = sync_redis.Redis(host="localhost", port=6379)
    client.rpush(f"interview:{session_id}:transcript", "{}")

    async def scenario():
        await init_session_meta(session_id, started_at=STARTED, candidate_identity="u")
        await claim_owner(session_id, "job-1")
        await purge_session_state(session_id)
        return await owner_allows(session_id, "job-2")

    blocked_after_purge = asyncio.run(scenario())
    # purge 대상 = transcript + meta (개인 식별 재료). owner는 룸 삭제까지 유효해야
    # 하므로 비대상 — purge 직후에도 가드가 살아있다 (recovery §2 내부 충돌 방지)
    assert client.exists(f"interview:{session_id}:transcript") == 0
    assert client.exists(f"interview:{session_id}:meta") == 0
    assert blocked_after_purge is False


@requires_redis
def test_malformed_started_at_is_flagged_as_lost(session_id):
    import redis as sync_redis

    client = sync_redis.Redis(host="localhost", port=6379)
    client.hset(f"interview:{session_id}:meta", "startedAt", "not-a-timestamp")

    state = asyncio.run(read_restore_state(session_id))
    assert state.started_at is None
    assert state.started_at_malformed is True  # 유실 취급 — 첫 발화 근사 폴백 층위로
