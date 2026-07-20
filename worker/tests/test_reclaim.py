"""회수(XAUTOCLAIM) 테스트 (§3) — 실제 Redis + 실제 DB.

시나리오: 다른 소비자("죽은 워커")가 메시지를 가져간 뒤 ACK 없이 사라짐
→ reclaim_pending_once 가 회수해 재처리·XACK 하는지 검증.
"""

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

import src.main as main
from src.ai import FakeEmbedder, FakeStructurer
from src.contract import AnalysisStatus, ParseRequest
from src.storage.repository import count_chunks, get_parse_status
from src.contract.structured_data import StructuredData
from tests.conftest import DIM, requires_postgres, seed_resume
from tests.test_pipeline import SD


def _redis_available() -> bool:
    import redis as sync_redis

    try:
        sync_redis.Redis(host="localhost", port=6379, socket_connect_timeout=2).ping()
        return True
    except Exception:
        return False


pytestmark = [
    requires_postgres,
    pytest.mark.skipif(not _redis_available(), reason="로컬 Redis(6379) 없음"),
]

STREAM = ParseRequest.STREAM_KEY
GROUP = "kkori-worker"


@pytest_asyncio.fixture
async def redis():
    r = aioredis.Redis(host="localhost", port=6379)
    # 깨끗한 스트림/그룹으로 시작
    try:
        await r.xgroup_destroy(STREAM, GROUP)
    except Exception:
        pass
    await r.delete(STREAM, "resume.parse.status.changed")
    await r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    yield r
    try:
        await r.xgroup_destroy(STREAM, GROUP)
    except Exception:
        pass
    await r.delete(STREAM, "resume.parse.status.changed")
    await r.aclose()


@pytest.fixture
def wired(conn, monkeypatch):
    """main 모듈의 자원·설정을 테스트용으로 배선."""
    monkeypatch.setattr(main._Resources, "db", conn)
    monkeypatch.setattr(main._Resources, "embedder", FakeEmbedder(dim=DIM))
    monkeypatch.setattr(
        main._Resources, "structurer", FakeStructurer(StructuredData.model_validate(SD))
    )
    monkeypatch.setattr(main.settings, "embedding_dim", DIM)
    monkeypatch.setattr(main.settings, "claim_min_idle_ms", 0)  # 테스트는 즉시 회수


async def _simulate_dead_worker(redis, rid: int, mode: str = "REINDEX") -> bytes:
    """메시지를 발행하고 '죽은 워커'가 가져가기만 하고 ACK 없이 사라진 상태를 만든다."""
    mid = await redis.xadd(
        STREAM,
        {"resumeId": str(rid), "userId": "1", "bucket": "b", "objectKey": "k", "mode": mode},
    )
    await redis.xreadgroup(GROUP, "dead-worker", {STREAM: ">"}, count=10)
    return mid


async def _pending_count(redis) -> int:
    info = await redis.xpending(STREAM, GROUP)
    return info["pending"]


@pytest.mark.asyncio
async def test_방치메시지_회수해_재처리하고_ACK(conn, redis, wired):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    await _simulate_dead_worker(redis, rid)
    assert await _pending_count(redis) == 1  # 죽은 워커의 PEL 에 잔류

    processed = await main.reclaim_pending_once(redis)

    assert processed == 1
    assert await get_parse_status(conn, rid) == "EMBEDDED"  # 체크포인트(EMBEDDING)부터 재개·완료
    assert await count_chunks(conn, rid) == 3
    assert await _pending_count(redis) == 0  # 직접 XACK 됨


@pytest.mark.asyncio
async def test_회수본도_포기규칙_임계도달시_FAILED_후_ACK(conn, redis, wired):
    """독성 메시지 무한 회수 차단 — 회수 경로에서 delivery count 임계 도달 → FAILED + ACK."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    mid = await _simulate_dead_worker(redis, rid)  # 1번째 전달
    # 회수 반복을 시뮬레이션해 delivery count 를 2로 (XCLAIM 마다 +1)
    await redis.xclaim(STREAM, GROUP, "dead-worker2", min_idle_time=0, message_ids=[mid])

    # reclaim 의 XAUTOCLAIM 이 3번째 전달 → 임계(3) 도달 → 재처리 없이 FAILED
    processed = await main.reclaim_pending_once(redis)

    assert processed == 1
    assert await get_parse_status(conn, rid) == "FAILED"
    assert await count_chunks(conn, rid) == 0  # 재처리(청킹·임베딩) 안 함
    assert await _pending_count(redis) == 0  # 포기 후에도 ACK — PEL 에서 제거

    # 발행된 마지막 이벤트가 FAILED 인지
    entries = await redis.xrange("resume.parse.status.changed")
    statuses = [
        {k.decode(): v.decode() for k, v in fields.items()}["status"] for _, fields in entries
    ]
    assert statuses[-1] == "FAILED"


@pytest.mark.asyncio
async def test_처리실패시_ACK안함_PEL잔류(conn, redis, wired, monkeypatch):
    """재처리 중 예외 → ACK 하지 않고 다음 주기 대상으로 남긴다."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    await _simulate_dead_worker(redis, rid)

    async def broken(*args, **kwargs):
        raise ConnectionError("일시 오류")

    monkeypatch.setattr(main, "process_request", broken)
    processed = await main.reclaim_pending_once(redis)

    assert processed == 0
    assert await _pending_count(redis) == 1  # 잔류 → 다음 회수 대상


@pytest.mark.asyncio
async def test_방치메시지_없으면_아무일도_안함(conn, redis, wired):
    assert await main.reclaim_pending_once(redis) == 0
