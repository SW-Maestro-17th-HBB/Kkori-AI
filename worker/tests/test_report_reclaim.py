"""리포트 회수·상태 발행 테스트 — 실제 Redis + 실제 DB (이력서 test_reclaim 과 같은 방식).

시나리오: 다른 소비자("죽은 워커")가 메시지를 가져간 뒤 ACK 없이 사라짐
→ reclaim_pending_once 가 회수해 재처리·XACK 하는지, 형식 위반 메시지를 제거하는지,
상태 발행이 Spring 이 읽는 네이티브 필드로 나가는지 검증.
"""

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from src.config import Settings
from src.contract import ReportGenerationRequested, ReportStatus, ReportStatusChanged
from src.report.evaluator import FakeEvaluator
from src.report.pipeline import process_generation_request
from src.report.reclaim import reclaim_pending_once
from src.report.streams import publish_status
from tests.conftest import requires_postgres, seed_session, seed_transcript
from tests.test_report_pipeline import UTTERANCES


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

STREAM = ReportGenerationRequested.STREAM_KEY
STATUS_STREAM = ReportStatusChanged.STREAM_KEY
GROUP = "kkori-report-worker"


@pytest_asyncio.fixture
async def redis():
    r = aioredis.Redis(host="localhost", port=6379)
    try:
        await r.xgroup_destroy(STREAM, GROUP)
    except Exception:
        pass
    await r.delete(STREAM, STATUS_STREAM)
    await r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    yield r
    try:
        await r.xgroup_destroy(STREAM, GROUP)
    except Exception:
        pass
    await r.delete(STREAM, STATUS_STREAM)
    await r.aclose()


def reclaim_settings(dsn: str) -> Settings:
    return Settings(
        postgres_dsn=dsn,
        claim_min_idle_ms=0,  # 테스트에선 즉시 방치로 판정
        retry_base_delay_s=0.0,
    )


async def dead_worker_takes(redis, fields: dict) -> None:
    """메시지를 넣고, 죽은 워커가 가져가기만 하고 ACK 없이 사라진 상황을 만든다."""
    await redis.xadd(STREAM, fields)
    await redis.xreadgroup(GROUP, "dead-worker", {STREAM: ">"}, count=10)


@pytest.mark.asyncio
async def test_방치_메시지를_회수해_재처리하고_ACK한다(conn, redis):
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)
    await dead_worker_takes(
        redis, ReportGenerationRequested(sessionId=session_id).encode()
    )
    settings = reclaim_settings(str(conn.info.dsn))

    async def process(request, delivery_count):
        await process_generation_request(
            request, conn=conn, evaluator=FakeEvaluator(),
            publish=lambda *a: publish_status(redis, *a),
            settings=settings, delivery_count=delivery_count, is_reclaimed=True,
        )

    processed = await reclaim_pending_once(redis, settings=settings, process=process)

    assert processed == 1
    cur = await conn.execute(
        "SELECT text_analyzed_at FROM reports WHERE interview_session_id = %s", (session_id,)
    )
    assert (await cur.fetchone())["text_analyzed_at"] is not None  # 재처리 완주
    summary = await redis.xpending(STREAM, GROUP)
    assert summary["pending"] == 0  # XACK 됨


@pytest.mark.asyncio
async def test_형식_위반_메시지는_제거된다(conn, redis):
    await dead_worker_takes(redis, {"garbage": "x"})  # sessionId 없는 깨진 메시지
    settings = reclaim_settings(str(conn.info.dsn))

    async def process(request, delivery_count):
        raise AssertionError("형식 위반 메시지가 처리 함수까지 오면 안 된다")

    processed = await reclaim_pending_once(redis, settings=settings, process=process)

    assert processed == 0
    summary = await redis.xpending(STREAM, GROUP)
    assert summary["pending"] == 0  # 재회수 반복 없이 제거(ACK)됨


@pytest.mark.asyncio
async def test_재처리_실패_메시지는_PEL에_남는다(conn, redis):
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)
    await dead_worker_takes(
        redis, ReportGenerationRequested(sessionId=session_id).encode()
    )
    settings = reclaim_settings(str(conn.info.dsn))

    async def process(request, delivery_count):
        raise RuntimeError("일시 장애")

    processed = await reclaim_pending_once(redis, settings=settings, process=process)

    assert processed == 0
    summary = await redis.xpending(STREAM, GROUP)
    assert summary["pending"] == 1  # ACK 안 됨 — 다음 주기에 다시 회수


@pytest.mark.asyncio
async def test_상태_발행은_네이티브_필드로_나간다(redis):
    await publish_status(redis, 7, 3, ReportStatus.PROCESSING)

    entries = await redis.xrange(STATUS_STREAM)
    assert len(entries) == 1
    _id, fields = entries[0]
    decoded = {k.decode(): v.decode() for k, v in fields.items()}
    assert decoded == {
        "reportId": "7", "userId": "3", "status": "PROCESSING", "message": "",
    }
    # 계약 왕복 — Spring 쪽 파싱과 같은 형태로 읽힌다
    assert ReportStatusChanged.decode(decoded).status is ReportStatus.PROCESSING
