"""리포트 회수·상태 발행 테스트 — 실제 Redis + 실제 DB (이력서 test_reclaim 과 같은 방식).

시나리오: 다른 소비자("죽은 워커")가 메시지를 가져간 뒤 ACK 없이 사라짐
→ 회수 구독자가 XAUTOCLAIM 으로 가져와 재처리·XACK 하는지, 형식 위반 메시지를 제거하는지,
상태 발행이 Spring 이 읽는 네이티브 필드로 나가는지 검증.

회수 구독자의 배관(폴링·커서)은 FastStream 몫이라, 테스트는 실제 XAUTOCLAIM 결과를
`report.main.reclaim_one` 에 그대로 넘겨 처리 규칙만 확인한다.
"""

import asyncio
import contextlib

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

import src.report.main as report_main
from src.contract import ReportGenerationRequested, ReportStatus, ReportStatusChanged
from src.report.evaluator import FakeEvaluator
from src.report.streams import publish_status
from tests.conftest import requires_postgres, seed_session, seed_transcript
from tests.report.test_pipeline import UTTERANCES


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


@pytest.fixture
def wired(conn, monkeypatch):
    """report.main 모듈의 자원·설정을 테스트용으로 배선."""
    monkeypatch.setattr(report_main._Resources, "db", conn)
    monkeypatch.setattr(report_main._Resources, "evaluator", FakeEvaluator())
    monkeypatch.setattr(report_main.settings, "retry_base_delay_s", 0.0)


async def dead_worker_takes(redis, fields: dict) -> None:
    """메시지를 넣고, 죽은 워커가 가져가기만 하고 ACK 없이 사라진 상황을 만든다."""
    await redis.xadd(STREAM, fields)
    await redis.xreadgroup(GROUP, "dead-worker", {STREAM: ">"}, count=10)


async def _reclaim_all(redis, limit: int = 10) -> int:
    """구독자가 하는 일을 그대로 재현 — XAUTOCLAIM 으로 가져와 한 건씩 처리한다."""
    processed = 0
    for _ in range(limit):
        _cursor, messages, _deleted = await redis.xautoclaim(
            name=STREAM,
            groupname=GROUP,
            consumername=report_main.settings.reclaim_consumer_name,
            min_idle_time=0,  # 테스트는 즉시 회수
            count=1,
        )
        if not messages:
            break
        message_id, fields = messages[0]
        await report_main.reclaim_one(redis, message_id, fields)
        processed += 1
    return processed


@pytest.mark.asyncio
async def test_방치_메시지를_회수해_재처리하고_ACK한다(conn, redis, wired):
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)
    await dead_worker_takes(
        redis, ReportGenerationRequested(sessionId=session_id).encode()
    )

    assert await _reclaim_all(redis) == 1

    cur = await conn.execute(
        "SELECT text_analyzed_at FROM reports WHERE interview_session_id = %s", (session_id,)
    )
    assert (await cur.fetchone())["text_analyzed_at"] is not None  # 재처리 완주
    summary = await redis.xpending(STREAM, GROUP)
    assert summary["pending"] == 0  # XACK 됨


@pytest.mark.asyncio
async def test_형식_위반_메시지는_제거된다(conn, redis, wired, monkeypatch):
    await dead_worker_takes(redis, {"garbage": "x"})  # sessionId 없는 깨진 메시지

    async def never(*args, **kwargs):
        raise AssertionError("형식 위반 메시지가 처리 함수까지 오면 안 된다")

    monkeypatch.setattr(report_main, "process_generation_request", never)

    await _reclaim_all(redis)

    summary = await redis.xpending(STREAM, GROUP)
    assert summary["pending"] == 0  # 재회수 반복 없이 제거(ACK)됨


@pytest.mark.asyncio
async def test_재처리_실패_메시지는_PEL에_남는다(conn, redis, wired, monkeypatch):
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)
    await dead_worker_takes(
        redis, ReportGenerationRequested(sessionId=session_id).encode()
    )

    async def broken(*args, **kwargs):
        raise RuntimeError("일시 장애")

    monkeypatch.setattr(report_main, "process_generation_request", broken)

    await _reclaim_all(redis, limit=1)

    summary = await redis.xpending(STREAM, GROUP)
    assert summary["pending"] == 1  # ACK 안 됨 — 다음 폴링에 다시 회수


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


# --- 구독 배관 통합 검증 ---------------------------------------------------
# 위 테스트들은 reclaim_one 을 직접 호출하므로 구독자 배선(StreamSub 라우팅·AckPolicy·
# 커넥션 주입)은 타지 않는다. 이력서 워커에서 그 사각지대에 주입 타입 오류가 났었다.


async def _run_subscriber(sub, seconds: float) -> None:
    """구독자 하나만 잠깐 띄운다 — 앱 전체 기동은 DB 스키마·평가기 준비가 필요해 무겁다.

    브로커는 모듈 전역이라 앞 테스트의 이벤트 루프에 묶인 커넥션이 남는다. pytest-asyncio 는
    테스트마다 새 루프를 만들므로, 매번 끊고 다시 연결해야 "attached to a different loop" 가
    나지 않는다.
    """
    with contextlib.suppress(Exception):
        await report_main.broker.stop()
    await report_main.broker.connect()
    task = asyncio.create_task(sub.start())
    try:
        await asyncio.sleep(seconds)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        with contextlib.suppress(Exception):
            await report_main.broker.stop()


def _subscribers():
    subs = report_main.broker._subscribers
    reclaim = [s for s in subs if s.stream_sub.min_idle_time is not None]
    fresh = [s for s in subs if s.stream_sub.min_idle_time is None]
    assert len(reclaim) == 1 and len(fresh) == 1, "구독자는 회수용·새 메시지용 하나씩이어야 한다"
    return reclaim[0], fresh[0]


@pytest.mark.asyncio
async def test_구독자_배선_회수구독자가_방치메시지를_제거한다(redis, monkeypatch):
    """StreamSub(min_idle_time) 라우팅·AckPolicy.MANUAL·커넥션 주입이 실제로 맞물리는지.

    형식 위반 메시지를 쓰는 이유는 DB·평가기 없이도 끝까지 도달하기 때문이다
    (decode 실패 → 로그 → ACK).
    """
    reclaim_sub, _ = _subscribers()
    monkeypatch.setattr(reclaim_sub, "min_idle_time", 0)  # 즉시 회수 대상으로

    await dead_worker_takes(redis, {"garbage": "x"})
    assert (await redis.xpending(STREAM, GROUP))["pending"] == 1

    await _run_subscriber(reclaim_sub, 2.0)

    assert (await redis.xpending(STREAM, GROUP))["pending"] == 0  # 구독자가 회수해 제거
    consumers = {c["name"].decode() for c in await redis.xinfo_consumers(STREAM, GROUP)}
    assert report_main.settings.reclaim_consumer_name in consumers


@pytest.mark.asyncio
async def test_구독자_배선_회수구독자는_새메시지를_읽지_않는다(redis, monkeypatch):
    """min_idle_time 을 준 구독자는 XAUTOCLAIM 만 돈다 — 새 메시지는 PEL 에 없어 못 읽는다.

    FastStream 공식 문서에는 한 구독자가 둘 다 한다는 예시가 있으나 구현과 어긋난다
    (프로젝트 이슈 #2848·#2927). 구독자를 합치면 새 메시지가 조용히 안 읽히므로 고정한다.
    """
    reclaim_sub, fresh_sub = _subscribers()
    monkeypatch.setattr(reclaim_sub, "min_idle_time", 0)

    await redis.xadd(STREAM, {"garbage": "x"})

    async def last_delivered() -> str:
        return (await redis.xinfo_groups(STREAM))[0]["last-delivered-id"].decode()

    assert await last_delivered() == "0-0"
    await _run_subscriber(reclaim_sub, 1.5)
    assert await last_delivered() == "0-0", "회수 구독자가 새 메시지를 읽으면 안 된다"

    await _run_subscriber(fresh_sub, 1.5)
    assert await last_delivered() != "0-0", "새 메시지 구독자는 읽어야 한다"


@pytest.mark.asyncio
async def test_구독자_배선_처리실패시_ACK안하고_PEL에_남는다(conn, redis, wired, monkeypatch):
    """AckPolicy.MANUAL 의 핵심 — 실패했는데 프레임워크가 대신 ACK 하면 안 된다.

    ack_policy 를 빼면 기본값(REJECT_ON_ERROR)으로 돌아가는데, 이 테스트가 그 회귀를 잡는다.
    """
    reclaim_sub, _ = _subscribers()
    monkeypatch.setattr(reclaim_sub, "min_idle_time", 0)

    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)
    await dead_worker_takes(redis, ReportGenerationRequested(sessionId=session_id).encode())
    assert (await redis.xpending(STREAM, GROUP))["pending"] == 1

    async def broken(*args, **kwargs):
        raise RuntimeError("일시 장애")

    monkeypatch.setattr(report_main, "process_generation_request", broken)

    await _run_subscriber(reclaim_sub, 2.0)

    assert (await redis.xpending(STREAM, GROUP))["pending"] == 1, "처리 실패 메시지가 ACK 되면 안 된다"
