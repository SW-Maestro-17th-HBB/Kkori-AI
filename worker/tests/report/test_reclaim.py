"""리포트 회수·상태 발행 테스트 — 실제 Redis(8.4+) + 실제 DB (이력서 test_reclaim 과 같은 방식).

시나리오: 다른 소비자("죽은 워커")가 메시지를 가져간 뒤 ACK 없이 사라짐
→ 구독자가 XREADGROUP CLAIM 으로 가져와 재처리·XACK 하는지, 형식 위반 메시지를 제거하는지,
상태 발행이 Spring 이 구독하는 Pub/Sub 채널에 JSON 으로 나가는지 검증.

구독자의 배관(폴링)은 FastStream 몫이라, 테스트는 실제 XREADGROUP CLAIM 결과를
`report.main.handle_generation_entry` 에 그대로 넘겨 처리 규칙만 확인한다.
"""

import asyncio
import contextlib
import json

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

import src.report.main as report_main
from src.contract import (
    AudioAnalysisRequested,
    ReportGenerationRequested,
    ReportStatus,
    ReportStatusChanged,
)
from src.report.evaluator import FakeEvaluator
from src.report.publish import publish_status
from tests.conftest import requires_postgres, requires_redis_with_claim, seed_session, seed_transcript
from tests.report.test_audio_pipeline import FakeS3
from tests.report.test_audio_pipeline import UTTERANCES as AUDIO_UTTERANCES
from tests.report.test_pipeline import UTTERANCES, run as run_generation


pytestmark = [requires_postgres, requires_redis_with_claim]

STREAM = ReportGenerationRequested.STREAM_KEY
STATUS_CHANNEL = ReportStatusChanged.CHANNEL
GROUP = "kkori-report-worker"


@pytest_asyncio.fixture
async def redis():
    r = aioredis.Redis(host="localhost", port=6379)
    try:
        await r.xgroup_destroy(STREAM, GROUP)
    except Exception:
        pass
    await r.delete(STREAM)
    await r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    yield r
    try:
        await r.xgroup_destroy(STREAM, GROUP)
    except Exception:
        pass
    await r.delete(STREAM)
    await r.aclose()


@pytest.fixture
def wired(conn, monkeypatch):
    """report.main 모듈의 자원·설정을 테스트용으로 배선."""
    monkeypatch.setattr(report_main._Resources, "db", conn)
    monkeypatch.setattr(report_main._Resources, "evaluator", FakeEvaluator())
    monkeypatch.setattr(report_main.settings, "retry_base_delay_s", 0.0)


async def seed_audio_ready(conn) -> int:
    """텍스트 분석이 끝나 음성을 기다리는 세션 (대본은 픽스처 녹음과 짝)."""
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, AUDIO_UTTERANCES)
    await run_generation(conn, session_id, audio_enabled=True)
    return session_id


async def dead_worker_takes(redis, fields: dict) -> None:
    """메시지를 넣고, 죽은 워커가 가져가기만 하고 ACK 없이 사라진 상황을 만든다."""
    await redis.xadd(STREAM, fields)
    await redis.xreadgroup(GROUP, "dead-worker", {STREAM: ">"}, count=10)


async def _read_all(redis, stream: str = STREAM, handle=None, limit: int = 10) -> int:
    """구독자가 하는 일을 그대로 재현 — XREADGROUP CLAIM 으로 가져와 한 건씩 처리한다."""
    handle = handle or report_main.handle_generation_entry
    processed = 0
    for _ in range(limit):
        response = await redis.xreadgroup(
            GROUP,
            report_main.settings.resolved_consumer_name,
            {stream: ">"},
            count=1,
            claim_min_idle_time=0,  # 테스트는 즉시 회수
        )
        if not response:
            break
        (_stream, ((message_id, fields, _idle, prior_deliveries),)), = response
        await handle(redis, message_id, fields, prior_deliveries + 1)
        processed += 1
    return processed


@pytest.mark.asyncio
async def test_방치_메시지를_회수해_재처리하고_ACK한다(conn, redis, wired):
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)
    await dead_worker_takes(
        redis, ReportGenerationRequested(sessionId=session_id).encode()
    )

    assert await _read_all(redis) == 1

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

    await _read_all(redis)

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

    await _read_all(redis, limit=1)

    summary = await redis.xpending(STREAM, GROUP)
    assert summary["pending"] == 1  # ACK 안 됨 — claim_min_idle_ms 가 지나면 다시 회수


@pytest.mark.asyncio
async def test_상태_발행은_PubSub_채널에_JSON으로_나간다(redis):
    pubsub = redis.pubsub()
    await pubsub.subscribe(STATUS_CHANNEL)  # 저장이 없으므로 발행 전에 구독
    assert (await pubsub.get_message(timeout=2.0))["type"] == "subscribe"  # 구독 확정 후 발행
    try:
        await publish_status(redis, 7, 3, ReportStatus.PROCESSING)

        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2.0)
        assert msg is not None
        assert msg["channel"].decode() == STATUS_CHANNEL
        decoded = json.loads(msg["data"])
        assert decoded == {
            "reportId": "7", "userId": "3", "status": "PROCESSING", "message": "",
        }
        # 계약 왕복 — Spring 쪽 파싱과 같은 형태로 읽힌다
        assert ReportStatusChanged.decode(decoded).status is ReportStatus.PROCESSING
    finally:
        await pubsub.unsubscribe(STATUS_CHANNEL)
        await pubsub.aclose()


# --- 구독 배관 통합 검증 ---------------------------------------------------
# 위 테스트들은 handle_generation_entry 를 직접 호출하므로 구독자 배선(StreamSub 라우팅·
# AckPolicy·커넥션 주입)은 타지 않는다. 이력서 워커에서 그 사각지대에 주입 타입 오류가 났었다.


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


def _subscriber(stream: str = STREAM):
    subs = [s for s in report_main.broker._subscribers if s.stream_sub.name == stream]
    assert len(subs) == 1, "스트림마다 구독자는 새 메시지와 회수를 함께 처리하는 하나여야 한다"
    return subs[0]


@pytest.mark.asyncio
async def test_구독자_배선_방치메시지를_회수해_제거한다(redis, monkeypatch):
    """StreamSub(claim_min_idle_time) 라우팅·AckPolicy.MANUAL·커넥션 주입이 실제로 맞물리는지.

    형식 위반 메시지를 쓰는 이유는 DB·평가기 없이도 끝까지 도달하기 때문이다
    (decode 실패 → 로그 → ACK).
    """
    sub = _subscriber()
    monkeypatch.setattr(sub, "claim_min_idle_time", 0)  # 즉시 회수 대상으로

    await dead_worker_takes(redis, {"garbage": "x"})
    assert (await redis.xpending(STREAM, GROUP))["pending"] == 1

    await _run_subscriber(sub, 2.0)

    assert (await redis.xpending(STREAM, GROUP))["pending"] == 0  # 구독자가 회수해 제거


@pytest.mark.asyncio
async def test_구독자_배선_새메시지도_같은_구독자가_읽는다(redis, monkeypatch):
    """회수 전용 구독자(XAUTOCLAIM)는 새 메시지를 못 읽어 구독자를 둘로 나눴었다.
    CLAIM 으로 합친 구독자가 새 메시지도 읽는지 고정한다."""
    sub = _subscriber()
    monkeypatch.setattr(sub, "claim_min_idle_time", 0)

    await redis.xadd(STREAM, {"garbage": "x"})

    async def last_delivered() -> str:
        return (await redis.xinfo_groups(STREAM))[0]["last-delivered-id"].decode()

    assert await last_delivered() == "0-0"
    await _run_subscriber(sub, 1.5)
    assert await last_delivered() != "0-0", "새 메시지를 읽어야 한다"
    assert (await redis.xpending(STREAM, GROUP))["pending"] == 0  # 형식 위반이라 읽고 바로 제거


@pytest.mark.asyncio
async def test_구독자_배선_처리실패시_ACK안하고_PEL에_남는다(conn, redis, wired, monkeypatch):
    """AckPolicy.MANUAL 의 핵심 — 실패했는데 프레임워크가 대신 ACK 하면 안 된다.

    ack_policy 를 빼면 기본값(REJECT_ON_ERROR)으로 돌아가는데, 이 테스트가 그 회귀를 잡는다.
    """
    sub = _subscriber()
    monkeypatch.setattr(sub, "claim_min_idle_time", 0)

    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)
    await dead_worker_takes(redis, ReportGenerationRequested(sessionId=session_id).encode())
    assert (await redis.xpending(STREAM, GROUP))["pending"] == 1

    # 구독자가 아예 안 돌아도 PEL 은 1로 남으므로, 실패 경로를 정말 탔는지 표시를 남긴다
    called = asyncio.Event()

    async def broken(*args, **kwargs):
        called.set()
        raise RuntimeError("일시 장애")

    monkeypatch.setattr(report_main, "process_generation_request", broken)

    await _run_subscriber(sub, 2.0)

    assert called.is_set(), "구독자가 메시지를 처리 함수까지 전달해야 한다"
    assert (await redis.xpending(STREAM, GROUP))["pending"] == 1, "처리 실패 메시지가 ACK 되면 안 된다"


# --- 음성 분석 요청 스트림 ---------------------------------------------------
# 처리 규칙은 생성 요청과 같은 `_handle_entry` 를 타므로, 음성 쪽은 배선(스트림 키·계약·처리 함수)만 확인한다.

AUDIO_STREAM = AudioAnalysisRequested.STREAM_KEY


@pytest_asyncio.fixture
async def audio_redis(redis):
    try:
        await redis.xgroup_destroy(AUDIO_STREAM, GROUP)
    except Exception:
        pass
    await redis.delete(AUDIO_STREAM)
    await redis.xgroup_create(AUDIO_STREAM, GROUP, id="0", mkstream=True)
    yield redis
    try:
        await redis.xgroup_destroy(AUDIO_STREAM, GROUP)
    except Exception:
        pass
    await redis.delete(AUDIO_STREAM)


async def _read_all_audio(redis, limit: int = 10) -> int:
    return await _read_all(redis, AUDIO_STREAM, report_main.handle_audio_entry, limit)


@pytest.mark.asyncio
async def test_음성_요청_방치_메시지를_회수해_재처리하고_ACK한다(conn, audio_redis, wired, monkeypatch):
    monkeypatch.setattr(report_main._Resources, "s3", FakeS3())
    session_id = await seed_audio_ready(conn)
    await audio_redis.xadd(AUDIO_STREAM, AudioAnalysisRequested(
        sessionId=session_id, bucket="b", objectKey="recordings/x.ogg").encode())
    await audio_redis.xreadgroup(GROUP, "dead-worker", {AUDIO_STREAM: ">"}, count=10)

    assert await _read_all_audio(audio_redis) == 1

    cur = await conn.execute(
        "SELECT status, audio_analyzed_at FROM reports WHERE interview_session_id = %s", (session_id,)
    )
    row = await cur.fetchone()
    assert row["audio_analyzed_at"] is not None
    assert row["status"] == ReportStatus.COMPLETED.value  # 텍스트는 이미 끝나 있었다
    assert (await audio_redis.xpending(AUDIO_STREAM, GROUP))["pending"] == 0  # XACK 됨


@pytest.mark.asyncio
async def test_음성_요청_형식_위반_메시지는_제거된다(conn, audio_redis, wired):
    await audio_redis.xadd(AUDIO_STREAM, {"sessionId": "1"})  # bucket·objectKey 없음
    await audio_redis.xreadgroup(GROUP, "dead-worker", {AUDIO_STREAM: ">"}, count=10)

    await _read_all_audio(audio_redis)

    assert (await audio_redis.xpending(AUDIO_STREAM, GROUP))["pending"] == 0


@pytest.mark.asyncio
async def test_음성_구독자_배선은_CLAIM_구독자_하나다():
    sub = _subscriber(AUDIO_STREAM)
    assert sub.stream_sub.claim_min_idle_time == report_main.settings.claim_min_idle_ms
    assert sub.stream_sub.group == report_main.settings.report_consumer_group
