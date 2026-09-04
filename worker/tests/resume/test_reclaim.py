"""회수(XAUTOCLAIM) 테스트 (§3) — 실제 Redis + 실제 DB.

시나리오: 다른 소비자("죽은 워커")가 메시지를 가져간 뒤 ACK 없이 사라짐
→ 회수 구독자가 XAUTOCLAIM 으로 가져와 재처리·XACK 하는지 검증.

회수 구독자의 배관(폴링·커서)은 FastStream 몫이라, 테스트는 실제 XAUTOCLAIM 결과를
`main.reclaim_one` 에 그대로 넘겨 처리 규칙만 확인한다.
"""

import asyncio
import contextlib

import pytest
import pytest_asyncio
import redis.asyncio as aioredis

import src.main as main
from src.ai import FakeEmbedder, FakeEnricher, FakeStructurer
from src.contract import AnalysisStatus, ParseRequest
from src.storage.repository import count_chunks, get_parse_status
from src.contract.structured_data import StructuredData
from tests.conftest import DIM, requires_postgres, seed_resume
from tests.resume.test_pipeline import SD


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
    monkeypatch.setattr(main._Resources, "enricher", FakeEnricher())
    monkeypatch.setattr(main.settings, "embedding_dim", DIM)


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


async def _reclaim_all(redis, limit: int = 10) -> int:
    """구독자가 하는 일을 그대로 재현 — XAUTOCLAIM 으로 가져와 한 건씩 처리한다.

    구독자는 폴링마다 한 건씩 받으므로(FastStream 이 count=1 고정), 여기서도 같은 단위로
    `reclaim_one` 을 호출한다. 반환 = 처리 시도 건수.
    """
    processed = 0
    for _ in range(limit):
        _cursor, messages, _deleted = await redis.xautoclaim(
            name=STREAM,
            groupname=GROUP,
            consumername=main.settings.reclaim_consumer_name,
            min_idle_time=0,  # 테스트는 즉시 회수
            count=1,
        )
        if not messages:
            break
        message_id, fields = messages[0]
        await main.reclaim_one(redis, message_id, fields)
        processed += 1
    return processed


@pytest.mark.asyncio
async def test_방치메시지_회수해_재처리하고_ACK(conn, redis, wired):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    await _simulate_dead_worker(redis, rid)
    assert await _pending_count(redis) == 1  # 죽은 워커의 PEL 에 잔류

    assert await _reclaim_all(redis) == 1

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

    # 회수의 XAUTOCLAIM 이 3번째 전달 → 임계(3) 도달 → 재처리 없이 FAILED
    assert await _reclaim_all(redis) == 1

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
    """재처리 중 예외 → ACK 하지 않고 다음 폴링 대상으로 남긴다."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    await _simulate_dead_worker(redis, rid)

    async def broken(*args, **kwargs):
        raise ConnectionError("일시 오류")

    monkeypatch.setattr(main, "process_request", broken)
    await _reclaim_all(redis, limit=1)

    assert await _pending_count(redis) == 1  # 잔류 → 다음 회수 대상


@pytest.mark.asyncio
async def test_방치메시지_없으면_아무일도_안함(conn, redis, wired):
    assert await _reclaim_all(redis) == 0


@pytest.mark.asyncio
async def test_형식위반_메시지는_제거하고_나머지는_정상처리(conn, redis, wired):
    """decode 실패 메시지가 뒤의 메시지를 막지 않고, ACK 로 제거되어 무한 재회수가
    발생하지 않는다. 같은 PEL 의 정상 메시지는 그대로 처리된다."""
    # 형식이 틀린 메시지 (mode 가 계약에 없는 값) + 정상 메시지를 함께 PEL 에 넣는다
    await redis.xadd(STREAM, {"resumeId": "1", "userId": "1", "bucket": "b",
                              "objectKey": "k", "mode": "WRONG"})
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    await redis.xadd(STREAM, {"resumeId": str(rid), "userId": "1", "bucket": "b",
                              "objectKey": "k", "mode": "REINDEX"})
    await redis.xreadgroup(GROUP, "dead-worker", {STREAM: ">"}, count=10)
    assert await _pending_count(redis) == 2

    assert await _reclaim_all(redis) == 2  # 형식 위반 1건 + 정상 1건

    assert await get_parse_status(conn, rid) == "EMBEDDED"  # 정상 메시지는 처리됨
    assert await _pending_count(redis) == 0  # 형식 위반도 ACK 로 제거 → 재회수 없음


# --- 구독 배관 통합 검증 ---------------------------------------------------
# 위 테스트들은 reclaim_one 을 직접 호출하므로 구독자 배선(StreamSub 라우팅·AckPolicy·
# 커넥션 주입)은 타지 않는다. 실제로 그 사각지대에서 주입 타입 오류가 났었기에,
# 구독자를 띄워서 도는지 확인하는 테스트를 따로 둔다.


async def _run_subscriber(sub, seconds: float) -> None:
    """구독자 하나만 잠깐 띄운다 — 앱 전체 기동은 DB 스키마·AI 자원이 필요해 무겁다.

    브로커는 모듈 전역이라 앞 테스트의 이벤트 루프에 묶인 커넥션이 남는다. pytest-asyncio 는
    테스트마다 새 루프를 만들므로, 매번 끊고 다시 연결해야 "attached to a different loop" 가
    나지 않는다.
    """
    with contextlib.suppress(Exception):
        await main.broker.stop()
    await main.broker.connect()
    task = asyncio.create_task(sub.start())
    try:
        await asyncio.sleep(seconds)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        with contextlib.suppress(Exception):
            await main.broker.stop()


def _subscribers():
    reclaim = [s for s in main.broker._subscribers if s.stream_sub.min_idle_time is not None]
    fresh = [s for s in main.broker._subscribers if s.stream_sub.min_idle_time is None]
    assert len(reclaim) == 1 and len(fresh) == 1, "구독자는 회수용·새 메시지용 하나씩이어야 한다"
    return reclaim[0], fresh[0]


@pytest.mark.asyncio
async def test_구독자_배선_회수구독자가_방치메시지를_제거한다(redis, monkeypatch):
    """StreamSub(min_idle_time) 라우팅·AckPolicy.MANUAL·커넥션 주입이 실제로 맞물리는지.

    형식 위반 메시지를 쓰는 이유는 DB·AI 자원 없이도 끝까지 도달하기 때문이다
    (decode 실패 → 로그 → ACK).
    """
    reclaim_sub, _ = _subscribers()
    monkeypatch.setattr(reclaim_sub, "min_idle_time", 0)  # 즉시 회수 대상으로

    await redis.xadd(STREAM, {"resumeId": "1", "userId": "1", "bucket": "b",
                              "objectKey": "k", "mode": "WRONG"})
    await redis.xreadgroup(GROUP, "dead-worker", {STREAM: ">"}, count=10)
    assert await _pending_count(redis) == 1

    await _run_subscriber(reclaim_sub, 2.0)

    assert await _pending_count(redis) == 0  # 구독자가 회수해 제거
    consumers = {c["name"].decode() for c in await redis.xinfo_consumers(STREAM, GROUP)}
    assert main.settings.reclaim_consumer_name in consumers  # 회수 전용 컨슈머가 소유권을 가져감


@pytest.mark.asyncio
async def test_구독자_배선_회수구독자는_새메시지를_읽지_않는다(redis, monkeypatch):
    """min_idle_time 을 준 구독자는 XAUTOCLAIM 만 돈다 — 새 메시지는 PEL 에 없어 못 읽는다.

    FastStream 공식 문서에는 한 구독자가 둘 다 한다는 예시가 있으나 구현과 어긋난다
    (프로젝트 이슈 #2848·#2927). 구독자를 합치면 새 메시지가 조용히 안 읽히므로 고정한다.
    """
    reclaim_sub, fresh_sub = _subscribers()
    monkeypatch.setattr(reclaim_sub, "min_idle_time", 0)

    await redis.xadd(STREAM, {"resumeId": "1", "userId": "1", "bucket": "b",
                              "objectKey": "k", "mode": "WRONG"})

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
    위 배선 테스트들은 성공 경로만 보므로 실패 경로를 따로 둔다.
    """
    reclaim_sub, _ = _subscribers()
    monkeypatch.setattr(reclaim_sub, "min_idle_time", 0)

    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    await _simulate_dead_worker(redis, rid)
    assert await _pending_count(redis) == 1

    # 구독자가 아예 안 돌아도 PEL 은 1로 남으므로, 실패 경로를 정말 탔는지 표시를 남긴다
    called = asyncio.Event()

    async def broken(*args, **kwargs):
        called.set()
        raise ConnectionError("일시 오류")

    monkeypatch.setattr(main, "process_request", broken)

    await _run_subscriber(reclaim_sub, 2.0)

    assert called.is_set(), "회수 구독자가 메시지를 처리 함수까지 전달해야 한다"
    assert await _pending_count(redis) == 1, "처리 실패 메시지가 ACK 되면 안 된다"
