"""리포트 워커 진입점 — 자원·구독의 연결만 담당한다 (로직은 pipeline).

이력서 워커(src.main)와 **별개 프로세스**로 실행한다 — 장애·배포 격리:
    faststream run src.report.main:app

`report.generation.requested` 를 consumer group 으로 소비해 생성 파이프라인을 태우고,
상태 전이마다 `report.status.changed` 를 발행한다.

구독자는 둘이다 — 새 메시지용과 방치 메시지 회수용. `StreamSub` 에 `min_idle_time` 을
주면 XREADGROUP 대신 XAUTOCLAIM 을 도는 회수 전용 모드가 되어 갓 들어온 메시지를 못
읽으므로(이력서에서 실측), 한 구독자가 둘 다 할 수 없어 나눈다.
"""

from __future__ import annotations

import logging

from redis.asyncio import Redis

from faststream import AckPolicy, FastStream
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub
from faststream.redis.annotations import Redis as InjectedRedis  # Context 주입용 애노테이션

from src.config import Settings, get_settings
from src.contract import ReportGenerationRequested, ReportStatus
from src.contract.fields import decode_fields
from src.messaging.pel import get_delivery_count
from src.report.evaluator import Evaluator, build_evaluator
from src.report.pipeline import process_generation_request
from src.report.repository import ensure_schema
from src.report.publish import publish_status
from src.storage.repository import connect

logger = logging.getLogger(__name__)

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = FastStream(broker)


class _Resources:
    """기동 시 준비되는 공유 자원 (평가기).

    DB 는 공유 커넥션을 두지 않는다 — 두 구독자(새 메시지·회수)가 동시에 돌 때 한 세션의
    트랜잭션을 공유하면 서로의 작업을 커밋할 수 있어, **요청당 커넥션**을 연다
    (이력서 main 과 같은 이유). `db` 는 테스트 주입용(주입되면 그걸 쓰고 닫지 않음).
    """

    db = None  # 테스트 주입용 psycopg AsyncConnection (프로덕션은 요청당 연결)
    evaluator: Evaluator | None = None


async def _process(
    request: ReportGenerationRequested,
    delivery_count: int,
    redis: Redis,
    *,
    is_reclaimed: bool = False,
) -> None:
    """파이프라인 호출 연결 — 커넥션·평가기·상태 발행을 묶는다."""

    async def publish(rid: int, uid: int, status: ReportStatus, message: str) -> None:
        await publish_status(redis, rid, uid, status, message)

    injected = _Resources.db is not None
    conn = _Resources.db if injected else await connect(settings)
    try:
        await process_generation_request(
            request,
            conn=conn,
            evaluator=_Resources.evaluator,
            publish=publish,
            settings=settings,
            delivery_count=delivery_count,
            is_reclaimed=is_reclaimed,
        )
    finally:
        if not injected:
            await conn.close()


@app.on_startup
async def startup() -> None:
    schema_conn = await connect(settings)
    try:
        await ensure_schema(schema_conn)  # 워커 소유 report_generation_jobs 멱등 생성
    finally:
        await schema_conn.close()
    _Resources.evaluator = build_evaluator(settings)


async def _delivery_count_of(redis: Redis, msg: RedisStreamMessage) -> int:
    """포기 규칙 판단 근거 — 이 메시지의 전달 횟수 (message id 없으면 1로 간주)."""
    message_ids = msg.raw_message.get("message_ids") or []
    if not message_ids:
        return 1
    return await get_delivery_count(
        redis,
        ReportGenerationRequested.STREAM_KEY,
        settings.report_consumer_group,
        message_ids[0],
    )


@broker.subscriber(
    stream=StreamSub(
        ReportGenerationRequested.STREAM_KEY,
        group=settings.report_consumer_group,
        consumer=settings.resolved_consumer_name,
    ),
    # max_workers 를 주면 같은 스트림의 메시지 여러 건을 동시에 처리할 수 있다
    # (정합성은 CAS·유니크 제약이 이미 보장). 기본은 순차 — 메시지 대기가 실측에서
    # 문제로 나타나면 Bedrock 속도 한도를 확인한 뒤 켠다.
)
async def handle_generation_requested(
    request: ReportGenerationRequested, msg: RedisStreamMessage
) -> None:
    """새 생성 요청 처리 진입점.

    정상 반환 = ACK(종결·스킵·양보·포기), 예외 = PEL 잔류 → 회수 대상.
    형식 위반 메시지는 여기 도달 전에 decode 에서 실패해 PEL 에 남고,
    회수 경로가 로그 후 제거한다.
    """
    delivery_count = await _delivery_count_of(broker._connection, msg)
    await _process(request, delivery_count, broker._connection)


async def reclaim_one(redis: Redis, message_id: bytes, fields: dict) -> None:
    """회수된 메시지 한 건 처리 — 계약 위반은 제거하고, 처리 실패는 PEL 에 남긴다.

    ACK 을 직접 가르므로 구독자는 `AckPolicy.MANUAL` 이다. 구독 배관에서 떼어놓아
    실제 XAUTOCLAIM 결과를 그대로 넘겨 테스트할 수 있게 한다.
    """

    async def ack() -> None:
        await redis.xack(
            ReportGenerationRequested.STREAM_KEY, settings.report_consumer_group, message_id
        )

    try:
        request = ReportGenerationRequested.decode(decode_fields(fields))
    except Exception:
        # 형식이 틀린 메시지는 재처리도, FAILED 기록도 불가능하다(sessionId 를 못 읽음).
        # ACK 하지 않으면 폴링마다 재회수가 반복되므로, 원본 필드를 로그에 남기고 제거한다.
        logger.exception("형식 위반 메시지 제거 (id=%s, fields=%r)", message_id, fields)
        await ack()
        return

    try:
        delivery_count = await get_delivery_count(
            redis,
            ReportGenerationRequested.STREAM_KEY,
            settings.report_consumer_group,
            message_id,
        )
        await _process(request, delivery_count, redis, is_reclaimed=True)
        await ack()
    except Exception:
        # ACK 하지 않아 PEL 에 남고 다음 폴링의 회수 대상이 된다.
        logger.exception("회수 재처리 실패 (sessionId=%s)", request.sessionId)


@broker.subscriber(
    stream=StreamSub(
        ReportGenerationRequested.STREAM_KEY,
        group=settings.report_consumer_group,
        consumer=settings.reclaim_consumer_name,
        # min_idle_time 을 주면 XREADGROUP 대신 XAUTOCLAIM 을 도는 회수 전용 구독자가 된다.
        min_idle_time=settings.claim_min_idle_ms,
        polling_interval=settings.reclaim_poll_interval_ms,
    ),
    ack_policy=AckPolicy.MANUAL,
)
async def handle_reclaimed(msg: RedisStreamMessage, redis: InjectedRedis) -> None:
    """회수 구독자 — 배선만 하고 처리는 `reclaim_one` 이 한다.

    `redis` 는 FastStream 이 Context 로 넣어주는 커넥션이다. `redis.asyncio.Redis` 를
    그대로 힌트로 쓰면 주입되지 않고 검증 오류가 난다.
    """
    message_ids = msg.raw_message.get("message_ids") or []
    if not message_ids:  # 회수 경로는 항상 한 건씩 오지만 방어적으로
        return
    await reclaim_one(redis, message_ids[0], msg.raw_message["data"])
