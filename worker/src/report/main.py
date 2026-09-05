"""리포트 워커 진입점 — 자원·구독의 연결만 담당한다 (로직은 pipeline·audio_pipeline).

이력서 워커(src.main)와 **별개 프로세스**로 실행한다 — 장애·배포 격리:
    faststream run src.report.main:app

소비하는 스트림은 둘이다 (백엔드 PRD 리포트 §2 "시작 신호는 요청 2개"):
- `report.generation.requested` → 텍스트 분석(1단계), 리포트 로우 생성부터 담당
- `report.audio.analysis.requested` → 음성 분석(2단계), 전달력 산출
상태 전이마다 `report.status.changed` 를 발행한다.

스트림마다 구독자가 둘이다 — 새 메시지용과 방치 메시지 회수용. `StreamSub` 에
`min_idle_time` 을 주면 XREADGROUP 대신 XAUTOCLAIM 을 도는 회수 전용 모드가 되어 갓
들어온 메시지를 못 읽으므로(이력서에서 실측), 한 구독자가 둘 다 할 수 없어 나눈다.

유예 완성(음성이 늦으면 전달력 없이 완성)은 구독자가 아니라 주기 작업이다 — 음성 요청이
아예 오지 않는 경우(녹음 실패·발행 유실)를 잡아야 하므로 메시지에 기대 수 없다.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Awaitable, Callable

from redis.asyncio import Redis

from faststream import AckPolicy, FastStream
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub
from faststream.redis.annotations import Redis as InjectedRedis  # Context 주입용 애노테이션

from src.analysis.extraction import build_s3_client
from src.config import Settings, get_settings
from src.contract import AudioAnalysisRequested, ReportGenerationRequested, ReportStatus
from src.contract.fields import decode_fields
from src.messaging.pel import get_delivery_count
from src.report.audio_pipeline import process_audio_request
from src.report.evaluator import Evaluator, build_evaluator
from src.report.pipeline import process_generation_request
from src.report.repository import complete_overdue_audio, ensure_schema
from src.report.streams import publish_status
from src.storage.repository import connect

logger = logging.getLogger(__name__)

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = FastStream(broker)

GENERATION_STREAM = ReportGenerationRequested.STREAM_KEY
AUDIO_STREAM = AudioAnalysisRequested.STREAM_KEY


class _Resources:
    """기동 시 준비되는 공유 자원 (평가기·S3 클라이언트).

    DB 는 공유 커넥션을 두지 않는다 — 여러 구독자가 동시에 돌 때 한 세션의 트랜잭션을
    공유하면 서로의 작업을 커밋할 수 있어, **요청당 커넥션**을 연다(이력서 main 과 같은
    이유). `db` 는 테스트 주입용(주입되면 그걸 쓰고 닫지 않음).
    """

    db = None  # 테스트 주입용 psycopg AsyncConnection (프로덕션은 요청당 연결)
    evaluator: Evaluator | None = None
    s3 = None  # boto3 client — 녹음 다운로드
    grace_task: asyncio.Task | None = None


def _publisher(redis: Redis):
    async def publish(rid: int, uid: int, status: ReportStatus, message: str) -> None:
        await publish_status(redis, rid, uid, status, message)

    return publish


@contextlib.asynccontextmanager
async def _connection():
    """요청당 커넥션 — 테스트가 주입한 커넥션이 있으면 그걸 쓰고 닫지 않는다."""
    injected = _Resources.db is not None
    conn = _Resources.db if injected else await connect(settings)
    try:
        yield conn
    finally:
        if not injected:
            await conn.close()


async def _process(
    request: ReportGenerationRequested,
    delivery_count: int,
    redis: Redis,
    *,
    is_reclaimed: bool = False,
) -> None:
    """생성 파이프라인 호출 연결 — 커넥션·평가기·상태 발행을 묶는다."""
    async with _connection() as conn:
        await process_generation_request(
            request,
            conn=conn,
            evaluator=_Resources.evaluator,
            publish=_publisher(redis),
            settings=settings,
            delivery_count=delivery_count,
            is_reclaimed=is_reclaimed,
        )


async def _process_audio(
    request: AudioAnalysisRequested, delivery_count: int, redis: Redis
) -> None:
    """음성 파이프라인 호출 연결 — 커넥션·S3·상태 발행을 묶는다."""
    async with _connection() as conn:
        await process_audio_request(
            request,
            conn=conn,
            s3=_Resources.s3,
            publish=_publisher(redis),
            settings=settings,
            delivery_count=delivery_count,
        )


# ---------------------------------------------------------------- 기동·유예 완성

@app.on_startup
async def startup() -> None:
    schema_conn = await connect(settings)
    try:
        await ensure_schema(schema_conn)  # 워커 소유 report_generation_jobs 멱등 생성
    finally:
        await schema_conn.close()
    _Resources.evaluator = build_evaluator(settings)
    _Resources.s3 = build_s3_client(settings)


@app.after_startup
async def start_grace_loop() -> None:
    """음성 경로가 켜져 있을 때만 유예 완성 주기 작업을 띄운다 (꺼져 있으면 텍스트만으로 즉시 완성)."""
    if settings.audio_analysis_enabled and _Resources.grace_task is None:
        _Resources.grace_task = asyncio.create_task(_grace_loop(broker._connection))


@app.on_shutdown
async def stop_grace_loop() -> None:
    task, _Resources.grace_task = _Resources.grace_task, None
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def _grace_loop(redis: Redis) -> None:
    while True:
        await asyncio.sleep(settings.audio_grace_poll_interval_s)
        try:
            await complete_overdue_once(redis)
        except Exception:
            logger.exception("유예 완성 주기 작업 실패 — 다음 주기에 재시도")


async def complete_overdue_once(redis: Redis) -> int:
    """유예 완성 1회 — 텍스트는 끝났는데 음성이 유예 시간을 넘긴 리포트를 완성하고 발행한다."""
    async with _connection() as conn:
        rows = await complete_overdue_audio(conn, settings.audio_grace_seconds)
    publish = _publisher(redis)
    for row in rows:
        logger.warning("음성 분석 유예 초과 — 전달력 없이 완성 (report_id=%s)", row["id"])
        await publish(row["id"], row["user_id"], ReportStatus.COMPLETED, "")
    return len(rows)


# ---------------------------------------------------------------- 새 메시지 구독자

async def _delivery_count_of(redis: Redis, stream: str, msg: RedisStreamMessage) -> int:
    """포기 규칙 판단 근거 — 이 메시지의 전달 횟수 (message id 없으면 1로 간주)."""
    message_ids = msg.raw_message.get("message_ids") or []
    if not message_ids:
        return 1
    return await get_delivery_count(
        redis, stream, settings.report_consumer_group, message_ids[0]
    )


@broker.subscriber(
    stream=StreamSub(
        GENERATION_STREAM,
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
    delivery_count = await _delivery_count_of(broker._connection, GENERATION_STREAM, msg)
    await _process(request, delivery_count, broker._connection)


@broker.subscriber(
    stream=StreamSub(
        AUDIO_STREAM,
        group=settings.report_consumer_group,
        consumer=settings.resolved_consumer_name,
    ),
)
async def handle_audio_requested(
    request: AudioAnalysisRequested, msg: RedisStreamMessage
) -> None:
    """새 음성 분석 요청 처리 진입점 — 규칙은 생성 요청과 같다(정상 반환 = ACK)."""
    delivery_count = await _delivery_count_of(broker._connection, AUDIO_STREAM, msg)
    await _process_audio(request, delivery_count, broker._connection)


# ---------------------------------------------------------------- 회수 구독자

async def _reclaim(
    redis: Redis,
    message_id: bytes,
    fields: dict,
    *,
    stream: str,
    decode: Callable[[dict], object],
    run: Callable[[object, int], Awaitable[None]],
) -> None:
    """회수된 메시지 한 건 처리 — 계약 위반은 제거하고, 처리 실패는 PEL 에 남긴다.

    ACK 을 직접 가르므로 구독자는 `AckPolicy.MANUAL` 이다. 구독 배관에서 떼어놓아
    실제 XAUTOCLAIM 결과를 그대로 넘겨 테스트할 수 있게 한다. 두 스트림이 규칙을
    공유하므로 계약 decode 와 처리 함수만 인자로 받는다.
    """

    async def ack() -> None:
        await redis.xack(stream, settings.report_consumer_group, message_id)

    try:
        request = decode(decode_fields(fields))
    except Exception:
        # 형식이 틀린 메시지는 재처리도, FAILED 기록도 불가능하다(sessionId 를 못 읽음).
        # ACK 하지 않으면 폴링마다 재회수가 반복되므로, 원본 필드를 로그에 남기고 제거한다.
        logger.exception("형식 위반 메시지 제거 (stream=%s, id=%s, fields=%r)", stream, message_id, fields)
        await ack()
        return

    try:
        delivery_count = await get_delivery_count(
            redis, stream, settings.report_consumer_group, message_id
        )
        await run(request, delivery_count)
        await ack()
    except Exception:
        # ACK 하지 않아 PEL 에 남고 다음 폴링의 회수 대상이 된다.
        logger.exception("회수 재처리 실패 (stream=%s, sessionId=%s)", stream, request.sessionId)


async def reclaim_one(redis: Redis, message_id: bytes, fields: dict) -> None:
    """생성 요청 회수 1건."""

    async def run(request, delivery_count: int) -> None:
        await _process(request, delivery_count, redis, is_reclaimed=True)

    await _reclaim(
        redis, message_id, fields,
        stream=GENERATION_STREAM, decode=ReportGenerationRequested.decode, run=run,
    )


async def reclaim_audio_one(redis: Redis, message_id: bytes, fields: dict) -> None:
    """음성 분석 요청 회수 1건 — 분석은 결정적이라 재개 구분 없이 같은 흐름을 다시 탄다."""

    async def run(request, delivery_count: int) -> None:
        await _process_audio(request, delivery_count, redis)

    await _reclaim(
        redis, message_id, fields,
        stream=AUDIO_STREAM, decode=AudioAnalysisRequested.decode, run=run,
    )


def _reclaim_sub(stream: str) -> StreamSub:
    return StreamSub(
        stream,
        group=settings.report_consumer_group,
        consumer=settings.reclaim_consumer_name,
        # min_idle_time 을 주면 XREADGROUP 대신 XAUTOCLAIM 을 도는 회수 전용 구독자가 된다.
        min_idle_time=settings.claim_min_idle_ms,
        polling_interval=settings.reclaim_poll_interval_ms,
    )


def _message_id_of(msg: RedisStreamMessage) -> bytes | None:
    message_ids = msg.raw_message.get("message_ids") or []
    return message_ids[0] if message_ids else None  # 회수 경로는 항상 한 건씩 오지만 방어적으로


@broker.subscriber(stream=_reclaim_sub(GENERATION_STREAM), ack_policy=AckPolicy.MANUAL)
async def handle_reclaimed(msg: RedisStreamMessage, redis: InjectedRedis) -> None:
    """생성 요청 회수 구독자 — 배선만 하고 처리는 `reclaim_one` 이 한다.

    `redis` 는 FastStream 이 Context 로 넣어주는 커넥션이다. `redis.asyncio.Redis` 를
    그대로 힌트로 쓰면 주입되지 않고 검증 오류가 난다.
    """
    if (message_id := _message_id_of(msg)) is not None:
        await reclaim_one(redis, message_id, msg.raw_message["data"])


@broker.subscriber(stream=_reclaim_sub(AUDIO_STREAM), ack_policy=AckPolicy.MANUAL)
async def handle_audio_reclaimed(msg: RedisStreamMessage, redis: InjectedRedis) -> None:
    """음성 분석 요청 회수 구독자."""
    if (message_id := _message_id_of(msg)) is not None:
        await reclaim_audio_one(redis, message_id, msg.raw_message["data"])
