"""리포트 워커 진입점 — 자원·구독·회수 루프의 연결만 담당한다 (로직은 pipeline).

이력서 워커(src.main)와 **별개 프로세스**로 실행한다 — 장애·배포 격리:
    faststream run src.report.main:app

`report.generation.requested` 를 consumer group 으로 소비해 생성 파이프라인을 태우고,
상태 전이마다 `report.status.changed` 를 발행한다. 방치 메시지 회수는 report.reclaim.
"""

from __future__ import annotations

import asyncio

from redis.asyncio import Redis

from faststream import FastStream
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub

from src.config import Settings, get_settings
from src.contract import ReportGenerationRequested, ReportStatus
from src.report.evaluator import Evaluator, build_evaluator
from src.report.pipeline import process_generation_request
from src.report.reclaim import reclaim_loop, reclaim_pending_once as _reclaim_once
from src.report.repository import ensure_schema
from src.report.streams import get_delivery_count, publish_status
from src.storage.repository import connect

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = FastStream(broker)


class _Resources:
    """기동 시 준비되는 공유 자원 (평가기·회수 루프).

    DB 는 공유 커넥션을 두지 않는다 — 구독 핸들러와 회수 루프가 동시에 돌 때 한 세션의
    트랜잭션을 공유하면 서로의 작업을 커밋할 수 있어, **요청당 커넥션**을 연다
    (이력서 main 과 같은 이유). `db` 는 테스트 주입용(주입되면 그걸 쓰고 닫지 않음).
    """

    db = None  # 테스트 주입용 psycopg AsyncConnection (프로덕션은 요청당 연결)
    evaluator: Evaluator | None = None
    reclaim_task: asyncio.Task | None = None


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
    _Resources.reclaim_task = asyncio.create_task(
        reclaim_loop(
            lambda: broker._connection,
            settings=settings,
            process=lambda req, count: _process(
                req, count, broker._connection, is_reclaimed=True
            ),
        )
    )


@app.on_shutdown
async def shutdown() -> None:
    if _Resources.reclaim_task is not None:
        _Resources.reclaim_task.cancel()


@broker.subscriber(
    stream=StreamSub(
        ReportGenerationRequested.STREAM_KEY,
        group=settings.report_consumer_group,
        consumer=settings.resolved_consumer_name,
        # 주의: StreamSub 에 min_idle_time 을 주면 새 메시지를 못 읽는다(이력서에서 실측).
        # 회수는 별도 루프 — report.reclaim.
    ),
    # max_workers 를 주면 같은 스트림의 메시지 여러 건을 동시에 처리할 수 있다
    # (정합성은 CAS·유니크 제약이 이미 보장). 기본은 순차 — 메시지 대기가 실측에서
    # 문제로 나타나면 Bedrock 속도 한도를 확인한 뒤 켠다.
)
async def handle_generation_requested(
    request: ReportGenerationRequested, msg: RedisStreamMessage
) -> None:
    """생성 요청 처리 진입점.

    정상 반환 = ACK(종결·스킵·양보·포기), 예외 = PEL 잔류 → 회수 대상.
    형식 위반 메시지는 여기 도달 전에 decode 에서 실패해 PEL 에 남고,
    회수 경로가 로그 후 제거한다.
    """
    message_ids = msg.raw_message.get("message_ids") or []
    delivery_count = (
        await get_delivery_count(
            broker._connection, settings.report_consumer_group, message_ids[0]
        )
        if message_ids
        else 1
    )
    await _process(request, delivery_count, broker._connection)


async def reclaim_pending_once(redis: Redis | None = None) -> int:
    """회수 1회 실행 — report.reclaim 에 현재 연결을 넘긴다 (테스트·수동 실행용)."""
    redis = redis if redis is not None else broker._connection
    return await _reclaim_once(
        redis,
        settings=settings,
        process=lambda req, count: _process(req, count, redis, is_reclaimed=True),
    )
