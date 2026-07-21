"""FastStream 워커 진입점 (§2, §3) — 자원·구독·회수 루프의 조립(배선)만 담당한다.

`resume.parse.requested` 를 consumer group 으로 소비해 분석 파이프라인
(`analysis.pipeline`)을 태우고, 단계마다 상태를 발행한다(`messaging.streams`).
방치 메시지 회수는 `messaging.reclaim` 이 담당한다.

실행: `faststream run src.main:app`
"""

from __future__ import annotations

import asyncio

from redis.asyncio import Redis

from faststream import FastStream
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub

from src.ai import Embedder, Enricher, Structurer, build_embedder, build_enricher, build_structurer
from src.analysis.extraction import build_s3_client, download_pdf, extract_text
from src.analysis.pipeline import process_request
from src.config import Settings, get_settings
from src.contract import AnalysisStatus, ParseRequest
from src.messaging.reclaim import reclaim_loop, reclaim_pending_once as _reclaim_once
from src.messaging.streams import get_delivery_count, publish_status
from src.storage.repository import connect, ensure_schema, record_last_error

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = FastStream(broker)


class _Resources:
    """기동 시 준비되는 공유 자원 (DB 커넥션·AI 제공자·S3·회수 루프)."""

    db = None  # psycopg AsyncConnection
    embedder: Embedder | None = None
    structurer: Structurer | None = None
    enricher: Enricher | None = None
    s3 = None  # boto3 client
    reclaim_task: asyncio.Task | None = None


async def fetch_text(bucket: str, object_key: str) -> str:
    """S3 다운로드 + PyMuPDF 추출. blocking 호출이라 스레드로 넘긴다."""

    def _work() -> str:
        pdf = download_pdf(_Resources.s3, bucket, object_key)
        return extract_text(pdf)

    return await asyncio.to_thread(_work)


async def _process(
    request: ParseRequest,
    delivery_count: int,
    redis: Redis,
    *,
    is_reclaimed: bool = False,
) -> None:
    """파이프라인 호출 배선 — 공유 자원·발행 콜백을 묶는다."""

    async def publish(rid: int, uid: int, status: AnalysisStatus, message: str) -> None:
        await publish_status(redis, rid, uid, status, message)

    try:
        await process_request(
            request,
            conn=_Resources.db,
            embedder=_Resources.embedder,
            structurer=_Resources.structurer,
            enricher=_Resources.enricher,
            fetch_text=fetch_text,
            publish=publish,
            settings=settings,
            delivery_count=delivery_count,
            is_reclaimed=is_reclaimed,
        )
    except Exception as e:
        # 예상 밖 예외도 원인 한 줄을 DB 에 남기고(§4 합류용, best-effort) 원래대로 전파한다
        # — 전파돼야 ACK 없이 끝나 PEL 재전달(회수)로 이어진다.
        await record_last_error(_Resources.db, request.resumeId, f"{type(e).__name__}: {e}")
        raise


@app.on_startup
async def startup() -> None:
    _Resources.db = await connect(settings)
    await ensure_schema(_Resources.db, dim=settings.embedding_dim)
    _Resources.embedder = build_embedder(settings)
    _Resources.structurer = build_structurer(settings)
    _Resources.enricher = build_enricher(settings)
    _Resources.s3 = build_s3_client(settings)
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
    if _Resources.db is not None:
        await _Resources.db.close()


@broker.subscriber(
    stream=StreamSub(
        ParseRequest.STREAM_KEY,
        group=settings.consumer_group,
        consumer=settings.resolved_consumer_name,
        # 주의: StreamSub 에 min_idle_time 을 주면 '새 메시지 읽기'가 아니라 '방치된 PEL 회수' 모드가
        # 되어 갓 들어온 메시지를 못 읽는다(실측 확인). 회수는 별도 루프 — messaging.reclaim (§3).
    )
)
async def handle_parse_requested(request: ParseRequest, msg: RedisStreamMessage) -> None:
    """분석 요청 처리 진입점.

    정상 반환 = ACK(종결·스킵·양보·포기), 예외 = PEL 잔류 → 회수 대상.
    """
    # 포기 규칙(§4) 판단 근거 — 이 메시지의 전달 횟수 (message id 없으면 1로 간주)
    message_ids = msg.raw_message.get("message_ids") or []
    delivery_count = (
        await get_delivery_count(
            broker._connection, settings.consumer_group, message_ids[0]
        )
        if message_ids
        else 1
    )
    await _process(request, delivery_count, broker._connection)


async def reclaim_pending_once(redis: Redis | None = None) -> int:
    """회수 1회 실행 — messaging.reclaim 에 현재 배선을 넘긴다 (테스트·수동 실행용)."""
    redis = redis if redis is not None else broker._connection
    return await _reclaim_once(
        redis,
        settings=settings,
        process=lambda req, count: _process(req, count, redis, is_reclaimed=True),
    )
