"""FastStream 워커 진입점 (§2, §3).

`resume.parse.requested` 를 consumer group 으로 소비해 분석 파이프라인을 태우고,
단계마다 `resume.parse.status.changed` 를 발행한다.

실행: `faststream run src.main:app`

## Redis Stream 상호운용 (실 Redis 로 확인, 2026-07-17)
- Spring 은 각 필드를 **네이티브 스트림 필드**로 XADD 한다(`mapBacked`).
- **소비**: FastStream 이 네이티브 필드를 읽어 모델로 검증해준다 → 핸들러가 `ParseRequest` 를 바로 받는다.
- **발행**: FastStream 기본 발행은 `__data__` 바이너리 봉투로 감싸 Spring 이 못 읽는다.
  → 상태 발행은 redis 커넥션으로 **네이티브 필드**를 직접 XADD 한다(`publish_status`).
"""

from __future__ import annotations

from redis.asyncio import Redis

from faststream import FastStream
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub

from src.ai import Embedder, Structurer, build_embedder, build_structurer
from src.config import Settings, get_settings
from src.contract import AnalysisStatus, ParseRequest, StatusChanged
from src.db import connect, ensure_schema
from src.extraction import build_s3_client, download_pdf, extract_text
from src.pipeline import process_request

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = FastStream(broker)


class _Resources:
    """기동 시 준비되는 공유 자원 (DB 커넥션·AI 제공자·S3)."""

    db = None  # psycopg AsyncConnection
    embedder: Embedder | None = None
    structurer: Structurer | None = None
    s3 = None  # boto3 client


async def fetch_text(bucket: str, object_key: str) -> str:
    """S3 다운로드 + PyMuPDF 추출. blocking 호출이라 스레드로 넘긴다."""
    import asyncio

    def _work() -> str:
        pdf = download_pdf(_Resources.s3, bucket, object_key)
        return extract_text(pdf)

    return await asyncio.to_thread(_work)


@app.on_startup
async def startup() -> None:
    _Resources.db = await connect(settings)
    await ensure_schema(_Resources.db, dim=settings.embedding_dim)
    _Resources.embedder = build_embedder(settings)
    _Resources.structurer = build_structurer(settings)
    _Resources.s3 = build_s3_client(settings)


@app.on_shutdown
async def shutdown() -> None:
    if _Resources.db is not None:
        await _Resources.db.close()


async def get_delivery_count(redis: Redis, message_id: str) -> int:
    """이 메시지가 몇 번째 전달인지 (Redis PEL 의 times_delivered).

    포기 규칙(§4)의 판단 근거. PEL 에서 못 찾으면(이미 ACK 등) 1로 간주한다.
    """
    entries = await redis.xpending_range(
        ParseRequest.STREAM_KEY,
        settings.consumer_group,
        min=message_id,
        max=message_id,
        count=1,
    )
    return entries[0]["times_delivered"] if entries else 1


async def publish_status(
    redis: Redis,
    resume_id: int,
    user_id: int,
    status: AnalysisStatus,
    message: str = "",
) -> None:
    """상태 이벤트를 Spring 이 읽는 **네이티브 필드** 형식으로 발행한다.

    FastStream 기본 발행(`__data__` 봉투)이 아니라 redis 커넥션으로 직접 XADD 한다.
    """
    payload = StatusChanged(
        resumeId=resume_id, userId=user_id, status=status, message=message
    ).encode()
    await redis.xadd(StatusChanged.STREAM_KEY, payload)


@broker.subscriber(
    stream=StreamSub(
        ParseRequest.STREAM_KEY,
        group=settings.consumer_group,
        consumer=settings.resolved_consumer_name,
        # 주의: StreamSub 에 min_idle_time 을 주면 '새 메시지 읽기'가 아니라 '방치된 PEL 회수' 모드가
        # 되어 갓 들어온 메시지를 못 읽는다(실측 확인). 회수(XAUTOCLAIM, §3)는 별도로 구현한다. TODO(§3).
    )
)
async def handle_parse_requested(request: ParseRequest, msg: RedisStreamMessage) -> None:
    """분석 요청 처리 진입점.

    정상 반환 = ACK(종결·스킵·양보·포기), 예외 = PEL 잔류 → 회수 대상.
    TODO(§3): XAUTOCLAIM 회수 루프.
    """

    async def publish(rid: int, uid: int, status: AnalysisStatus, message: str) -> None:
        await publish_status(broker._connection, rid, uid, status, message)

    # 포기 규칙(§4) 판단 근거 — 이 메시지의 전달 횟수 (message id 없으면 1로 간주)
    message_ids = msg.raw_message.get("message_ids") or []
    delivery_count = (
        await get_delivery_count(broker._connection, message_ids[0]) if message_ids else 1
    )

    await process_request(
        request,
        conn=_Resources.db,
        embedder=_Resources.embedder,
        structurer=_Resources.structurer,
        fetch_text=fetch_text,
        publish=publish,
        settings=settings,
        delivery_count=delivery_count,
    )
