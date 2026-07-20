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

import asyncio
import logging

from redis.asyncio import Redis

from faststream import FastStream
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub

from src.ai import Embedder, Structurer, build_embedder, build_structurer
from src.config import Settings, get_settings
from src.contract import AnalysisStatus, ParseRequest, StatusChanged
from src.db import connect, ensure_schema
from src.extraction import build_s3_client, download_pdf, extract_text
from src.pipeline import process_request

logger = logging.getLogger(__name__)

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = FastStream(broker)


class _Resources:
    """기동 시 준비되는 공유 자원 (DB 커넥션·AI 제공자·S3·회수 루프)."""

    db = None  # psycopg AsyncConnection
    embedder: Embedder | None = None
    structurer: Structurer | None = None
    s3 = None  # boto3 client
    reclaim_task: asyncio.Task | None = None


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
    _Resources.reclaim_task = asyncio.create_task(reclaim_loop())


@app.on_shutdown
async def shutdown() -> None:
    if _Resources.reclaim_task is not None:
        _Resources.reclaim_task.cancel()
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
        # 되어 갓 들어온 메시지를 못 읽는다(실측 확인). 회수는 별도 루프로 구현 — reclaim_loop (§3).
    )
)
async def handle_parse_requested(request: ParseRequest, msg: RedisStreamMessage) -> None:
    """분석 요청 처리 진입점.

    정상 반환 = ACK(종결·스킵·양보·포기), 예외 = PEL 잔류 → 회수 대상.
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


# ---------------------------------------------------------------- 회수 (§3, XAUTOCLAIM)

def _decode_fields(fields: dict) -> dict[str, str]:
    """Redis 가 주는 bytes 필드맵을 계약 모델이 기대하는 str 맵으로."""
    return {
        (k.decode() if isinstance(k, bytes) else k): (
            v.decode() if isinstance(v, bytes) else v
        )
        for k, v in fields.items()
    }


async def reclaim_pending_once(redis: Redis | None = None) -> int:
    """ACK 없이 min_idle 이상 방치된 메시지를 회수해 재처리한다 (§3). 반환 = 처리 시도 건수.

    - 회수본도 포기 규칙(§4) 적용 — XAUTOCLAIM 이 delivery count 를 +1 시키므로,
      계속 실패하는 메시지는 회수 경로에서 임계에 도달해 FAILED 로 종결된다(무한 회수 차단).
    - 정상 반환(완료·스킵·양보·포기) 후 **직접 XACK** — 자동 ACK 은 구독 핸들러 전용이다.
      예외 시 ACK 하지 않아 다음 주기에 다시 회수된다.
    """
    redis = redis if redis is not None else broker._connection

    _cursor, messages, _deleted = await redis.xautoclaim(
        name=ParseRequest.STREAM_KEY,
        groupname=settings.consumer_group,
        consumername=settings.resolved_consumer_name,
        min_idle_time=settings.claim_min_idle_ms,
        count=settings.reclaim_batch_size,
    )

    processed = 0
    for message_id, fields in messages:
        request = ParseRequest.decode(_decode_fields(fields))
        delivery_count = await get_delivery_count(redis, message_id)

        async def publish(rid: int, uid: int, status: AnalysisStatus, message: str) -> None:
            await publish_status(redis, rid, uid, status, message)

        try:
            await process_request(
                request,
                conn=_Resources.db,
                embedder=_Resources.embedder,
                structurer=_Resources.structurer,
                fetch_text=fetch_text,
                publish=publish,
                settings=settings,
                delivery_count=delivery_count,
                is_reclaimed=True,  # 같은 런의 연장 — retry_count 리셋 안 함 (§3.2)
            )
        except Exception:
            logger.exception("회수 재처리 실패 — 다음 주기에 재시도 (resumeId=%s)", request.resumeId)
            continue  # ACK 안 함 → PEL 잔류 → 다음 회수 대상

        await redis.xack(ParseRequest.STREAM_KEY, settings.consumer_group, message_id)
        processed += 1
    return processed


async def reclaim_loop() -> None:
    """주기적으로 회수 시도 (§9: 주기 5분 — 최악 복구 지연 = min_idle + 주기 ≤ 10분).

    루프 자체는 어떤 예외에도 죽지 않는다 — 죽으면 크래시 복구 기능이 사라지므로.
    """
    while True:
        await asyncio.sleep(settings.reclaim_interval_s)
        try:
            reclaimed = await reclaim_pending_once()
            if reclaimed:
                logger.info("방치 메시지 %d건 회수 처리", reclaimed)
        except Exception:
            logger.exception("회수 루프 오류 — 다음 주기에 재시도")
