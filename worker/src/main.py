"""FastStream 워커 진입점 (§2, §3) — 자원·구독의 조립(배선)만 담당한다.

`resume.parse.requested` 를 consumer group 으로 소비해 분석 파이프라인
(`analysis.pipeline`)을 태우고, 단계마다 상태를 발행한다(`messaging.streams`).

구독자는 둘이다 — 새 메시지용과 방치 메시지 회수용. `StreamSub` 에 `min_idle_time` 을
주면 XREADGROUP 대신 XAUTOCLAIM 을 도는 회수 전용 모드가 되어 갓 들어온 메시지를 못
읽으므로(실측 확인), 한 구독자가 둘 다 할 수 없어 나눈다.

실행: `faststream run src.main:app`
"""

from __future__ import annotations

import asyncio
import logging

from redis.asyncio import Redis

from faststream import AckPolicy, FastStream
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub

from src.ai import Embedder, Enricher, Structurer, build_embedder, build_enricher, build_structurer
from src.analysis.extraction import build_s3_client, download_pdf, extract_text
from src.analysis.pipeline import process_request
from src.config import Settings, get_settings
from src.contract import AnalysisStatus, ParseRequest
from src.contract.fields import decode_fields
from src.messaging.pel import get_delivery_count
from src.messaging.streams import publish_status
from src.storage.repository import connect, ensure_schema, record_last_error

logger = logging.getLogger(__name__)

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = FastStream(broker)


class _Resources:
    """기동 시 준비되는 공유 자원 (AI 제공자·S3).

    DB 는 공유 커넥션을 두지 않는다 — 두 구독자(새 메시지·회수)가 동시에 돌 때 한 세션의
    트랜잭션을 공유하면 서로의 작업을 커밋할 수 있어, **요청당 커넥션**을 연다(§3.3 안전).
    `db` 는 테스트 주입용(주입되면 그걸 쓰고 닫지 않음). 물량이 늘면 커넥션 풀로 교체.
    """

    db = None  # 테스트 주입용 psycopg AsyncConnection (프로덕션은 요청당 연결)
    embedder: Embedder | None = None
    structurer: Structurer | None = None
    enricher: Enricher | None = None
    s3 = None  # boto3 client


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

    # 요청당 커넥션 — 공유 세션의 트랜잭션 섞임 방지 (테스트가 주입한 커넥션은 재사용·비소유)
    injected = _Resources.db is not None
    conn = _Resources.db if injected else await connect(settings)
    try:
        await process_request(
            request,
            conn=conn,
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
        # 예상 밖 예외도 원인을 DB 에 남기고(§4 합류용, best-effort) 원래대로 전파한다
        # — 전파돼야 ACK 없이 끝나 PEL 재전달(회수)로 이어진다.
        # DB 에는 예외 타입명만 기록: 원문에는 접속 문자열 등 내부 정보가 섞일 수 있고,
        # error_message 는 백엔드 조회 API 로 노출될 수 있다. 원문 전체는 로그가 담당.
        await record_last_error(conn, request.resumeId, type(e).__name__)
        raise
    finally:
        if not injected:
            await conn.close()


@app.on_startup
async def startup() -> None:
    schema_conn = await connect(settings)
    try:
        await ensure_schema(schema_conn, dim=settings.embedding_dim)
    finally:
        await schema_conn.close()
    _Resources.embedder = build_embedder(settings)
    _Resources.structurer = build_structurer(settings)
    _Resources.enricher = build_enricher(settings)
    _Resources.s3 = build_s3_client(settings)


async def _delivery_count_of(redis: Redis, msg: RedisStreamMessage) -> int:
    """포기 규칙(§4) 판단 근거 — 이 메시지의 전달 횟수 (message id 없으면 1로 간주)."""
    message_ids = msg.raw_message.get("message_ids") or []
    if not message_ids:
        return 1
    return await get_delivery_count(
        redis, ParseRequest.STREAM_KEY, settings.consumer_group, message_ids[0]
    )


@broker.subscriber(
    stream=StreamSub(
        ParseRequest.STREAM_KEY,
        group=settings.consumer_group,
        consumer=settings.resolved_consumer_name,
    )
)
async def handle_parse_requested(request: ParseRequest, msg: RedisStreamMessage) -> None:
    """새 분석 요청 처리 진입점.

    정상 반환 = ACK(종결·스킵·양보·포기), 예외 = PEL 잔류 → 회수 대상.
    """
    delivery_count = await _delivery_count_of(broker._connection, msg)
    await _process(request, delivery_count, broker._connection)


async def reclaim_one(redis: Redis, message_id: bytes, fields: dict) -> None:
    """회수된 메시지 한 건 처리 — 계약 위반은 제거하고, 처리 실패는 PEL 에 남긴다.

    ACK 을 직접 가르므로 구독자는 `AckPolicy.MANUAL` 이다. 구독 배관에서 떼어놓아
    실제 XAUTOCLAIM 결과를 그대로 넘겨 테스트할 수 있게 한다.
    """

    async def ack() -> None:
        await redis.xack(ParseRequest.STREAM_KEY, settings.consumer_group, message_id)

    try:
        request = ParseRequest.decode(decode_fields(fields))
    except Exception:
        # 형식이 틀린 메시지는 재처리도, FAILED 기록도 불가능하다(resumeId 를 못 읽음).
        # ACK 하지 않으면 폴링마다 재회수가 반복되므로, 원본 필드를 로그에 남기고 제거한다.
        logger.exception("형식 위반 메시지 제거 (id=%s, fields=%r)", message_id, fields)
        await ack()
        return

    try:
        delivery_count = await get_delivery_count(
            redis, ParseRequest.STREAM_KEY, settings.consumer_group, message_id
        )
        await _process(request, delivery_count, redis, is_reclaimed=True)
        await ack()
    except Exception:
        # ACK 하지 않아 PEL 에 남고 다음 폴링의 회수 대상이 된다.
        logger.exception("회수 재처리 실패 (resumeId=%s)", request.resumeId)


@broker.subscriber(
    stream=StreamSub(
        ParseRequest.STREAM_KEY,
        group=settings.consumer_group,
        consumer=settings.reclaim_consumer_name,
        # min_idle_time 을 주면 XREADGROUP 대신 XAUTOCLAIM 을 도는 회수 전용 구독자가 된다.
        min_idle_time=settings.claim_min_idle_ms,
        polling_interval=settings.reclaim_poll_interval_ms,
    ),
    ack_policy=AckPolicy.MANUAL,
)
async def handle_reclaimed(msg: RedisStreamMessage, redis: Redis) -> None:
    """회수 구독자 — 배선만 하고 처리는 `reclaim_one` 이 한다."""
    message_ids = msg.raw_message.get("message_ids") or []
    if not message_ids:  # 회수 경로는 항상 한 건씩 오지만 방어적으로
        return
    await reclaim_one(redis, message_ids[0], msg.raw_message["data"])
