"""FastStream 워커 진입점 (§2, §3) — 자원·구독의 조립(배선)만 담당한다.

`resume.parse.requested` 를 consumer group 으로 소비해 분석 파이프라인
(`analysis.pipeline`)을 태우고, 단계마다 상태를 발행한다(`messaging.publish`).

구독자는 둘이다 — 새 메시지용과 방치 메시지 회수용. `StreamSub` 에 `min_idle_time` 을
주면 XREADGROUP 대신 XAUTOCLAIM 을 도는 회수 전용 모드가 되어 갓 들어온 메시지를 못
읽으므로(실측 확인), 한 구독자가 둘 다 할 수 없어 나눈다.

동기 디스패치 실험(PRD §11)으로 같은 프로세스에 HTTP 엔드포인트
(`POST /internal/analyses/resume`)도 서빙한다 — 앱은 `AsgiFastStream`.

실행: `faststream run src.main:app` (HTTP 는 uvicorn 기본 127.0.0.1:8000)
"""

from __future__ import annotations

import asyncio
import logging

from psycopg import AsyncConnection
from psycopg_pool import PoolTimeout
from pydantic import ValidationError
from redis.asyncio import Redis

from faststream import AckPolicy
from faststream.asgi import AsgiFastStream, AsgiResponse, Request, post
from faststream.asgi.response import JSONResponse
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub
from faststream.redis.annotations import Redis as InjectedRedis  # Context 주입용 애노테이션

from src.ai import Embedder, Enricher, Structurer, build_embedder, build_enricher, build_structurer
from src.analysis.extraction import build_s3_client, download_pdf, extract_text
from src.analysis.pipeline import process_request
from src.config import Settings, get_settings
from src.contract import AnalysisStatus, ParseRequest
from src.contract.fields import decode_fields
from src.messaging.pel import get_delivery_count
from src.messaging.publish import publish_status
from src.storage.repository import (
    connect,
    create_pool,
    ensure_schema,
    get_error_message,
    get_parse_status,
    record_last_error,
)

logger = logging.getLogger(__name__)

settings: Settings = get_settings()
broker = RedisBroker(settings.redis_url)
app = AsgiFastStream(broker)


class _Resources:
    """기동 시 준비되는 공유 자원 (AI 제공자·S3·커넥션 풀).

    DB 는 공유 커넥션을 두지 않는다 — 동시 작업들이 한 세션의 트랜잭션을 공유하면
    서로의 작업을 커밋할 수 있어(§3.3 안전), **작업당 풀에서 전용 연결을 빌려** 쓴다.
    `db` 는 테스트 주입용(주입되면 그걸 쓰고 닫지 않음).
    """

    db = None  # 테스트 주입용 psycopg AsyncConnection
    pool = None  # HTTP·스트림 두 경로가 공유하는 커넥션 풀 (§11.4) — PG 연결 총량 = 풀 크기
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
    conn: AsyncConnection | None = None,
) -> None:
    """파이프라인 호출 배선 — 공유 자원·발행 콜백을 묶는다.

    커넥션 선택 우선순위 (§11.4 — 커넥션은 가져온 쪽이 반환한다):
    1. `conn` 인자 — HTTP 경로가 풀에서 가져와 넘긴 커넥션 (상태 재조회까지 같이 쓰려고)
    2. `_Resources.db` — 테스트 주입
    3. `_Resources.pool` 에서 가져옴 — 스트림·회수 경로. 못 가져오면 예외 → ACK 없음 → PEL 회수
    4. 요청당 새 커넥션 — 풀 없는 테스트 폴백 (공유 세션의 트랜잭션 섞임 방지, §3.3)
    """

    async def publish(rid: int, uid: int, status: AnalysisStatus, message: str) -> None:
        await publish_status(redis, rid, uid, status, message)

    async def run(conn: AsyncConnection) -> None:
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

    if conn is not None:
        await run(conn)
    elif _Resources.db is not None:
        await run(_Resources.db)
    elif _Resources.pool is not None:
        async with _Resources.pool.connection(
            timeout=settings.db_pool_wait_timeout_s
        ) as pooled:
            await run(pooled)
    else:
        fresh = await connect(settings)
        try:
            await run(fresh)
        finally:
            await fresh.close()


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
    _Resources.pool = create_pool(settings, max_size=settings.db_pool_max_size)
    await _Resources.pool.open()


@app.on_shutdown
async def shutdown() -> None:
    if _Resources.pool is not None:
        await _Resources.pool.close()
        _Resources.pool = None


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
    ),
    # 동시 소비 (§11.4) — 1 이면 기존 순차, >1 이면 StreamConcurrentSubscriber 로
    # N 건을 동시에 처리한다. ACK 는 동시에서도 메시지별·핸들러 완료 후라 PEL 회수 설계 유지.
    max_workers=settings.stream_max_workers,
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
async def handle_reclaimed(msg: RedisStreamMessage, redis: InjectedRedis) -> None:
    """회수 구독자 — 배선만 하고 처리는 `reclaim_one` 이 한다.

    `redis` 는 FastStream 이 Context 로 넣어주는 커넥션이다. `redis.asyncio.Redis` 를
    그대로 힌트로 쓰면 주입되지 않고 검증 오류가 난다.
    """
    message_ids = msg.raw_message.get("message_ids") or []
    if not message_ids:  # 회수 경로는 항상 한 건씩 오지만 방어적으로
        return
    await reclaim_one(redis, message_ids[0], msg.raw_message["data"])


# ------------------------------------------------- 동기 디스패치 실험 (PRD §11)

SYNC_ANALYZE_PATH = "/internal/analyses/resume"  # Spring ResumeAnalysisSyncHttpRequester 와 일치


@post
async def handle_sync_analyze(request: Request) -> AsgiResponse:
    """실험용 동기 분석 진입점 — 2xx = EMBEDDED 종결까지 완료 (§11.1 계약).

    파싱·검증만 하고 처리는 `analyze_sync` 가 한다(구독자와 같은 얇은 배선 스타일).
    """
    try:
        payload = await request.json()
    except Exception as e:
        return JSONResponse({"error": f"잘못된 JSON: {e}"}, 400)
    try:
        parsed = ParseRequest.model_validate(payload)
    except ValidationError as e:
        return JSONResponse({"error": f"계약 위반 바디: {e}"}, 422)
    return await analyze_sync(parsed, broker._connection)


async def analyze_sync(request: ParseRequest, redis: Redis) -> AsgiResponse:
    """동기 처리 배선 — 풀에서 커넥션을 가져와 처리 전 구간에 쓴다 (§11.4).

    요청마다 새 커넥션을 열지 않으므로 동시 요청이 몰려도 PG 커넥션은
    `db_pool_max_size` 를 넘지 않는다. 커넥션을 전부 사용 중이면
    `db_pool_wait_timeout_s` 까지 기다리다가 503 — Spring 이 비-2xx 를 받아 FAILED 로 바꾼다.
    """
    if _Resources.db is not None:  # 테스트 주입 커넥션 — 풀 우회
        return await _analyze_with_conn(request, redis, _Resources.db)
    try:
        async with _Resources.pool.connection(
            timeout=settings.db_pool_wait_timeout_s
        ) as conn:
            return await _analyze_with_conn(request, redis, conn)
    except PoolTimeout:
        logger.warning("동기 분석 대기 초과 (resumeId=%s)", request.resumeId)
        return JSONResponse(
            {
                "resumeId": request.resumeId,
                "error": f"동시 처리 상한 대기 초과({settings.db_pool_wait_timeout_s}s)",
            },
            503,
        )


async def _analyze_with_conn(
    request: ParseRequest, redis: Redis, conn: AsyncConnection
) -> AsgiResponse:
    """스트림 경로와 같은 `_process` 를 태우고 종단 상태로 응답을 가른다.

    상태 이벤트도 동일하게 발행되어 두 경로의 작업량이 같다(§11.2 공정 비교 조건).
    프레임워크(HttpHandler)의 포괄 500 은 에러 내용을 숨기므로 여기서 직접 잡아 노출한다
    — 비-2xx 시 FAILED 전이는 호출자(Spring) 책임이라 실패 원인이 보여야 한다.
    """
    try:
        await _process(request, delivery_count=1, redis=redis, conn=conn)
    except Exception as e:
        logger.exception("동기 분석 실패 (resumeId=%s)", request.resumeId)
        return JSONResponse(
            {"resumeId": request.resumeId, "error": f"{type(e).__name__}: {e}"}, 500
        )

    # process_request 는 유령·REINDEX 계약 위반에서도 예외 없이 반환하므로 상태 재조회로 판정
    status = await get_parse_status(conn, request.resumeId)
    error = await get_error_message(conn, request.resumeId) if status == AnalysisStatus.FAILED else None

    body = {"resumeId": request.resumeId, "status": status}
    if status == AnalysisStatus.EMBEDDED:  # 이미 EMBEDDED 였던 중복 호출도 200 — 멱등(§2.4)
        return JSONResponse(body, 200)
    if status is None:
        return JSONResponse({**body, "error": "상태 레코드 없음"}, 404)
    if status == AnalysisStatus.FAILED:
        return JSONResponse({**body, "error": error or "분석 실패"}, 500)
    return JSONResponse({**body, "error": "비종결 상태로 반환됨"}, 409)  # 다른 처리자에 양보 등


app.mount(SYNC_ANALYZE_PATH, handle_sync_analyze)
