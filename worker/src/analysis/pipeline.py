"""분석 파이프라인 — 진입 라우팅(§3.1)과 단계 실행.

진입은 **DB 상태가 결정**하고(§2.3), mode 는 cross-check 로만 쓴다.
모든 의존(커넥션·임베딩기·추출·구조화·상태발행)은 인자로 받아 테스트에서 갈아끼울 수 있다.

라우팅(§3.1 재개 표):
- EMBEDDED / FAILED / 레코드 없음(유령) → 스킵 (이벤트 재발행 없음)
- EMBEDDING / PARSED → 임베딩 단계 (REINDEX 진입·재개)
- 이른 상태(UPLOADED~STRUCTURING) → FULL 처음부터 (원문 미저장이므로 추출부터).
  단 mode=REINDEX 가 이 상태로 오면 계약 위반 → FAILED (§2.3)
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar

from psycopg import AsyncConnection

from src.ai import Embedder, Structurer
from src.analysis.chunking import chunk_structured_data
from src.analysis.extraction import is_empty_text
from src.config import Settings
from src.contract import AnalysisMode, AnalysisStatus, ParseRequest
from src.storage.repository import (
    get_parse_status,
    increment_retry_count,
    load_structured_data,
    mark_failed,
    replace_chunks,
    reset_retry_count,
    save_structured_data,
    try_transition,
)

T = TypeVar("T")

# 상태 발행 콜백: (resume_id, user_id, status, message)
PublishStatus = Callable[[int, int, AnalysisStatus, str], Awaitable[None]]
# 텍스트 획득 콜백: (bucket, objectKey) → 추출된 텍스트 (S3 다운로드 + PyMuPDF 를 감싼다)
FetchText = Callable[[str, str], Awaitable[str]]

_EARLY_STATES = {
    AnalysisStatus.UPLOADED.value,
    AnalysisStatus.PARSING.value,
    AnalysisStatus.TEXT_EXTRACTING.value,
    AnalysisStatus.STRUCTURING.value,
}


class _Yield(Exception):
    """다른 처리자가 앞서감 — 트랜잭션 롤백 후 조용히 물러나기 위한 내부 신호."""


async def _enter_stage(
    conn: AsyncConnection,
    rid: int,
    uid: int,
    from_status: AnalysisStatus,
    to_status: AnalysisStatus,
    publish: PublishStatus,
) -> bool:
    """단계 진입 = CAS 전이 + 진입 이벤트 발행 (§1.3, §3.3). False = 다른 처리자에 양보."""
    if not await try_transition(conn, rid, from_status, to_status):
        return False
    await publish(rid, uid, to_status, "")
    return True


async def _fail(
    conn: AsyncConnection, rid: int, uid: int, publish: PublishStatus, msg: str
) -> None:
    """FAILED 종결 — 실제로 기록됐을 때만 이벤트 발행 (완료·기존 실패를 덮지 않음, §4)."""
    if await mark_failed(conn, rid, msg):
        await publish(rid, uid, AnalysisStatus.FAILED, msg)


async def _with_retry(
    thunk: Callable[[], Awaitable[T]],
    *,
    conn: AsyncConnection,
    resume_id: int,
    settings: Settings,
) -> T:
    """외부 호출(S3·LLM·임베딩)의 일시 오류 내부 재시도 (§9).

    실패할 때마다 retry_count 를 DB 에 즉시 +1 하고(§6 — 크래시 생존성·관측성),
    지수 백오프(1s→2s→4s) 후 재시도한다. 최대 시도를 소진하면 마지막 예외를 전파한다
    → 핸들러/회수 경로에서 ACK 없이 끝나 PEL 재전달로 이어진다.
    """
    for attempt in range(settings.retry_max_attempts):
        try:
            return await thunk()
        except Exception:
            await increment_retry_count(conn, resume_id)
            if attempt + 1 >= settings.retry_max_attempts:
                raise
            await asyncio.sleep(settings.retry_base_delay_s * (2**attempt))
    raise AssertionError("unreachable")  # for 문은 반드시 return 또는 raise 로 끝난다


async def process_request(
    request: ParseRequest,
    *,
    conn: AsyncConnection,
    embedder: Embedder,
    structurer: Structurer,
    fetch_text: FetchText,
    publish: PublishStatus,
    settings: Settings,
    delivery_count: int = 1,
    is_reclaimed: bool = False,
) -> None:
    """요청 1건 처리. 정상 반환 = ACK(종결/스킵/양보), 예외 = PEL 잔류(재시도 대상).

    is_reclaimed: 회수(XAUTOCLAIM) 경로 여부. 신규 메시지는 새 런이라 retry_count 를 0으로
    리셋하지만, 회수 재개는 같은 런의 연장이라 리셋하지 않는다 (§3.2, §6).
    """

    # 포기 규칙 (§4): 처리 시작 전에 재전달 횟수 확인. 임계 이상이면 재처리 없이
    # ① FAILED 기록 → ② 정상 반환(=XACK). 이 순서 고정 — 중간에 죽어도 재전달본이
    # 같은 경로로 수렴한다(mark_failed 는 멱등, 종결 상태는 덮지 않음).
    if delivery_count >= settings.delivery_count_threshold:
        msg = f"재전달 임계 초과(delivery count={delivery_count})"
        await _fail(conn, request.resumeId, request.userId, publish, msg)
        return

    status = await get_parse_status(conn, request.resumeId)

    # 레코드 없음 = 발행 후 커밋 실패한 유령 메시지 → 스킵 (백엔드 걸어둔 방어)
    if status is None:
        return

    # 종결 상태 = at-least-once 중복 → 스킵, 이벤트 재발행 없음 (§3.1)
    if status in (AnalysisStatus.EMBEDDED.value, AnalysisStatus.FAILED.value):
        return

    # 신규 메시지 = 새 런 시작 → retry_count 0 리셋. 회수 재개는 같은 런이라 유지 (§3.2, §6).
    if not is_reclaimed:
        await reset_retry_count(conn, request.resumeId)

    # EMBEDDING(REINDEX 진입·재개) / PARSED(구조화 완료) → 임베딩 단계
    if status in (AnalysisStatus.EMBEDDING.value, AnalysisStatus.PARSED.value):
        await _run_embedding_stage(
            request, entry_status=status, conn=conn,
            embedder=embedder, publish=publish, settings=settings,
        )
        return

    # 이른 상태인데 REINDEX = 계약 위반 (structured_data 가 있어야 할 모드, §2.3) → FAILED
    if request.mode is AnalysisMode.REINDEX:
        msg = f"계약 위반: mode=REINDEX 인데 상태가 {status} (structured_data 이전 단계)"
        await _fail(conn, request.resumeId, request.userId, publish, msg)
        return

    # FULL — 처음부터 (§2.1). 크래시 재개(PARSING~STRUCTURING)도 원문 미저장이라 처음부터(§3.1).
    await _run_full_pipeline(
        request, entry_status=status, conn=conn, embedder=embedder,
        structurer=structurer, fetch_text=fetch_text, publish=publish, settings=settings,
    )


async def _run_full_pipeline(
    request: ParseRequest,
    *,
    entry_status: str,
    conn: AsyncConnection,
    embedder: Embedder,
    structurer: Structurer,
    fetch_text: FetchText,
    publish: PublishStatus,
    settings: Settings,
) -> None:
    """FULL: 추출 → 구조화 → PARSED → 임베딩 단계 합류 (§2.1 단계표).

    각 단계 진입은 CAS — 0행이면 다른 처리자가 앞서간 것이므로 양보(§3.3).
    추출·구조화의 예외(다운로드 실패·손상 PDF·LLM 오류)는 전파 → PEL 잔류 → 재시도.
    """
    rid, uid = request.resumeId, request.userId
    entry = AnalysisStatus(entry_status)

    # 1~2단계: PARSING 진입 → 텍스트 추출 (양보 또는 빈 추출 FAILED 시 None)
    if not await _enter_stage(conn, rid, uid, entry, AnalysisStatus.PARSING, publish):
        return
    text = await _extract_stage(request, conn=conn, fetch_text=fetch_text,
                                publish=publish, settings=settings)
    if text is None:
        return

    # 3단계: LLM 구조화 + 저장 (양보 시 False)
    if not await _structure_stage(request, text, conn=conn, structurer=structurer,
                                  publish=publish, settings=settings):
        return

    # 4~5단계: 임베딩 (PARSED 에서 합류 — REINDEX 와 같은 경로)
    await _run_embedding_stage(
        request, entry_status=AnalysisStatus.PARSED.value, conn=conn,
        embedder=embedder, publish=publish, settings=settings,
    )


async def _extract_stage(
    request: ParseRequest,
    *,
    conn: AsyncConnection,
    fetch_text: FetchText,
    publish: PublishStatus,
    settings: Settings,
) -> str | None:
    """2단계: 텍스트 추출. 원문은 저장하지 않는다 — 변수로만 흐른다 (§2.1).

    반환 None = 여기서 종결됨(양보 또는 빈 추출 FAILED). 예외는 전파(재시도 대상).
    """
    rid, uid = request.resumeId, request.userId
    if not await _enter_stage(
        conn, rid, uid, AnalysisStatus.PARSING, AnalysisStatus.TEXT_EXTRACTING, publish
    ):
        return None
    text = await _with_retry(
        lambda: fetch_text(request.bucket, request.objectKey),
        conn=conn, resume_id=rid, settings=settings,
    )
    if is_empty_text(text):
        # 이미지-only(스캔) PDF — "0청크 EMBEDDED"와 구분해 명확히 실패 처리 (§2.1)
        await _fail(conn, rid, uid, publish,
                    "텍스트 추출 실패(이미지-only PDF 가능성, OCR 미지원)")
        return None
    return text


async def _structure_stage(
    request: ParseRequest,
    text: str,
    *,
    conn: AsyncConnection,
    structurer: Structurer,
    publish: PublishStatus,
    settings: Settings,
) -> bool:
    """3단계: LLM 구조화 → 산출물 저장 + PARSED 전이를 한 트랜잭션으로 (§2.4 불변식 1).

    반환 False = 다른 처리자에 양보. 예외는 전파(재시도 대상).
    """
    rid, uid = request.resumeId, request.userId
    if not await _enter_stage(
        conn, rid, uid, AnalysisStatus.TEXT_EXTRACTING, AnalysisStatus.STRUCTURING, publish
    ):
        return False

    async def _structure():
        return structurer.structure(text)

    data = await _with_retry(_structure, conn=conn, resume_id=rid, settings=settings)
    try:
        async with conn.transaction():
            await save_structured_data(conn, rid, data)
            if not await try_transition(
                conn, rid, AnalysisStatus.STRUCTURING, AnalysisStatus.PARSED
            ):
                raise _Yield()
    except _Yield:
        return False
    await publish(rid, uid, AnalysisStatus.PARSED, "")
    return True


async def _run_embedding_stage(
    request: ParseRequest,
    *,
    entry_status: str,
    conn: AsyncConnection,
    embedder: Embedder,
    publish: PublishStatus,
    settings: Settings,
) -> None:
    """청킹 → 임베딩 → 청크 교체 + EMBEDDED 전이(같은 트랜잭션, §2.4) → 이벤트 발행."""
    rid, uid = request.resumeId, request.userId

    # PARSED 에서 왔으면 EMBEDDING 으로 CAS 진입. REINDEX 는 DB 가 이미 EMBEDDING(Spring 세팅)
    # 이라 전이 없이 진입 이벤트만 발행한다 (§1.3).
    if entry_status == AnalysisStatus.PARSED.value:
        if not await _enter_stage(
            conn, rid, uid, AnalysisStatus.PARSED, AnalysisStatus.EMBEDDING, publish
        ):
            return
    else:
        await publish(rid, uid, AnalysisStatus.EMBEDDING, "")

    data = await load_structured_data(conn, rid)
    if data is None:
        await _fail(conn, rid, uid, publish, "계약 위반: EMBEDDING 단계인데 structured_data 없음")
        return

    chunks = chunk_structured_data(
        data,
        target_tokens=settings.chunk_target_tokens,
        overlap_sentences=settings.chunk_overlap_sentences,
        chunk_version=settings.chunk_version,
    )

    async def _embed():
        return embedder.embed_documents([c.content for c in chunks])

    embeddings = await _with_retry(_embed, conn=conn, resume_id=rid, settings=settings)

    if not await _commit_chunks(conn, rid, chunks, embeddings):
        return
    await publish(rid, uid, AnalysisStatus.EMBEDDED, "")


async def _commit_chunks(conn: AsyncConnection, rid: int, chunks, embeddings) -> bool:
    """청크 교체 + EMBEDDED 전이를 한 트랜잭션으로 (§2.4 불변식 1, §3.3).

    반환 False = 다른 처리자가 먼저 완료 → 롤백(승자의 청크 보존) 후 양보.
    """
    try:
        async with conn.transaction():
            await replace_chunks(conn, rid, chunks, embeddings)
            if not await try_transition(
                conn, rid, AnalysisStatus.EMBEDDING, AnalysisStatus.EMBEDDED
            ):
                raise _Yield()
    except _Yield:
        return False
    return True
