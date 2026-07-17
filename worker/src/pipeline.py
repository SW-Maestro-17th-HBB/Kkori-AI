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

from typing import Awaitable, Callable

from psycopg import AsyncConnection

from src.ai import Embedder, Structurer
from src.chunking import chunk_structured_data
from src.config import Settings
from src.contract import AnalysisMode, AnalysisStatus, ParseRequest
from src.db import (
    get_parse_status,
    load_structured_data,
    mark_failed,
    replace_chunks,
    save_structured_data,
    try_transition,
)
from src.extraction import is_empty_text

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


async def process_request(
    request: ParseRequest,
    *,
    conn: AsyncConnection,
    embedder: Embedder,
    structurer: Structurer,
    fetch_text: FetchText,
    publish: PublishStatus,
    settings: Settings,
) -> None:
    """요청 1건 처리. 정상 반환 = ACK(종결/스킵/양보), 예외 = PEL 잔류(재시도 대상)."""
    status = await get_parse_status(conn, request.resumeId)

    # 레코드 없음 = 발행 후 커밋 실패한 유령 메시지 → 스킵 (백엔드 걸어둔 방어)
    if status is None:
        return

    # 종결 상태 = at-least-once 중복 → 스킵, 이벤트 재발행 없음 (§3.1)
    if status in (AnalysisStatus.EMBEDDED.value, AnalysisStatus.FAILED.value):
        return

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
        if await mark_failed(conn, request.resumeId, msg):
            await publish(request.resumeId, request.userId, AnalysisStatus.FAILED, msg)
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

    # 1단계: PARSING 진입 (started_at 기록은 db 계층이 담당)
    if not await try_transition(conn, rid, entry, AnalysisStatus.PARSING):
        return
    await publish(rid, uid, AnalysisStatus.PARSING, "")

    # 2단계: 텍스트 추출 (원문은 저장하지 않는다 — 변수로만 흐름)
    if not await try_transition(conn, rid, AnalysisStatus.PARSING, AnalysisStatus.TEXT_EXTRACTING):
        return
    await publish(rid, uid, AnalysisStatus.TEXT_EXTRACTING, "")
    text = await fetch_text(request.bucket, request.objectKey)
    if is_empty_text(text):
        # 이미지-only(스캔) PDF — "0청크 EMBEDDED"와 구분해 명확히 실패 처리 (§2.1)
        msg = "텍스트 추출 실패(이미지-only PDF 가능성, OCR 미지원)"
        if await mark_failed(conn, rid, msg):
            await publish(rid, uid, AnalysisStatus.FAILED, msg)
        return

    # 3단계: LLM 구조화 → 산출물 저장과 PARSED 전이를 한 트랜잭션으로 (§2.4 불변식 1)
    if not await try_transition(conn, rid, AnalysisStatus.TEXT_EXTRACTING, AnalysisStatus.STRUCTURING):
        return
    await publish(rid, uid, AnalysisStatus.STRUCTURING, "")
    data = structurer.structure(text)
    try:
        async with conn.transaction():
            await save_structured_data(conn, rid, data)
            if not await try_transition(conn, rid, AnalysisStatus.STRUCTURING, AnalysisStatus.PARSED):
                raise _Yield()
    except _Yield:
        return
    await publish(rid, uid, AnalysisStatus.PARSED, "")

    # 4~5단계: 임베딩 (PARSED 에서 합류 — REINDEX 와 같은 경로)
    await _run_embedding_stage(
        request, entry_status=AnalysisStatus.PARSED.value, conn=conn,
        embedder=embedder, publish=publish, settings=settings,
    )


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

    # PARSED 에서 왔으면 EMBEDDING 으로 CAS 진입 (0행 = 다른 처리자 → 양보)
    if entry_status == AnalysisStatus.PARSED.value:
        if not await try_transition(
            conn, rid, AnalysisStatus.PARSED, AnalysisStatus.EMBEDDING
        ):
            return
    # 단계 진입 이벤트 (§1.3). REINDEX 는 DB 가 이미 EMBEDDING(Spring 세팅)이라 전이 없이 발행만.
    await publish(rid, uid, AnalysisStatus.EMBEDDING, "")

    data = await load_structured_data(conn, rid)
    if data is None:
        msg = "계약 위반: EMBEDDING 단계인데 structured_data 없음"
        if await mark_failed(conn, rid, msg):
            await publish(rid, uid, AnalysisStatus.FAILED, msg)
        return

    chunks = chunk_structured_data(
        data,
        target_tokens=settings.chunk_target_tokens,
        overlap_sentences=settings.chunk_overlap_sentences,
        chunk_version=settings.chunk_version,
    )
    embeddings = embedder.embed_documents([c.content for c in chunks])

    # 산출물 저장 + 상태 전이를 한 트랜잭션으로 (§2.4 불변식 1, §3.3)
    try:
        async with conn.transaction():
            await replace_chunks(conn, rid, chunks, embeddings)
            if not await try_transition(
                conn, rid, AnalysisStatus.EMBEDDING, AnalysisStatus.EMBEDDED
            ):
                raise _Yield()  # 다른 처리자가 먼저 완료 → 롤백(승자의 청크 보존) 후 양보
    except _Yield:
        return

    await publish(rid, uid, AnalysisStatus.EMBEDDED, "")
