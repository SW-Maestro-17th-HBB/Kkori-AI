"""분석 파이프라인 — 진입 라우팅(§3.1)과 단계 실행.

진입은 **DB 상태가 결정**하고(§2.3), mode 는 cross-check 로만 쓴다.
모든 의존(커넥션·임베딩기·상태발행)은 인자로 받아 테스트에서 갈아끼울 수 있게 한다.

현재 구현 범위:
- EMBEDDED / FAILED / 레코드 없음(유령) → 스킵 (이벤트 재발행 없음, §3.1)
- EMBEDDING / PARSED → 임베딩 단계 (REINDEX 와 재개가 여기로) — **완성**
- 이른 상태(UPLOADED~STRUCTURING) → FULL 처음부터 — 추출·구조화 미구현(TODO), 단
  mode=REINDEX 가 이 상태로 오면 계약 위반 → FAILED (§2.3)
"""

from __future__ import annotations

from typing import Awaitable, Callable, Protocol

from psycopg import AsyncConnection

from src.ai import Embedder
from src.chunking import chunk_structured_data
from src.config import Settings
from src.contract import AnalysisMode, AnalysisStatus, ParseRequest
from src.db import (
    get_parse_status,
    load_structured_data,
    mark_failed,
    replace_chunks,
    try_transition,
)

# 상태 발행 콜백: (resume_id, user_id, status, message)
PublishStatus = Callable[[int, int, AnalysisStatus, str], Awaitable[None]]

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

    # FULL 처음부터 — 텍스트 추출·구조화 단계에서 구현 (§2.1)
    raise NotImplementedError("FULL 파이프라인(추출→구조화)은 다음 단계에서 구현")


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
