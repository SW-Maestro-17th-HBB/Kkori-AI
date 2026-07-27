"""transcript DB 저장 — 세션당 1행 jsonb flush. docs/prd/interview-end.md §4.

flush 원본은 메모리 완전본(대화 로그)이다 — 정상 종료 경로에서만 실행되므로
메모리가 항상 완전하고, Redis 사본(best-effort)은 원본 자격이 없다.
`session_id` UNIQUE + ON CONFLICT DO NOTHING으로 중복 flush는 no-op이다(멱등).
실패 시 1회 재시도하고, 소진되면 False — 호출자(EndSequence)가 Redis 사본을
보존한 채 종료 시퀀스를 계속한다. 대화 내용은 운영 로그에 남기지 않는다.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

DATABASE_URL_ENV = "KKORI_AGENT_DATABASE_URL"

_CONNECT_TIMEOUT_SECONDS = 5
_FLUSH_ATTEMPTS = 2  # 실패 시 1회 재시도 (PRD §4)

_INSERT_SQL = (
    "INSERT INTO interview_transcript (session_id, content) VALUES (%s, %s) "
    "ON CONFLICT (session_id) DO NOTHING"
)


async def flush_transcript(session_id: str, utterances: Sequence[dict]) -> bool:
    """대화 로그(발화 객체 배열)를 1행 INSERT한다. True = flush 완료(중복 no-op 포함)."""
    url = os.getenv(DATABASE_URL_ENV)
    if not url:
        logger.warning("%s 미설정 — transcript flush 생략", DATABASE_URL_ENV)
        return False
    try:
        numeric_session_id = int(session_id)
    except ValueError:
        # 세션 계약상 sessionId는 숫자(interview_session.id) — 아니면 저장 대상이 아니다
        logger.error("sessionId가 숫자가 아님 — transcript flush 불가")
        return False

    content = Jsonb(list(utterances))
    for attempt in range(1, _FLUSH_ATTEMPTS + 1):
        try:
            async with await psycopg.AsyncConnection.connect(
                url, connect_timeout=_CONNECT_TIMEOUT_SECONDS
            ) as conn:
                await conn.execute(_INSERT_SQL, (numeric_session_id, content))
            logger.info("transcript flush 완료 — %d개 발화", len(utterances))
            return True
        except Exception as exc:
            logger.warning(
                "transcript flush 실패(%s) — 시도 %d/%d",
                type(exc).__name__,
                attempt,
                _FLUSH_ATTEMPTS,
            )
    logger.error("transcript flush 소진 — Redis 사본 보존(복구 재료)")
    return False
