"""리포트 생성 요청 발행 — flush 성공 후 Redis Stream XADD. docs/prd/interview-end.md §5.

메시지는 포인터다 — 전사 본문을 넣지 않고, worker가 sessionId로 DB의 transcript를
읽는다. 재시도로 같은 세션의 메시지가 중복 발행될 수 있으므로 소비 측(worker)이
sessionId 기준으로 멱등 처리한다는 것이 계약이다. 발행이 끝내 실패해도 transcript는
DB에 있다 — "transcript 행 존재 & 리포트 없음"이 미발행 검출식이고, 감지·재발행은
리포트 생성 스토리(worker)의 완료 조건으로 이관돼 있다. sessionId는 개인정보가
아니므로 로그 기록 가능하다.
"""

from __future__ import annotations

import logging
import os
from contextlib import suppress
from datetime import datetime, timezone

from redis.asyncio import Redis

from src.config import REPORT_REQUEST_STREAM_KEY
from src.interview.redis_sink import REDIS_URL_ENV

logger = logging.getLogger(__name__)

_OP_TIMEOUT_SECONDS = 2.0
_PUBLISH_ATTEMPTS = 2  # 실패 시 1회 재시도 (PRD §5)


async def publish_report_request(session_id: str) -> bool:
    """리포트 생성 요청을 발행한다. True = 발행 완료."""
    url = os.getenv(REDIS_URL_ENV)  # transcript 사본과 같은 구성 공유 (PRD §5)
    if not url:
        logger.warning("%s 미설정 — 리포트 요청 발행 생략", REDIS_URL_ENV)
        return False
    fields = {
        "sessionId": session_id,
        "requestedAt": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    }
    for attempt in range(1, _PUBLISH_ATTEMPTS + 1):
        redis: Redis | None = None
        try:
            # 클라이언트 생성도 실패 경로다 — 잘못된 URL(ValueError)이 재시도와
            # sessionId 식별 로그를 우회해 전파되지 않게 try 안에서 만든다
            redis = Redis.from_url(
                url,
                socket_timeout=_OP_TIMEOUT_SECONDS,
                socket_connect_timeout=_OP_TIMEOUT_SECONDS,
            )
            await redis.xadd(REPORT_REQUEST_STREAM_KEY, fields)
            logger.info("리포트 생성 요청 발행 완료 — sessionId=%s", session_id)
            return True
        except Exception as exc:
            logger.warning(
                "리포트 요청 발행 실패(%s) — 시도 %d/%d",
                type(exc).__name__,
                attempt,
                _PUBLISH_ATTEMPTS,
            )
        finally:
            if redis is not None:
                with suppress(Exception):
                    await redis.aclose()
    # 식별 가능한 오류 로그 — 미발행 세션의 수동·배치 복구 재료 (PRD §5)
    logger.error(
        "리포트 요청 발행 소진 — sessionId=%s (검출식: transcript 행 존재 & 리포트 없음)",
        session_id,
    )
    return False
