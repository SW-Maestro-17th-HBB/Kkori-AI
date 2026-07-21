"""방치 메시지 회수 (§3, XAUTOCLAIM).

의존(레디스·설정·처리 함수)은 전부 인자로 받는다 — 배선은 main 이 담당하고,
이 모듈은 회수 절차 자체만 안다.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from redis.asyncio import Redis

from src.config import Settings
from src.contract import ParseRequest
from src.messaging.streams import decode_fields, get_delivery_count

logger = logging.getLogger(__name__)

# 회수된 요청 처리 콜백: (request, delivery_count) — 구현은 main 이 배선
ProcessReclaimed = Callable[[ParseRequest, int], Awaitable[None]]


async def reclaim_pending_once(
    redis: Redis, *, settings: Settings, process: ProcessReclaimed
) -> int:
    """ACK 없이 min_idle 이상 방치된 메시지를 회수해 재처리한다. 반환 = 처리 시도 건수.

    - 회수본도 포기 규칙(§4) 적용 — XAUTOCLAIM 이 delivery count 를 +1 시키므로,
      계속 실패하는 메시지는 회수 경로에서 임계에 도달해 FAILED 로 종결된다(무한 회수 차단).
    - 정상 반환(완료·스킵·양보·포기) 후 **직접 XACK** — 자동 ACK 은 구독 핸들러 전용이다.
      예외 시 ACK 하지 않아 다음 주기에 다시 회수된다.
    """
    _cursor, messages, _deleted = await redis.xautoclaim(
        name=ParseRequest.STREAM_KEY,
        groupname=settings.consumer_group,
        consumername=settings.resolved_consumer_name,
        min_idle_time=settings.claim_min_idle_ms,
        count=settings.reclaim_batch_size,
    )

    processed = 0
    for message_id, fields in messages:
        # 형식 검증(decode)은 실패 시 예외를 던진다 — try 안에서 처리해야
        # 깨진 메시지 하나가 배치 전체를 중단시키지 않는다 (리뷰 반영).
        try:
            request = ParseRequest.decode(decode_fields(fields))
        except Exception:
            # 형식이 틀린 메시지는 재처리도, FAILED 기록도 불가능하다(resumeId 를 못 읽음).
            # ACK 하지 않으면 매 주기 재회수가 반복되므로, 로그를 남기고 제거한다.
            logger.exception("형식 위반 메시지 제거 (id=%s, fields=%r)", message_id, fields)
            await redis.xack(ParseRequest.STREAM_KEY, settings.consumer_group, message_id)
            continue
        try:
            delivery_count = await get_delivery_count(
                redis, settings.consumer_group, message_id
            )
            await process(request, delivery_count)
        except Exception:
            logger.exception(
                "회수 재처리 실패 — 다음 주기에 재시도 (resumeId=%s)", request.resumeId
            )
            continue  # ACK 안 함 → PEL 잔류 → 다음 회수 대상
        await redis.xack(ParseRequest.STREAM_KEY, settings.consumer_group, message_id)
        processed += 1
    return processed


async def reclaim_loop(
    get_redis: Callable[[], Redis], *, settings: Settings, process: ProcessReclaimed
) -> None:
    """주기적으로 회수 시도 (§9: 주기 5분 — 최악 복구 지연 = min_idle + 주기 ≤ 10분).

    루프 자체는 어떤 예외에도 죽지 않는다 — 죽으면 크래시 복구 기능이 사라지므로.
    """
    while True:
        await asyncio.sleep(settings.reclaim_interval_s)
        try:
            reclaimed = await reclaim_pending_once(
                get_redis(), settings=settings, process=process
            )
            if reclaimed:
                logger.info("방치 메시지 %d건 회수 처리", reclaimed)
        except Exception:
            logger.exception("회수 루프 오류 — 다음 주기에 재시도")
