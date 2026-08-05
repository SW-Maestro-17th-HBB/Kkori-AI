"""PEL(Pending Entries List) 조회 — 도메인 공통.

이력서·리포트 두 워커가 같은 회수 규칙을 쓰므로 스트림 키만 인자로 받아 공유한다.
계약(ParseRequest·ReportGenerationRequested)에 의존하지 않아 어느 도메인에서든 임포트 가능하다.
"""

from __future__ import annotations

from redis.asyncio import Redis

__all__ = ["get_delivery_count"]


async def get_delivery_count(redis: Redis, stream: str, group: str, message_id: str) -> int:
    """이 메시지가 몇 번째 전달인지 (Redis PEL 의 times_delivered).

    포기 규칙(§4)의 판단 근거. PEL 에서 못 찾으면(이미 ACK 등) 1로 간주한다.
    XAUTOCLAIM 은 가져오는 순간 이 값을 +1 시키므로, 회수 직후 조회값이 "이번이 몇 번째"다.
    """
    entries = await redis.xpending_range(
        stream, group, min=message_id, max=message_id, count=1
    )
    return entries[0]["times_delivered"] if entries else 1
