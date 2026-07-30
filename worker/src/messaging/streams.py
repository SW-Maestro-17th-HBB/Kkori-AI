"""스트림 입출력 도구 — 상태 발행(네이티브 필드)·전달 횟수 조회·필드 변환.

## Redis Stream 상호운용 (실 Redis 로 확인, 2026-07-17)
- Spring 은 각 필드를 **네이티브 스트림 필드**로 XADD 한다(`mapBacked`).
- FastStream 기본 발행은 `__data__` 바이너리 봉투로 감싸 Spring 이 못 읽는다
  → 상태 발행은 redis 커넥션으로 네이티브 필드를 직접 XADD 한다.
"""

from __future__ import annotations

from redis.asyncio import Redis

from src.contract import AnalysisStatus, ParseRequest, StatusChanged
from src.contract.fields import decode_fields  # 공용 계층으로 이동 — 재노출 (기존 호출부 호환)

__all__ = ["decode_fields", "get_delivery_count", "publish_status"]


async def publish_status(
    redis: Redis,
    resume_id: int,
    user_id: int,
    status: AnalysisStatus,
    message: str = "",
) -> None:
    """상태 이벤트를 Spring 이 읽는 네이티브 필드 형식으로 발행한다."""
    payload = StatusChanged(
        resumeId=resume_id, userId=user_id, status=status, message=message
    ).encode()
    await redis.xadd(StatusChanged.STREAM_KEY, payload)


async def get_delivery_count(redis: Redis, group: str, message_id: str) -> int:
    """이 메시지가 몇 번째 전달인지 (Redis PEL 의 times_delivered).

    포기 규칙(§4)의 판단 근거. PEL 에서 못 찾으면(이미 ACK 등) 1로 간주한다.
    """
    entries = await redis.xpending_range(
        ParseRequest.STREAM_KEY, group, min=message_id, max=message_id, count=1
    )
    return entries[0]["times_delivered"] if entries else 1
