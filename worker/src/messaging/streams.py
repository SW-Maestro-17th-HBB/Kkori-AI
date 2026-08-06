"""이력서 상태 발행 — 네이티브 필드 형식.

## Redis Stream 상호운용 (실 Redis 로 확인, 2026-07-17)
- Spring 은 각 필드를 **네이티브 스트림 필드**로 XADD 한다(`mapBacked`).
- FastStream 기본 발행은 `__data__` 바이너리 봉투로 감싸 Spring 이 못 읽는다
  → 상태 발행은 redis 커넥션으로 네이티브 필드를 직접 XADD 한다.

전달 횟수 조회는 도메인 공통이라 `messaging.pel` 로 옮겼다.
"""

from __future__ import annotations

from redis.asyncio import Redis

from src.contract import AnalysisStatus, StatusChanged
from src.contract.fields import decode_fields  # 공용 계층으로 이동 — 재노출 (기존 호출부 호환)

__all__ = ["decode_fields", "publish_status"]


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
