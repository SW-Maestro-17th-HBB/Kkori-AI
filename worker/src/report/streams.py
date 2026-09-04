"""리포트 상태 발행 — 네이티브 필드 형식.

이력서 messaging/streams.py 와 같은 원리다: Spring 은 네이티브 스트림 필드만 읽으므로
FastStream 기본 발행(__data__ 봉투) 대신 redis 커넥션으로 직접 XADD 한다.
발행 계약(ReportStatusChanged)이 도메인 소유라 모듈을 따로 둔다.

전달 횟수 조회는 도메인 공통이라 `messaging.pel` 을 쓴다.
"""

from __future__ import annotations

from redis.asyncio import Redis

from src.contract import ReportStatus, ReportStatusChanged


async def publish_status(
    redis: Redis,
    report_id: int,
    user_id: int,
    status: ReportStatus,
    message: str = "",
) -> None:
    """상태 이벤트를 Spring 이 읽는 네이티브 필드 형식으로 발행한다 (SSE 중계용)."""
    payload = ReportStatusChanged(
        reportId=report_id, userId=user_id, status=status, message=message
    ).encode()
    await redis.xadd(ReportStatusChanged.STREAM_KEY, payload)
