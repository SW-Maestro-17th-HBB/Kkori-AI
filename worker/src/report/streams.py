"""리포트 스트림 입출력 — 상태 발행·전달 횟수 조회.

이력서 messaging/streams.py 와 같은 원리다: Spring 은 네이티브 스트림 필드만 읽으므로
FastStream 기본 발행(__data__ 봉투) 대신 redis 커넥션으로 직접 XADD 한다.
그 모듈은 이력서 계약(ParseRequest)에 묶여 있어 리포트 몫을 따로 둔다.
"""

from __future__ import annotations

from redis.asyncio import Redis

from src.contract import ReportGenerationRequested, ReportStatus, ReportStatusChanged


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


async def get_delivery_count(redis: Redis, group: str, message_id: str) -> int:
    """생성 요청 메시지가 몇 번째 전달인지 (Redis PEL 의 times_delivered).

    포기 규칙의 판단 근거. PEL 에서 못 찾으면(이미 ACK 등) 1로 간주한다.
    """
    entries = await redis.xpending_range(
        ReportGenerationRequested.STREAM_KEY, group,
        min=message_id, max=message_id, count=1,
    )
    return entries[0]["times_delivered"] if entries else 1
