"""리포트 상태 발행 — Pub/Sub 채널에 JSON 으로 PUBLISH.

이력서 messaging/publish.py 와 같은 원리다: 상태 알림은 Spring 전 인스턴스가 받아야
하므로 스트림이 아니라 Pub/Sub 으로 보내고(HBB1-332), FastStream 기본 발행(__data__ 봉투)
대신 redis 커넥션으로 직접 PUBLISH 한다. 발행 계약(ReportStatusChanged)이 도메인 소유라
모듈을 따로 둔다.

전달 횟수 조회는 도메인 공통이라 `messaging.pel` 을 쓴다.
"""

from __future__ import annotations

import json

from redis.asyncio import Redis

from src.contract import ReportStatus, ReportStatusChanged


async def publish_status(
    redis: Redis,
    report_id: int,
    user_id: int,
    status: ReportStatus,
    message: str = "",
) -> None:
    """상태 이벤트를 Pub/Sub 채널에 JSON 으로 발행한다 (Spring 전 인스턴스가 구독, SSE 중계용)."""
    payload = ReportStatusChanged(
        reportId=report_id, userId=user_id, status=status, message=message
    ).encode()
    await redis.publish(ReportStatusChanged.CHANNEL, json.dumps(payload, ensure_ascii=False))
