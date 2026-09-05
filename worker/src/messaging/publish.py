"""이력서 상태 발행 — Pub/Sub 채널에 JSON 으로 PUBLISH.

## 왜 스트림이 아니라 Pub/Sub 인가 (HBB1-332)
- Spring 이 상태 스트림을 Consumer Group 하나로 읽으면 메시지가 인스턴스별로 나뉘고,
  SSE 연결이 없는 인스턴스가 받은 몫은 버려져 알림이 유실됐다.
- Pub/Sub 은 구독 중인 Spring 전 인스턴스에 같은 메시지가 가므로 SSE 연결이 있는 쪽이 받는다.
- 페이로드는 `StatusChanged.encode()` 의 문자열맵을 그대로 JSON 으로 직렬화한다(값 전부 문자열).
- FastStream 기본 발행은 `__data__` 봉투로 감싸므로 쓰지 않고, redis 커넥션으로 직접 PUBLISH 한다.

요청 스트림(`resume.parse.requested`) 소비는 그대로 Stream + Consumer Group 이다.
전달 횟수 조회는 도메인 공통이라 `messaging.pel` 로 옮겼다.
"""

from __future__ import annotations

import json

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
    """상태 이벤트를 Pub/Sub 채널에 JSON 으로 발행한다 (Spring 전 인스턴스가 구독)."""
    payload = StatusChanged(
        resumeId=resume_id, userId=user_id, status=status, message=message
    ).encode()
    await redis.publish(StatusChanged.CHANNEL, json.dumps(payload, ensure_ascii=False))
