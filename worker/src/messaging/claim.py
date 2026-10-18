"""XREADGROUP CLAIM 공통 — 서버 버전 확인과 받은 메시지 읽기.

이력서·리포트 두 워커가 같은 소비 규칙을 쓰므로 공유한다.
계약(ParseRequest·ReportGenerationRequested)에 의존하지 않아 어느 도메인에서든 임포트 가능하다.
"""

from __future__ import annotations

from faststream.redis import RedisStreamMessage
from redis.asyncio import Redis

__all__ = ["CLAIM_MIN_REDIS_VERSION", "ensure_claim_supported", "entry_of", "supports_claim"]

CLAIM_MIN_REDIS_VERSION = (8, 4)


def supports_claim(redis_version: str) -> bool:
    """`INFO server` 의 redis_version 이 XREADGROUP CLAIM 을 지원하는지."""
    major, minor = (int(part) for part in redis_version.split(".")[:2])
    return (major, minor) >= CLAIM_MIN_REDIS_VERSION


async def ensure_claim_supported(redis_url: str) -> None:
    """Redis 가 XREADGROUP CLAIM(8.4+)을 지원하지 않으면 기동을 막는다.

    CLAIM 이 거부되면 FastStream 은 구독자를 멈추고 앱 종료를 요청하지만, AsgiFastStream
    (이력서 워커)은 HTTP 서버가 살아 있어 메시지를 읽지 않는 채로 프로세스가 남는다(실측).
    그래서 구독자가 뜨기 전(on_startup)에 먼저 확인한다.
    """
    client = Redis.from_url(redis_url)
    try:
        version = (await client.info("server"))["redis_version"]
    finally:
        await client.aclose()
    if not supports_claim(version):
        required = ".".join(map(str, CLAIM_MIN_REDIS_VERSION))
        raise RuntimeError(
            f"Redis {version} 은 XREADGROUP CLAIM 을 지원하지 않는다 — {required} 이상이 필요하다"
        )


def entry_of(msg: RedisStreamMessage) -> tuple[bytes, dict, int]:
    """(message id, 원본 필드, 이번이 몇 번째 전달인지).

    `claim_min_idle_time` 을 준 구독자는 메시지마다 이전 전달 횟수를 함께 받는다
    (새 메시지 0, 회수한 메시지는 XPENDING 의 times_delivered - 1). 이번 전달까지 세려고
    +1 한다 — 포기 규칙(§4)의 판단 근거라 XPENDING 의 times_delivered 와 같은 값이어야 한다.
    """
    raw = msg.raw_message
    return raw["message_ids"][0], raw["data"], raw["delivery_counts"][0] + 1
