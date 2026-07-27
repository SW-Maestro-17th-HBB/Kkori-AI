"""대화 로그 Redis write-through 사본 — 단일 FIFO writer task. docs/prd/follow-up-question.md §4.

메모리 로그가 주 사본이고 Redis는 크래시 복원 대비 최선 노력 사본이다. enqueue는
논블로킹이며(느린 Redis가 턴 경로를 막지 않는다), 단일 writer task가 큐를 순서대로
소비해 메모리 순서 = Redis 순서를 보장한다. 어떤 실패도 면접을 중단시키지 않는다.
대화 내용은 운영 로그에 남기지 않는다(개인정보 — 건수·길이만).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import suppress
from datetime import datetime, timezone

from redis.asyncio import Redis

from src.config import REDIS_TRANSCRIPT_TTL_SECONDS

logger = logging.getLogger(__name__)

REDIS_URL_ENV = "KKORI_AGENT_REDIS_URL"

# writer 재량 파라미터 — 계약과 무관한 내부 설정
_QUEUE_MAX_SIZE = 256
_OP_TIMEOUT_SECONDS = 2.0
_DRAIN_TIMEOUT_SECONDS = 5.0


class RedisTranscriptWriter:
    """`interview:{sessionId}:transcript` List에 발화 객체 JSON을 순서대로 적재한다."""

    def __init__(self, *, url: str, session_id: str, ttl_seconds: int) -> None:
        self._key = f"interview:{session_id}:transcript"
        self._ttl_seconds = ttl_seconds
        self._redis = Redis.from_url(
            url,
            socket_timeout=_OP_TIMEOUT_SECONDS,
            socket_connect_timeout=_OP_TIMEOUT_SECONDS,
        )
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._task: asyncio.Task | None = None
        self._closed = False

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._drain_loop())

    def enqueue(self, utterance_json: dict) -> None:
        """논블로킹 적재 — 포화·종료 시 드롭하고 경고만 남긴다(gap 감수)."""
        if self._closed:
            logger.warning("전사 writer 종료 후 enqueue — 발화 1건 드롭")
            return
        try:
            self._queue.put_nowait(json.dumps(utterance_json, ensure_ascii=False))
        except asyncio.QueueFull:
            logger.warning("전사 큐 포화 — 발화 1건 드롭 (gap 감수)")

    async def wait_for_drain(self) -> None:
        """큐가 비워질 때까지 대기 — 테스트·종료 경로 전용."""
        await self._queue.join()

    async def aclose(self) -> None:
        """bounded drain 후 종료 — 멱등."""
        if self._closed:
            return
        self._closed = True
        if self._task is not None:
            try:
                await asyncio.wait_for(
                    self._queue.join(), timeout=_DRAIN_TIMEOUT_SECONDS
                )
            except TimeoutError:
                logger.warning(
                    "전사 큐 drain 시한 초과 — 잔여 %d건 유실", self._queue.qsize()
                )
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        await self._redis.aclose()

    async def _drain_loop(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    # RPUSH+EXPIRE 원자 실행 — TTL 없는 개인정보 키가 남지 않게,
                    # 매 append TTL 갱신(의미 = 마지막 발화 이후 잔존 상한)
                    pipe.rpush(self._key, item)
                    pipe.expire(self._key, self._ttl_seconds)
                    await pipe.execute()
            except Exception as exc:
                logger.warning(
                    "Redis 전사 적재 실패 — 면접은 계속 (gap 감수): %s",
                    type(exc).__name__,
                )
            finally:
                self._queue.task_done()


async def write_termination_marker(session_id: str, cause: str) -> bool:
    """종료 표식 기록 — transcript 행과 독립적인 종료 증거. docs/prd/interview-end.md §3.

    CLOSING 진입 부수효과로 1회 기록되며(클로징 재생 전 — 청취 보장 아님, "종료
    국면 진입" 증거), flush까지 실패한 최악 경로에서 Spring의 AGENT_LOST 판별
    재료가 된다. best-effort — 실패해도 종료 국면은 계속되고, 판별 계약은
    transcript 행 우선순위가 흡수한다. True = 기록 성공.
    """
    url = os.getenv(REDIS_URL_ENV)
    if not url:
        logger.warning("%s 미설정 — 종료 표식 생략", REDIS_URL_ENV)
        return False
    redis: Redis | None = None
    try:
        # 클라이언트 생성도 실패 경로다 — 잘못된 URL(ValueError)이 실패 로그를
        # 우회해 호출자로 전파되지 않게 try 안에서 만든다
        redis = Redis.from_url(
            url,
            socket_timeout=_OP_TIMEOUT_SECONDS,
            socket_connect_timeout=_OP_TIMEOUT_SECONDS,
        )
        payload = json.dumps(
            {
                "cause": cause,
                "markedAt": datetime.now(timezone.utc)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z"),
            }
        )
        # TTL은 transcript 사본과 동일 원칙 — 표식도 세션 데이터라 잔존을 제한한다
        await redis.set(
            f"interview:{session_id}:termination",
            payload,
            ex=REDIS_TRANSCRIPT_TTL_SECONDS,
        )
        return True
    except Exception as exc:
        logger.warning("종료 표식 기록 실패(%s) — 종료 국면은 계속", type(exc).__name__)
        return False
    finally:
        if redis is not None:
            with suppress(Exception):
                await redis.aclose()


def create_transcript_writer(session_id: str | None) -> RedisTranscriptWriter | None:
    """env·sessionId가 갖춰졌을 때만 writer를 만들어 시작한다. 아니면 메모리 단독."""
    url = os.getenv(REDIS_URL_ENV)
    if not url:
        logger.warning("%s 미설정 — 대화 로그는 메모리 단독으로 동작", REDIS_URL_ENV)
        return None
    if not session_id:
        logger.warning("sessionId 없음 — Redis 적재 생략(메모리 단독)")
        return None
    writer = RedisTranscriptWriter(
        url=url, session_id=session_id, ttl_seconds=REDIS_TRANSCRIPT_TTL_SECONDS
    )
    writer.start()
    return writer
