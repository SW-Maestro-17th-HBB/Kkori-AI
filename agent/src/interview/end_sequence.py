"""종료 시퀀스 — 정상 종료의 정리 단계. docs/prd/interview-end.md §3.

CLEANING 진입 시 실행된다: (1) 대화 로그 확정(Redis writer drain·종료 — 대기 중
append가 이후 정리를 되살리지 않게) → (2) transcript DB flush(HBB1-287) →
(3) Redis 사본 정리(flush 성공 시에만) → (4) 리포트 생성 요청 발행(HBB1-288,
flush 성공 시에만) → (5) 룸 삭제(bounded retry — best-effort가 아니다: 실패한 채
퇴장하면 AGENT_LOST 오인 경로) → (6) 잡 종료.

룸 삭제를 제외한 각 단계는 실패해도 로그 후 다음 단계로 진행하고(퇴장 보장),
모든 외부 호출은 단계별 타임아웃 안에서 끝난다 — 시퀀스 전체가 유한 시간 안에
반드시 잡 종료에 도달한다.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from src.config import (
    END_STEP_TIMEOUT_SECONDS,
    ROOM_DELETE_MAX_ATTEMPTS,
    ROOM_DELETE_RETRY_BACKOFF_SECONDS,
)
from src.interview.end_state import EndCause

logger = logging.getLogger(__name__)


class EndSequence:
    """의존성 주입 기반 종료 시퀀스 — flush(HBB1-287)·발행(HBB1-288)은 주입점만 정의."""

    def __init__(
        self,
        *,
        shutdown_fn: Callable[[str], None],
        writer=None,  # RedisTranscriptWriter — aclose로 drain·종료 (멱등)
        flush_fn: Callable[[], Awaitable[bool]] | None = None,  # True = flush 성공
        purge_fn: Callable[[], Awaitable[None]] | None = None,  # Redis 사본 DEL
        publish_fn: Callable[[], Awaitable[None]] | None = None,  # 리포트 요청 발행
        delete_room_fn: Callable[[], Awaitable[None]] | None = None,  # 룸 삭제 1회 시도
        step_timeout_seconds: float = END_STEP_TIMEOUT_SECONDS,
        room_delete_max_attempts: int = ROOM_DELETE_MAX_ATTEMPTS,
        room_delete_backoff_seconds: float = ROOM_DELETE_RETRY_BACKOFF_SECONDS,
    ) -> None:
        self._shutdown_fn = shutdown_fn
        self._writer = writer
        self._flush_fn = flush_fn
        self._purge_fn = purge_fn
        self._publish_fn = publish_fn
        self._delete_room_fn = delete_room_fn
        self._step_timeout = step_timeout_seconds
        self._room_delete_attempts = room_delete_max_attempts
        self._room_delete_backoff = room_delete_backoff_seconds

    async def run(self, cause: EndCause) -> None:
        logger.info("종료 시퀀스 시작 — 원인=%s", cause)
        await self._finalize_log()
        flush_ok = await self._flush()
        if flush_ok:
            await self._purge_redis()
            await self._publish_report_request()
        else:
            logger.warning("flush 미완료 — Redis 사본 보존, 리포트 발행 생략")
        await self._delete_room()
        self._shutdown_fn(f"interview end: {cause}")

    async def _finalize_log(self) -> None:
        """신규 커밋은 종료 국면이 이미 차단했다 — writer만 drain·종료한다."""
        if self._writer is None:
            return
        try:
            # 주입된 writer의 내부 구현에 유한 시간을 의존하지 않는다 —
            # drain이 hang해도 종료 시퀀스는 단계 타임아웃 안에 계속된다
            await asyncio.wait_for(self._writer.aclose(), self._step_timeout)
        except Exception as exc:
            logger.warning("전사 writer 종료 실패(%s) — 계속", type(exc).__name__)

    async def _flush(self) -> bool:
        if self._flush_fn is None:
            logger.warning("transcript flush 미구현(HBB1-287) — 생략")
            return False
        try:
            return await asyncio.wait_for(self._flush_fn(), self._step_timeout)
        except Exception as exc:
            logger.error(
                "transcript flush 실패(%s) — Redis 사본 보존, 종료 시퀀스 계속",
                type(exc).__name__,
            )
            return False

    async def _purge_redis(self) -> None:
        if self._purge_fn is None:
            return
        try:
            await asyncio.wait_for(self._purge_fn(), self._step_timeout)
        except Exception as exc:
            # TTL 안전망이 남아 있다 — 정리 실패가 퇴장을 막지 않는다
            logger.warning("Redis 사본 정리 실패(%s) — TTL로 만료", type(exc).__name__)

    async def _publish_report_request(self) -> None:
        if self._publish_fn is None:
            logger.warning("리포트 요청 발행 미구현(HBB1-288) — 생략")
            return
        try:
            await asyncio.wait_for(self._publish_fn(), self._step_timeout)
        except Exception as exc:
            # "transcript 행 존재 & 리포트 없음"이 미발행 검출식 — 재발행은 worker 계약
            logger.error("리포트 요청 발행 실패(%s) — 종료 시퀀스 계속", type(exc).__name__)

    async def _delete_room(self) -> None:
        """bounded retry — 실패한 채 퇴장하면 Spring이 AGENT_LOST로 오인할 수 있어
        best-effort로 다루지 않는다. 소진 시에도 퇴장하되, Spring은 종료 표식·
        transcript 행 판별 계약으로 재dispatch를 막는다(PRD §3)."""
        if self._delete_room_fn is None:
            logger.warning("룸 삭제 생략(로컬·콘솔 — LiveKit 정리 대상 없음)")
            return
        for attempt in range(1, self._room_delete_attempts + 1):
            try:
                await asyncio.wait_for(self._delete_room_fn(), self._step_timeout)
                logger.info("룸 삭제 완료 — room_finished로 Spring에 정상 종료 신호")
                return
            except Exception as exc:
                logger.warning(
                    "룸 삭제 실패(%s) — 시도 %d/%d",
                    type(exc).__name__,
                    attempt,
                    self._room_delete_attempts,
                )
                if attempt < self._room_delete_attempts:
                    # 연속 재시도는 같은 일시 장애 창에서 상관돼 전부 실패한다 —
                    # 간격을 두어 장애 창을 벗어날 기회를 준다 (총 시간은 여전히 유한)
                    await asyncio.sleep(self._room_delete_backoff)
        logger.error(
            "룸 삭제 소진 — 종료 표식·transcript 행 기반 판별 계약으로 수렴 (PRD §3)"
        )
