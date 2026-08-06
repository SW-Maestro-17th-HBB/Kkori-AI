"""candidate 이탈·재입장 관장 — 재연결 창의 소유자. docs/prd/interview-recovery.md §1.

candidate 이탈이 면접의 끝이 아니게 한다 — 이탈 관측 시 재연결 deadline(절대
시각)을 내구 기록하고 창 타이머를 시작하며, 진행 중 생성·발화를 폐기한다(청자
없음). 창 내 identity 일치 재입장이면 deadline을 지우고 재개(안내+앵커)로,
창 소진이면 종료 수렴으로 보낸다 — 승자는 종료 상태 머신이 정한다(first-wins).

candidate 판정은 최초 고정된 identity 일치로만 한다 — 위치·순서 기반 판정 금지.
모니터는 candidate 재실 상태에서 무장(arm)된다 — 최초 입장·복원 입장 대기는
main이 담당하고, 이후의 이탈·재입장 사이클만 여기서 관장한다.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import datetime, timedelta, timezone

from src.interview.end_state import EndCause

logger = logging.getLogger(__name__)


class PresenceMonitor:
    """의존성 주입 기반 — LiveKit 이벤트는 조립 코드(main)가 배선한다."""

    def __init__(
        self,
        *,
        candidate_identity: str,
        window_seconds: float,
        begin_closing_fn: Callable[[EndCause], bool],
        resume_fn: Callable[[], Awaitable[None]],
        invalidate_fn: Callable[[], None],
        record_deadline_fn: Callable[[datetime], Awaitable[object]] | None = None,
        clear_deadline_fn: Callable[[], Awaitable[object]] | None = None,
        hard_exceeded_fn: Callable[[], bool] | None = None,
        wall_clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._candidate_identity = candidate_identity
        self._window_seconds = window_seconds
        self._begin_closing_fn = begin_closing_fn
        self._resume_fn = resume_fn
        self._invalidate_fn = invalidate_fn
        self._record_deadline_fn = record_deadline_fn
        self._clear_deadline_fn = clear_deadline_fn
        self._hard_exceeded_fn = hard_exceeded_fn
        self._wall_clock = wall_clock
        self._present = True  # arm 시점 = candidate 재실
        self._left_at: datetime | None = None
        self._epoch = 0  # 이탈 관측 횟수 — 관측·로그 재료 + 입력 경계(epoch)
        # deadline 기록·삭제는 발생 순서대로 직렬 실행한다 — 빠른 재입장 시
        # 늦은 HSET이 HDEL 뒤에 실행돼 소진된 deadline을 되살리는 역전 차단
        self._store_lock = asyncio.Lock()
        self._watch_task: asyncio.Task | None = None
        self._tasks: set[asyncio.Task] = set()
        self._closed = False

    @property
    def is_present(self) -> bool:
        """candidate 재실 여부 — 파이프라인의 커밋·클로징 발화 게이트."""
        return self._present

    @property
    def epoch(self) -> int:
        """connection epoch — 이탈 관측마다 증가. 이전 연결 구간에서 시작된
        입력의 커밋 차단 재료(파이프라인 입력 경계, PRD §1)."""
        return self._epoch

    def on_participant_disconnected(self, identity: str) -> None:
        if self._closed:
            return
        if identity != self._candidate_identity:
            logger.info("candidate 아닌 참가자 퇴장 — 무시")
            return
        if not self._present:
            return
        self._present = False
        self._epoch += 1
        self._left_at = self._wall_clock()
        # 진행 중이던 질문 생성·발화 폐기 — 청자 없음. 이탈 관측 이후 완료되는
        # user turn은 파이프라인의 재실 게이트가 폐기한다 (PRD §1 입력 경계)
        self._invalidate_fn()
        deadline = self._left_at + timedelta(seconds=self._window_seconds)
        if self._record_deadline_fn is not None:
            self._spawn(self._serialized(self._record_deadline_fn(deadline)))
        self._cancel_watch()
        self._watch_task = asyncio.create_task(self._watch(deadline))
        logger.info(
            "candidate 이탈 관측(%d번째) — 재연결 창 시작(%.0f초)",
            self._epoch,
            self._window_seconds,
        )

    def on_participant_connected(self, identity: str) -> None:
        if self._closed:
            return
        if identity != self._candidate_identity:
            # 관전자·오입장·다른 identity — 재개 판정에서 무시 (PRD §1 식별 확정)
            logger.info("candidate 아닌 참가자 입장 — 재개로 처리하지 않음")
            return
        if self._present:
            return
        self._present = True
        absence = (
            (self._wall_clock() - self._left_at).total_seconds()
            if self._left_at
            else 0.0
        )
        self._cancel_watch()
        if self._clear_deadline_fn is not None:
            self._spawn(self._serialized(self._clear_deadline_fn()))
        logger.info("candidate 재입장 — 부재 %.0f초, 면접 재개", absence)
        self._spawn(self._resume_fn())

    async def _watch(self, deadline: datetime) -> None:
        """창 타이머 — 절대 deadline 기준(재디스패치돼도 창이 다시 부여되지 않는다)."""
        while True:
            remaining = (deadline - self._wall_clock()).total_seconds()
            if remaining <= 0:
                break
            await asyncio.sleep(remaining)
        # 만료 — 시간 기준 이원화(PRD Overview): hard 선소진이면 정상 종료(flush),
        # 아니면 창 소진(flush 생략). 재입장·hard 타이머와의 경합은 종료 상태
        # 머신의 first-wins가 수렴한다.
        cause = (
            EndCause.HARD_TIMEOUT
            if self._hard_exceeded_fn is not None and self._hard_exceeded_fn()
            else EndCause.RECONNECT_TIMEOUT
        )
        logger.warning("재연결 창 소진 — 종료 수렴 시도(원인=%s)", cause)
        self._begin_closing_fn(cause)

    async def _serialized(self, op) -> None:
        """deadline 저장소 작업의 FIFO 직렬 실행 — asyncio.Lock은 대기자를
        선착순으로 깨우므로 spawn 순서 = 실행 순서다."""
        async with self._store_lock:
            await op

    def _cancel_watch(self) -> None:
        if self._watch_task is not None and not self._watch_task.done():
            self._watch_task.cancel()
        self._watch_task = None

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("재연결 처리 task 예외(%s)", type(task.exception()).__name__)

    async def aclose(self) -> None:
        """타이머·부수 task 정리 — 멱등, job shutdown 시점에 호출."""
        if self._closed:
            return
        self._closed = True
        self._cancel_watch()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            with suppress(Exception):
                await asyncio.gather(*self._tasks, return_exceptions=True)
