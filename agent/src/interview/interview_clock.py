"""면접 시계 — candidate 입장 관측 시각 기준. docs/prd/interview-end.md §1.

경과 계산은 단조 시계(monotonic) 기준이라 벽시계 조정에 영향받지 않는다.
시간의 계산·강제 권한은 코드에 있고, LLM에는 판단 재료로만 전달된다(§2 스토리).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


class InterviewClock:
    def __init__(
        self,
        *,
        duration_seconds: float,
        wrap_up_remaining_seconds: float,
        hard_grace_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._duration = duration_seconds
        self._wrap_up_remaining = wrap_up_remaining_seconds
        self._hard_grace = hard_grace_seconds
        self._monotonic = monotonic
        self._started_at: float | None = None

    @property
    def started(self) -> bool:
        return self._started_at is not None

    def start(self) -> None:
        """candidate 입장 관측 시점에 호출 — 재호출은 무시한다(최초 관측 유지)."""
        if self._started_at is None:
            self._started_at = self._monotonic()

    def start_with_elapsed(self, elapsed_seconds: float) -> None:
        """복원 시작 — 내구 저장된 시작 시각에서 역산한 경과를 주입한다.

        docs/prd/interview-recovery.md §2: 경과 = 현재 벽시계 − startedAt.
        음수 경과(미래 startedAt — 벽시계 역행·오염)는 0으로 clamp한다 —
        면접이 길어지는 방향의 오류만 차단한다. 재호출은 무시(최초 관측 유지).
        """
        if self._started_at is not None:
            return
        if elapsed_seconds < 0:
            logger.warning(
                "복원 경과가 음수(%.1fs) — 0으로 clamp(startedAt 오염 의심)",
                elapsed_seconds,
            )
            elapsed_seconds = 0.0
        self._started_at = self._monotonic() - elapsed_seconds

    def elapsed_seconds(self) -> float:
        if self._started_at is None:
            raise RuntimeError("시계 미시작 — candidate 입장 관측 후 start()가 선행돼야 한다")
        return self._monotonic() - self._started_at

    def remaining_seconds(self) -> float:
        """예정 종료까지 남은 시간 — 초과하면 음수."""
        return self._duration - self.elapsed_seconds()

    def in_wrap_up(self) -> bool:
        """soft 신호 — 남은 시간이 임계치 이하로 내려간 마무리 단계."""
        return self.remaining_seconds() <= self._wrap_up_remaining

    def hard_deadline_in(self) -> float:
        """hard 강제(예정 종료 + 유예)까지 남은 시간 — 초과하면 음수."""
        return self.remaining_seconds() + self._hard_grace

    def hard_exceeded(self) -> bool:
        return self.hard_deadline_in() <= 0
