"""면접 시계 — candidate 입장 관측 시각 기준. docs/prd/interview-end.md §1.

경과 계산은 단조 시계(monotonic) 기준이라 벽시계 조정에 영향받지 않는다.
시간의 계산·강제 권한은 코드에 있고, LLM에는 판단 재료로만 전달된다(§2 스토리).
"""

from __future__ import annotations

import time
from collections.abc import Callable


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
