"""면접 시계 단위 테스트 — 주입 monotonic 기준 경계 검증. docs/prd/interview-end.md §1."""

import pytest

from src.interview.interview_clock import InterviewClock

DURATION = 1800.0
WRAP_UP = 300.0
GRACE = 180.0


def _clock(now: dict) -> InterviewClock:
    return InterviewClock(
        duration_seconds=DURATION,
        wrap_up_remaining_seconds=WRAP_UP,
        hard_grace_seconds=GRACE,
        monotonic=lambda: now["t"],
    )


def test_elapsed_before_start_raises():
    clock = _clock({"t": 0.0})
    assert clock.started is False
    with pytest.raises(RuntimeError):
        clock.elapsed_seconds()


def test_start_is_idempotent_and_keeps_first_observation():
    now = {"t": 10.0}
    clock = _clock(now)
    clock.start()
    now["t"] = 20.0
    clock.start()  # 재호출 무시 — 최초 관측 유지
    now["t"] = 30.0
    assert clock.elapsed_seconds() == 20.0


def test_wrap_up_boundary_is_inclusive():
    now = {"t": 0.0}
    clock = _clock(now)
    clock.start()
    now["t"] = DURATION - WRAP_UP - 1  # 남은 시간 301초
    assert clock.in_wrap_up() is False
    now["t"] = DURATION - WRAP_UP  # 남은 시간 == 임계치 (이하 → 진입)
    assert clock.in_wrap_up() is True


def test_hard_boundary_is_inclusive():
    now = {"t": 0.0}
    clock = _clock(now)
    clock.start()
    now["t"] = DURATION + GRACE - 1
    assert clock.hard_exceeded() is False
    assert clock.hard_deadline_in() == 1.0
    now["t"] = DURATION + GRACE
    assert clock.hard_exceeded() is True


def test_remaining_goes_negative_after_scheduled_end():
    now = {"t": 0.0}
    clock = _clock(now)
    clock.start()
    now["t"] = DURATION + 60
    assert clock.remaining_seconds() == -60
