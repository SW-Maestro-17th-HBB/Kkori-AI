"""종료 상태 머신 단위 테스트 — 전진 전용·first-wins 원인 확정. docs/prd/interview-end.md §1."""

import pytest

from src.interview.end_state import EndCause, EndPhase, EndState


def test_initial_phase_is_running_without_cause():
    state = EndState()
    assert state.phase is EndPhase.RUNNING
    assert state.cause is None


def test_forward_transition_wins_and_duplicate_is_noop():
    state = EndState()
    assert state.try_advance(EndPhase.WAITING_FINAL_ANSWER) is True
    assert state.try_advance(EndPhase.WAITING_FINAL_ANSWER) is False  # 중복 no-op
    assert state.phase is EndPhase.WAITING_FINAL_ANSWER


def test_backward_transition_is_noop():
    state = EndState()
    state.try_advance(EndPhase.CLOSING, EndCause.USER_REQUEST)
    state.try_advance(EndPhase.CLEANING)
    assert state.try_advance(EndPhase.CLOSING, EndCause.HARD_TIMEOUT) is False
    assert state.phase is EndPhase.CLEANING
    assert state.cause is EndCause.USER_REQUEST  # 역방향 시도가 원인을 덮지 않는다


def test_closing_requires_cause():
    state = EndState()
    with pytest.raises(ValueError):
        state.try_advance(EndPhase.CLOSING)


def test_cause_is_only_allowed_on_closing_entry():
    state = EndState()
    with pytest.raises(ValueError):
        state.try_advance(EndPhase.WAITING_FINAL_ANSWER, EndCause.FINAL_QUESTION)


def test_hard_promotes_waiting_final_answer_to_closing():
    state = EndState()
    state.try_advance(EndPhase.WAITING_FINAL_ANSWER)
    assert state.try_advance(EndPhase.CLOSING, EndCause.HARD_TIMEOUT) is True
    assert state.cause is EndCause.HARD_TIMEOUT


def test_closing_cause_is_first_wins():
    state = EndState()
    assert state.try_advance(EndPhase.CLOSING, EndCause.USER_REQUEST) is True
    assert state.try_advance(EndPhase.CLOSING, EndCause.HARD_TIMEOUT) is False
    assert state.cause is EndCause.USER_REQUEST


def test_running_can_enter_closing_directly():
    state = EndState()
    assert state.try_advance(EndPhase.CLOSING, EndCause.LLM_END) is True
    assert state.phase is EndPhase.CLOSING


def test_cannot_skip_closing_into_cleanup():
    state = EndState()
    with pytest.raises(ValueError):
        state.try_advance(EndPhase.CLEANING)


def test_full_forward_walk():
    state = EndState()
    assert state.try_advance(EndPhase.WAITING_FINAL_ANSWER) is True
    assert state.try_advance(EndPhase.CLOSING, EndCause.FINAL_QUESTION) is True
    assert state.try_advance(EndPhase.CLEANING) is True
    assert state.try_advance(EndPhase.CLOSED) is True
    assert state.phase is EndPhase.CLOSED
    assert state.cause is EndCause.FINAL_QUESTION
