"""종료 상태 머신 — 전진 전용 단일 수렴점. docs/prd/interview-end.md §1.

soft 마무리 액션·hard 타이머·외부 종료 신호가 모두 이 상태 전이로 수렴한다.
모든 전이는 이벤트 루프 위의 동기 호출(await 없음)이라 호출 자체가 원자적이다 —
try_advance가 True를 반환한(전이에 승리한) 호출자만 해당 상태의 진입 부수효과를
정확히 1회 실행한다. 종료 원인은 CLOSING 진입 전이에서만 first-wins로 확정되며,
클로징 문구 세트 선택(§2)이 이 원인을 따른다.
"""

from __future__ import annotations

from enum import IntEnum, StrEnum


class EndPhase(IntEnum):
    """전진 전용 — 값의 대소가 곧 진행 방향이다."""

    RUNNING = 0
    WAITING_FINAL_ANSWER = 1
    CLOSING = 2
    CLEANING = 3
    CLOSED = 4


class EndCause(StrEnum):
    """CLOSING 진입 경로 = 종료 원인. 일반형 문구=FINAL_QUESTION·USER_REQUEST,
    시간 소진형 문구=LLM_END·HARD_TIMEOUT (docs/prd/interview-end.md §2)."""

    FINAL_QUESTION = "FINAL_QUESTION"  # 마지막 답변 완료
    LLM_END = "LLM_END"  # Orchestrator END 판단
    HARD_TIMEOUT = "HARD_TIMEOUT"  # hard 강제(안전망)
    USER_REQUEST = "USER_REQUEST"  # 사용자 명시 종료
    RECONNECT_TIMEOUT = "RECONNECT_TIMEOUT"  # 재연결 창 소진 — flush 생략, ABORTED 수렴 (recovery §1)
    RECOVERED_CLOSING = "RECOVERED_CLOSING"  # 복원된 closing — 재발화 없이 종료 시퀀스 재개 (recovery §2)


class EndState:
    """종료 국면 상태 보관자 — 파이프라인·타이머·외부 신호 핸들러가 공유한다."""

    def __init__(self) -> None:
        self._phase = EndPhase.RUNNING
        self._cause: EndCause | None = None

    @property
    def phase(self) -> EndPhase:
        return self._phase

    @property
    def cause(self) -> EndCause | None:
        return self._cause

    def try_advance(self, to: EndPhase, cause: EndCause | None = None) -> bool:
        """전진 전이 시도. True = 이 호출이 승자(진입 부수효과 실행 권한).

        중복·역방향 전이는 no-op(False). CLOSING 진입에는 원인이 필수이며
        그때만 확정된다 — WAITING_FINAL_ANSWER 자체에는 원인을 두지 않는다.
        """
        if to is EndPhase.CLOSING and cause is None:
            raise ValueError("CLOSING 진입에는 종료 원인이 필요하다")
        if to is not EndPhase.CLOSING and cause is not None:
            raise ValueError("종료 원인은 CLOSING 진입에서만 확정한다")
        if to <= self._phase:
            return False
        if to > EndPhase.CLOSING and self._phase < EndPhase.CLOSING:
            raise ValueError("CLOSING(원인 확정)을 거치지 않고 정리 단계로 갈 수 없다")
        self._phase = to
        if to is EndPhase.CLOSING:
            self._cause = cause
        return True
