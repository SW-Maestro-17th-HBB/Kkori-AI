"""면접 상태 복원 — 판별·경과 역산·종료 국면 유도. docs/prd/interview-recovery.md §2.

재디스패치된 잡이 Redis 상태 스냅샷(RestoreState)으로 "그 면접"을 이어가기 위한
순수 판별 로직이다 — I/O 없음(스냅샷 입력 → 계획 출력), main이 계획을 실행한다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto

from src.interview.conversation_log import (
    ConversationLog,
    QuestionType,
    Speaker,
    rebuild_conversation_log,
)
from src.interview.end_state import EndCause
from src.interview.session_store import RestoreState

logger = logging.getLogger(__name__)


class ResumeMode(Enum):
    """종료 국면 유도 판별표(PRD §2) — 마지막 발화 기준."""

    RUNNING = auto()  # 본론 재개 — 재개 안내 + 앵커
    WAITING_FINAL_ANSWER = auto()  # final 미답변 — 국면 복원 + final 재낭독
    CLOSE_FINAL_ANSWERED = auto()  # final 답변까지 완료 — CLOSING 진입
    CLOSE_RECOVERED = auto()  # closing까지 재생됨 — 표식 재기록 후 종료 시퀀스 재개


@dataclass(frozen=True)
class RestorePlan:
    """복원 실행 계획 — main의 복원 분기 입력."""

    log: ConversationLog
    dropped: int  # JSON 파싱 불가 + 스키마 위반 드롭 합계
    elapsed_seconds: float
    started_at_approximated: bool  # 첫 발화 spokenAt 근사 여부(관측 재료)
    candidate_identity: str | None  # None = fail-closed 종료 수렴 (PRD §2)
    reconnect_deadline: datetime | None
    mode: ResumeMode
    closing_cause: EndCause | None  # CLOSE_* 모드에서만 존재
    orphan_branch: bool  # 현재 줄기 루트 유실 — 다음 판단 강제 전환


def build_restore_plan(state: RestoreState, *, now: datetime) -> RestorePlan | None:
    """판별표 적용 — None이면 복원 재료 부족(새 면접 폴백).

    시작 시각: startedAt → 유실(부재·파싱 불가) 시 첫 발화 spokenAt 근사(시작 ≤
    첫 발화 시각이므로 사용자에게 유리한 방향). 둘 다 없으면 신규와 구조적으로
    구분 불가 — 새 면접(수용 리스크 확정).
    """
    log, schema_dropped = rebuild_conversation_log(state.utterances)
    dropped = state.dropped + schema_dropped
    if dropped:
        logger.warning("복원 재구성 — 발화 %d건 드롭(파싱 불가·스키마 위반)", dropped)

    started_at = state.started_at
    approximated = False
    if started_at is None:
        if state.started_at_malformed:
            logger.warning("startedAt 파싱 불가 — 유실로 취급, 첫 발화 근사 폴백")
        if not log.utterances:
            return None  # 시작 시각·대화 모두 없음 — 새 면접 폴백
        started_at = log.utterances[0].spoken_at
        approximated = True

    elapsed = (now - started_at).total_seconds()  # 음수는 시계가 clamp(경고)

    mode, closing_cause = _derive_mode(log)
    return RestorePlan(
        log=log,
        dropped=dropped,
        elapsed_seconds=elapsed,
        started_at_approximated=approximated,
        candidate_identity=state.candidate_identity,
        reconnect_deadline=state.reconnect_deadline,
        mode=mode,
        closing_cause=closing_cause,
        orphan_branch=not log.has_valid_current_root(),
    )


def _derive_mode(log: ConversationLog) -> tuple[ResumeMode, EndCause | None]:
    """종료 국면 유도 — 별도 상태 저장 없이 마지막 발화에서 유도한다(PRD §2).

    gap으로 closing·final이 유실되면 국면을 과소 복원한다 — 표식 기록 실패와
    동시에 난 경우로 한정되는 잔여 리스크(감수·로그 관측).
    """
    utterances = log.utterances
    last = utterances[-1] if utterances else None
    if last is None:
        return ResumeMode.RUNNING, None
    if last.question_type is QuestionType.CLOSING:
        # 클로징까지 재생됐으나 flush 전 소실 — 표식을 RECOVERED_CLOSING으로
        # 재기록해야 복원 flush까지 실패해도 재디스패치가 반복되지 않는다
        return ResumeMode.CLOSE_RECOVERED, EndCause.RECOVERED_CLOSING
    if last.speaker is Speaker.CANDIDATE and last.question_number is not None:
        question = log.question_for(last.question_number)
        if question is not None and question.question_type is QuestionType.FINAL:
            return ResumeMode.CLOSE_FINAL_ANSWERED, EndCause.FINAL_QUESTION
    if (
        last.speaker is Speaker.INTERVIEWER
        and last.question_type is QuestionType.FINAL
    ):
        return ResumeMode.WAITING_FINAL_ANSWER, None
    return ResumeMode.RUNNING, None
