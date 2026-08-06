"""면접 상태 복원 단위 테스트 — 재구성·역산·국면 유도. docs/prd/interview-recovery.md §2."""

from datetime import datetime, timedelta, timezone

from src.interview.conversation_log import (
    QuestionType,
    rebuild_conversation_log,
)
from src.interview.end_state import EndCause
from src.interview.interview_clock import InterviewClock
from src.interview.restore import ResumeMode, build_restore_plan
from src.interview.session_store import RestoreState

BASE = datetime(2026, 8, 6, 9, 0, 0, tzinfo=timezone.utc)
NOW = BASE + timedelta(minutes=22)  # 시작 20분 진행 + 소실 후 2분 뒤 복원 시나리오


def _iso(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds").replace("+00:00", "Z")


def _q(number, *, qtype="topic", parent=None, offset=0, content="질문입니다"):
    data = {
        "questionNumber": number,
        "parentQuestionNumber": parent if parent is not None else number,
        "speaker": "INTERVIEWER",
        "questionType": qtype,
        "content": content,
        "spokenAt": _iso(BASE + timedelta(seconds=offset)),
    }
    if qtype == "followup":
        data["followUpType"] = "DEEPEN"
    return data


def _a(number, *, parent=None, offset=0, content="답변입니다"):
    return {
        "questionNumber": number,
        "parentQuestionNumber": parent if parent is not None else number,
        "speaker": "CANDIDATE",
        "content": content,
        "spokenAt": _iso(BASE + timedelta(seconds=offset)),
    }


def _closing(offset=0):
    return {
        "speaker": "INTERVIEWER",
        "questionType": "closing",
        "content": "오늘 면접은 여기까지입니다. 수고하셨습니다.",
        "spokenAt": _iso(BASE + timedelta(seconds=offset)),
    }


def _state(utterances=(), **kwargs) -> RestoreState:
    return RestoreState(utterances=tuple(utterances), **kwargs)


# --- 관대한 재구성 ---

def test_rebuild_drops_only_schema_violations_and_keeps_orphans():
    items = [
        _q(1, qtype="initial"),
        _a(1),
        {"speaker": "INTERVIEWER", "content": "spokenAt 없음"},  # 파싱 불가 — 드롭
        _q(3, qtype="followup", parent=2),  # 루트(2) 유실 orphan — 보존
    ]
    log, dropped = rebuild_conversation_log(items)
    assert dropped == 1
    assert [u.question_number for u in log.utterances] == [1, 1, 3]
    assert log.has_valid_current_root() is False  # orphan 줄기 — 강제 전환 재료


def test_rebuild_number_floor_prevents_collision_after_gap():
    # 질문 2가 유실되고 답변 2만 남은 gap — 다음 발번은 max 관측 번호 + 1이어야 한다
    log, dropped = rebuild_conversation_log([_q(1, qtype="initial"), _a(1), _a(2, parent=2)])
    assert dropped == 0
    assert log.last_question_number() == 2  # floor — 마지막 질문(1)이 아니라 최대 관측(2)
    log.append_question(
        question_number=3,
        parent_question_number=3,
        question_type=QuestionType.TOPIC,
        content="다음 질문입니다",
        spoken_at=BASE,
    )  # 번호 충돌 없이 이어진다


def test_rebuild_roundtrip_preserves_valid_utterances():
    items = [_q(1, qtype="initial"), _a(1), _q(2), _a(2), _q(3, qtype="followup", parent=2)]
    log, dropped = rebuild_conversation_log(items)
    assert dropped == 0
    assert [u.to_json_dict() for u in log.utterances] == items
    assert log.has_valid_current_root() is True


# --- 판별·경과 역산 ---

def test_plan_elapsed_from_durable_started_at():
    plan = build_restore_plan(
        _state([_q(1, qtype="initial"), _a(1)], started_at=BASE), now=NOW
    )
    assert plan is not None
    assert plan.elapsed_seconds == 22 * 60  # 끊김·재디스패치 지연 포함 — 시계는 계속 흐른다
    assert plan.started_at_approximated is False
    assert plan.mode is ResumeMode.RUNNING


def test_plan_approximates_started_at_from_first_utterance_when_lost():
    plan = build_restore_plan(
        _state([_q(1, qtype="initial", offset=5), _a(1)], started_at=None), now=NOW
    )
    assert plan is not None
    assert plan.started_at_approximated is True
    assert plan.elapsed_seconds == 22 * 60 - 5  # 첫 발화 spokenAt 근사 — 사용자에게 유리한 방향


def test_plan_is_none_when_nothing_restorable():
    assert build_restore_plan(_state([], started_at=None), now=NOW) is None


def test_plan_empty_log_with_started_at_resumes_as_running():
    plan = build_restore_plan(_state([], started_at=BASE), now=NOW)
    assert plan is not None
    assert plan.mode is ResumeMode.RUNNING
    assert len(plan.log.utterances) == 0  # 빈 로그 — 신규와 같은 진행 경로(초기 질문 재수행)


# --- 종료 국면 유도 판별표 ---

def test_mode_recovered_closing_when_last_is_closing():
    plan = build_restore_plan(
        _state([_q(1, qtype="initial"), _a(1), _closing()], started_at=BASE), now=NOW
    )
    assert plan.mode is ResumeMode.CLOSE_RECOVERED
    assert plan.closing_cause is EndCause.RECOVERED_CLOSING  # 표식 재기록 원인


def test_mode_close_when_final_question_answered():
    plan = build_restore_plan(
        _state(
            [_q(1, qtype="initial"), _a(1), _q(2, qtype="final"), _a(2)],
            started_at=BASE,
        ),
        now=NOW,
    )
    assert plan.mode is ResumeMode.CLOSE_FINAL_ANSWERED
    assert plan.closing_cause is EndCause.FINAL_QUESTION


def test_mode_waiting_final_answer_when_final_unanswered():
    plan = build_restore_plan(
        _state([_q(1, qtype="initial"), _a(1), _q(2, qtype="final")], started_at=BASE),
        now=NOW,
    )
    assert plan.mode is ResumeMode.WAITING_FINAL_ANSWER
    assert plan.closing_cause is None


def test_plan_reports_orphan_branch():
    plan = build_restore_plan(
        _state([_q(1, qtype="initial"), _a(1), _q(3, qtype="followup", parent=2)],
               started_at=BASE),
        now=NOW,
    )
    assert plan.orphan_branch is True


# --- 시계 복원 ---

def test_clock_restore_injects_elapsed():
    clock = InterviewClock(
        duration_seconds=1800, wrap_up_remaining_seconds=300, hard_grace_seconds=180
    )
    clock.start_with_elapsed(1200)
    assert 599 <= clock.remaining_seconds() <= 600  # 20분 시점 복원 → 남은 시간 ~10분


def test_clock_restore_clamps_negative_elapsed():
    clock = InterviewClock(
        duration_seconds=1800, wrap_up_remaining_seconds=300, hard_grace_seconds=180
    )
    clock.start_with_elapsed(-120)  # 미래 startedAt(오염) — 면접이 길어지는 방향만 차단
    assert clock.remaining_seconds() <= 1800


def test_clock_restore_is_idempotent():
    clock = InterviewClock(
        duration_seconds=1800, wrap_up_remaining_seconds=300, hard_grace_seconds=180
    )
    clock.start_with_elapsed(1200)
    clock.start_with_elapsed(0)  # 재호출 무시 — 최초 관측 유지
    clock.start()
    assert clock.remaining_seconds() <= 600
