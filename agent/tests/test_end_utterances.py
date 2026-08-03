"""마무리 국면 발화 스키마·문구 세트 단위 테스트 — docs/prd/interview-end.md §2.

판별 규칙: 질문이면 번호 있음(final 포함), 인사면 없음(closing).
"""

from datetime import datetime, timezone

import pytest

from src.interview.conversation_log import (
    ConversationLog,
    QuestionType,
    Speaker,
    Utterance,
)
from src.interview.end_state import EndCause
from src.interview.prompts import (
    CLOSING_STATEMENTS_GENERAL,
    CLOSING_STATEMENTS_TIME_UP,
    FINAL_QUESTIONS,
    closing_statements_for,
    orchestrator_instructions,
)

NOW = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)


def _log_with_final() -> ConversationLog:
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.TOPIC, content="협업 경험을 들려주세요.", spoken_at=NOW,
    )
    log.append_answer("팀 프로젝트에서 백엔드를 맡았습니다.", NOW)
    log.append_question(
        question_number=2, parent_question_number=2,
        question_type=QuestionType.FINAL, content=FINAL_QUESTIONS[0], spoken_at=NOW,
        reason="주제가 소진됨",
    )
    return log


# --- final: 번호 있는 루트 질문 ---

def test_final_question_is_root_and_answer_inherits_number():
    log = _log_with_final()
    answer = log.append_answer("마지막으로 한마디 드리겠습니다.", NOW)
    assert answer.question_number == 2  # 마지막 답변이 final 번호 승계
    assert answer.parent_question_number == 2


def test_final_question_cannot_carry_follow_up_metadata():
    with pytest.raises(ValueError):
        Utterance(
            question_number=2, parent_question_number=2,
            speaker=Speaker.INTERVIEWER, content=FINAL_QUESTIONS[0], spoken_at=NOW,
            question_type=QuestionType.FINAL, follow_up_type=None,
            ref_question_number=1,
        )


# --- closing: 번호 없는 인사 ---

def test_closing_has_no_numbers_and_serializes_without_number_keys():
    log = _log_with_final()
    closing = log.append_closing(
        CLOSING_STATEMENTS_TIME_UP[0], NOW, reason="시간이 소진됨"
    )
    data = closing.to_json_dict()
    assert "questionNumber" not in data
    assert "parentQuestionNumber" not in data
    assert data["questionType"] == "closing"
    assert data["reason"] == "시간이 소진됨"
    assert data["speaker"] == "INTERVIEWER"


def test_closing_rejects_question_numbers():
    with pytest.raises(ValueError):
        Utterance(
            question_number=3, parent_question_number=3,
            speaker=Speaker.INTERVIEWER, content=CLOSING_STATEMENTS_GENERAL[0],
            spoken_at=NOW, question_type=QuestionType.CLOSING,
        )


def test_closing_does_not_disturb_question_numbering_or_branches():
    log = _log_with_final()
    log.append_closing(CLOSING_STATEMENTS_GENERAL[0], NOW)
    assert log.last_question_number() == 2  # closing은 질문이 아니다
    assert log.branch_roots() == (1, 2)  # 줄기 체계 밖
    assert CLOSING_STATEMENTS_GENERAL[0] not in log.all_question_contents()


# --- 클로징 문구 세트 매핑 ---

def test_closing_statement_sets_by_cause():
    assert closing_statements_for(EndCause.LLM_END) is CLOSING_STATEMENTS_TIME_UP
    assert closing_statements_for(EndCause.HARD_TIMEOUT) is CLOSING_STATEMENTS_TIME_UP
    assert closing_statements_for(EndCause.FINAL_QUESTION) is CLOSING_STATEMENTS_GENERAL
    assert closing_statements_for(EndCause.USER_REQUEST) is CLOSING_STATEMENTS_GENERAL


def test_final_questions_do_not_invite_counter_questions():
    # 역질문 유도형 금지 — candidate 질문에 답변할 능력이 없는 상황을 만들지 않는다
    for question in FINAL_QUESTIONS:
        assert "궁금한 점" not in question
        assert "질문" not in question


# --- 마무리 단계 판단 지시 ---

def test_wrap_up_instructions_swap_rules_and_inject_remaining_time():
    text = orchestrator_instructions("[Q1|root1] ...", wrap_up_remaining_minutes=4)
    assert "마무리 단계" in text
    assert "약 4분" in text
    assert "FINAL_QUESTION" in text
    assert "END" in text
    assert "NEXT_TOPIC" not in text  # 마무리 단계에 새 주제 없음


def test_wrap_up_instructions_handle_exhausted_time():
    text = orchestrator_instructions("[Q1|root1] ...", wrap_up_remaining_minutes=0)
    assert "1분도 채 남지 않았습니다" in text


def test_base_instructions_do_not_expose_wrap_up_actions():
    text = orchestrator_instructions("[Q1|root1] ...")
    assert "FINAL_QUESTION" not in text
    assert "마무리 단계" not in text
    assert "NEXT_TOPIC" in text
