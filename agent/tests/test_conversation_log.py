"""대화 로그 단위 테스트 — 발화 불변식·직렬화·줄기 조회. docs/prd/follow-up-question.md §4."""

from datetime import datetime, timezone

import pytest

from src.interview.conversation_log import (
    ConversationLog,
    FollowUpType,
    QuestionType,
    Speaker,
    Utterance,
)

NOW = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)


def _sample_log() -> ConversationLog:
    """초기(1) → 주제(2) → 꼬리(3) → 일관성 꼬리(4) → 주제(5) 전형 흐름."""
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.INITIAL, content="자기소개 부탁드립니다.", spoken_at=NOW,
    )
    log.append_answer("백엔드 지망 홍길동입니다.", NOW)
    log.append_question(
        question_number=2, parent_question_number=2,
        question_type=QuestionType.TOPIC, content="결제 프로젝트를 소개해 주세요.",
        spoken_at=NOW, reason="이력서의 결제 프로젝트가 핵심 경험",
    )
    log.append_answer("Redis 캐시로 조회를 개선했습니다.", NOW)
    log.append_question(
        question_number=3, parent_question_number=2,
        question_type=QuestionType.FOLLOW_UP, content="캐시 무효화는 어떻게 하셨나요?",
        spoken_at=NOW, follow_up_type=FollowUpType.DEEPEN, reason="개선 원리 설명이 없음",
    )
    log.append_answer("TTL과 이벤트 무효화를 함께 썼습니다.", NOW)
    log.append_question(
        question_number=4, parent_question_number=2,
        question_type=QuestionType.FOLLOW_UP, content="아까는 혼자 하셨다고 했는데요.",
        spoken_at=NOW, follow_up_type=FollowUpType.CONSISTENCY,
        reason="첫 답변과 역할 서술 상충", ref_question_number=1,
    )
    log.append_answer("설계는 혼자, 구현은 둘이 했습니다.", NOW)
    log.append_question(
        question_number=5, parent_question_number=5,
        question_type=QuestionType.TOPIC, content="협업 갈등 경험이 있나요?",
        spoken_at=NOW, reason="협업 주제 미탐색",
    )
    return log


# --- 불변식 ---

def test_candidate_cannot_carry_question_metadata():
    with pytest.raises(ValueError):
        Utterance(
            question_number=1, parent_question_number=1, speaker=Speaker.CANDIDATE,
            content="답변", spoken_at=NOW, question_type=QuestionType.INITIAL,
        )


def test_interviewer_requires_question_type():
    with pytest.raises(ValueError):
        Utterance(
            question_number=1, parent_question_number=1, speaker=Speaker.INTERVIEWER,
            content="질문", spoken_at=NOW,
        )


def test_followup_requires_type_and_earlier_root():
    with pytest.raises(ValueError):
        Utterance(
            question_number=3, parent_question_number=2, speaker=Speaker.INTERVIEWER,
            content="질문", spoken_at=NOW, question_type=QuestionType.FOLLOW_UP,
        )
    with pytest.raises(ValueError):
        Utterance(
            question_number=3, parent_question_number=3, speaker=Speaker.INTERVIEWER,
            content="질문", spoken_at=NOW, question_type=QuestionType.FOLLOW_UP,
            follow_up_type=FollowUpType.DEEPEN,
        )


def test_root_question_must_be_parent_of_self():
    with pytest.raises(ValueError):
        Utterance(
            question_number=2, parent_question_number=1, speaker=Speaker.INTERVIEWER,
            content="질문", spoken_at=NOW, question_type=QuestionType.TOPIC,
        )


def test_initial_cannot_carry_reason():
    with pytest.raises(ValueError):
        Utterance(
            question_number=1, parent_question_number=1, speaker=Speaker.INTERVIEWER,
            content="질문", spoken_at=NOW, question_type=QuestionType.INITIAL,
            reason="이유",
        )


def test_consistency_requires_ref_and_others_reject_ref():
    with pytest.raises(ValueError):
        Utterance(
            question_number=3, parent_question_number=2, speaker=Speaker.INTERVIEWER,
            content="질문", spoken_at=NOW, question_type=QuestionType.FOLLOW_UP,
            follow_up_type=FollowUpType.CONSISTENCY,
        )
    with pytest.raises(ValueError):
        Utterance(
            question_number=3, parent_question_number=2, speaker=Speaker.INTERVIEWER,
            content="질문", spoken_at=NOW, question_type=QuestionType.FOLLOW_UP,
            follow_up_type=FollowUpType.DEEPEN, ref_question_number=1,
        )


def test_spoken_at_must_be_timezone_aware():
    with pytest.raises(ValueError):
        Utterance(
            question_number=1, parent_question_number=1, speaker=Speaker.CANDIDATE,
            content="답변", spoken_at=datetime(2026, 7, 24, 9, 0, 0),
        )


# --- 직렬화 ---

def test_json_dict_omits_absent_fields():
    answer = _sample_log().utterances[1]
    data = answer.to_json_dict()
    assert data == {
        "questionNumber": 1,
        "parentQuestionNumber": 1,
        "speaker": "CANDIDATE",
        "content": "백엔드 지망 홍길동입니다.",
        "spokenAt": "2026-07-24T09:00:00Z",
    }
    assert "questionType" not in data and "reason" not in data


def test_json_dict_consistency_question_carries_all_metadata():
    question = _sample_log().question_for(4)
    data = question.to_json_dict()
    assert data["questionType"] == "followup"
    assert data["followUpType"] == "CONSISTENCY"
    assert data["refQuestionNumber"] == 1
    assert data["reason"] == "첫 답변과 역할 서술 상충"
    assert None not in data.values()


# --- 적재 규칙 ---

def test_question_number_must_increase_by_one():
    log = _sample_log()
    with pytest.raises(ValueError):
        log.append_question(
            question_number=7, parent_question_number=7,
            question_type=QuestionType.TOPIC, content="질문", spoken_at=NOW,
        )


def test_answer_inherits_current_question_and_allows_multiple():
    log = _sample_log()
    first = log.append_answer("추가 답변 하나", NOW)
    second = log.append_answer("아, 그리고 하나 더요.", NOW)
    assert (first.question_number, first.parent_question_number) == (5, 5)
    assert (second.question_number, second.parent_question_number) == (5, 5)


def test_answer_before_any_question_is_rejected():
    with pytest.raises(ValueError):
        ConversationLog().append_answer("답변", NOW)


def test_followup_chain_parent_is_rejected():
    """꼬리질문의 꼬리질문도 parent는 줄기 루트 — 직전 꼬리질문 참조(체인)는 거부."""
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.TOPIC, content="주제", spoken_at=NOW,
    )
    log.append_question(
        question_number=2, parent_question_number=1,
        question_type=QuestionType.FOLLOW_UP, content="꼬리", spoken_at=NOW,
        follow_up_type=FollowUpType.DEEPEN,
    )
    with pytest.raises(ValueError):
        log.append_question(
            question_number=3, parent_question_number=2,  # 체인 — Q2는 루트가 아님
            question_type=QuestionType.FOLLOW_UP, content="꼬리의 꼬리", spoken_at=NOW,
            follow_up_type=FollowUpType.DEEPEN,
        )


def test_followup_to_previous_branch_root_is_rejected():
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.TOPIC, content="주제1", spoken_at=NOW,
    )
    log.append_question(
        question_number=2, parent_question_number=2,
        question_type=QuestionType.TOPIC, content="주제2", spoken_at=NOW,
    )
    with pytest.raises(ValueError):
        log.append_question(
            question_number=3, parent_question_number=1,  # 이전 줄기 루트 — 현재 줄기 아님
            question_type=QuestionType.FOLLOW_UP, content="꼬리", spoken_at=NOW,
            follow_up_type=FollowUpType.DEEPEN,
        )


# --- 조회 ---

def test_branch_queries():
    log = _sample_log()
    assert log.last_question_number() == 5
    assert log.current_root() == 5
    assert log.branch_roots() == (1, 2, 5)
    assert [u.question_number for u in log.branch(2)] == [2, 2, 3, 3, 4, 4]
    assert len(log.previous_branches()) == 2
    assert [b[0].question_number for b in log.recent_branches(2)] == [2, 5]


def test_followup_count_includes_consistency():
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.TOPIC, content="주제 질문", spoken_at=NOW,
    )
    log.append_answer("답변", NOW)
    log.append_question(
        question_number=2, parent_question_number=1,
        question_type=QuestionType.FOLLOW_UP, content="꼬리", spoken_at=NOW,
        follow_up_type=FollowUpType.DEEPEN,
    )
    log.append_question(
        question_number=3, parent_question_number=1,
        question_type=QuestionType.FOLLOW_UP, content="일관성 꼬리", spoken_at=NOW,
        follow_up_type=FollowUpType.CONSISTENCY, ref_question_number=1,
    )
    assert log.followup_count_in_current_branch() == 2


def test_first_answer_detection_helpers():
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.INITIAL, content="자기소개?", spoken_at=NOW,
    )
    assert not log.has_topic_or_followup_question()
    assert _sample_log().has_topic_or_followup_question()


def test_question_for_and_contents():
    log = _sample_log()
    assert log.question_for(3).content == "캐시 무효화는 어떻게 하셨나요?"
    assert log.question_for(99) is None
    assert len(log.all_question_contents()) == 5
