"""컨텍스트 구성 단위 테스트 — 선택 규칙·직렬화·절단. docs/prd/follow-up-question.md §4."""

from datetime import datetime, timezone

from src.interview.context import (
    branch_text,
    estimate_tokens,
    follow_up_messages,
    orchestrator_context,
    recent_branch_messages,
)
from src.interview.conversation_log import ConversationLog, FollowUpType, QuestionType

NOW = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)
WIDE = 10_000  # 절단이 일어나지 않는 넉넉한 예산


def _log() -> ConversationLog:
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.INITIAL, content="자기소개 부탁드립니다.", spoken_at=NOW,
    )
    log.append_answer("백엔드 지망입니다.", NOW)
    log.append_question(
        question_number=2, parent_question_number=2,
        question_type=QuestionType.TOPIC, content="결제 프로젝트 이야기를 해볼까요?", spoken_at=NOW,
    )
    log.append_answer("Redis 캐시를 도입했습니다.", NOW)
    log.append_question(
        question_number=3, parent_question_number=2,
        question_type=QuestionType.FOLLOW_UP, content="왜 Redis였나요?", spoken_at=NOW,
        follow_up_type=FollowUpType.DEEPEN,
    )
    log.append_answer("조회 지연 때문입니다.", NOW)
    return log


# --- 직렬화 형식 ---

def test_orchestrator_context_uses_number_tags():
    text = orchestrator_context(_log(), token_budget=WIDE, utterance_token_cap=WIDE)
    lines = text.splitlines()
    assert lines[0] == "[Q1|root1] 자기소개 부탁드립니다."
    assert lines[1] == "[A1] 백엔드 지망입니다."
    assert lines[4] == "[Q3|root2] 왜 Redis였나요?"
    assert len(lines) == 6


def test_follow_up_messages_are_current_branch_with_role_mapping():
    messages = follow_up_messages(_log(), utterance_token_cap=WIDE)
    assert [role for role, _ in messages] == ["assistant", "user", "assistant", "user"]
    assert messages[0][1] == "결제 프로젝트 이야기를 해볼까요?"  # 초기 줄기 제외


def test_recent_branch_messages_respects_n():
    messages = recent_branch_messages(_log(), n=1, utterance_token_cap=WIDE)
    assert len(messages) == 4  # 현재 줄기(2번 루트)만
    all_messages = recent_branch_messages(_log(), n=5, utterance_token_cap=WIDE)
    assert len(all_messages) == 6


def test_branch_text_serializes_referenced_branch():
    text = branch_text(_log(), 1, utterance_token_cap=WIDE)
    assert text == "[Q1|root1] 자기소개 부탁드립니다.\n[A1] 백엔드 지망입니다."


# --- 토큰 추정·절단 ---

def test_estimate_tokens_is_conservative_for_korean():
    korean = "가" * 300
    assert estimate_tokens(korean) >= 150  # chars/4(75)보다 훨씬 보수적


def test_truncation_drops_oldest_branch_first_and_keeps_current():
    log = _log()
    full = orchestrator_context(log, token_budget=WIDE, utterance_token_cap=WIDE)
    current_only = orchestrator_context(
        log,
        token_budget=estimate_tokens(full) - 1,
        utterance_token_cap=WIDE,
    )
    assert "[Q1" not in current_only  # 오래된 줄기(초기)부터 제외
    assert current_only.startswith("[Q2|root2]")
    assert "[Q3|root2]" in current_only


def test_truncation_within_current_branch_keeps_last_question_and_answers():
    log = _log()
    minimal = orchestrator_context(log, token_budget=1, utterance_token_cap=WIDE)
    assert minimal.splitlines() == ["[Q3|root2] 왜 Redis였나요?", "[A3] 조회 지연 때문입니다."]
    assert estimate_tokens(minimal) > 1  # 예산을 넘어도 직전 Q+A는 최소 보장


def test_per_utterance_cap_clips_only_at_injection():
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.TOPIC, content="질문", spoken_at=NOW,
    )
    long_answer = "가" * 500
    log.append_answer(long_answer, NOW)
    text = orchestrator_context(log, token_budget=WIDE, utterance_token_cap=10)
    assert "…(중략)" in text
    assert log.utterances[-1].content == long_answer  # 로그 원문은 보존


def test_empty_log_serializes_to_empty():
    assert orchestrator_context(ConversationLog(), token_budget=WIDE, utterance_token_cap=WIDE) == ""
