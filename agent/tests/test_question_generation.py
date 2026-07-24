"""본론 질문 생성 단위 테스트 — 분기·출력 검증·폴백. docs/prd/follow-up-question.md §3."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from src.config import QUESTION_MAX_CHARS
from src.interview.conversation_log import (
    Action,
    ConversationLog,
    FollowUpType,
    QuestionType,
)
from src.interview.orchestrator import Decision, DecisionSource, forced_next_topic
from src.interview.prompts import FALLBACK_QUESTIONS
from src.interview.question_generation import generate_question

NOW = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)


class _StubStream:
    def __init__(self, text: str) -> None:
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def __aiter__(self):
        return self._chunks()

    async def _chunks(self):
        yield SimpleNamespace(delta=SimpleNamespace(content=self._text))


class _StubLLM:
    def __init__(self, text: str) -> None:
        self._text = text
        self.calls: list[dict] = []

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        return _StubStream(self._text)


class _FailingLLM:
    def chat(self, **kwargs):
        raise RuntimeError("boom")


def _log() -> ConversationLog:
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.TOPIC, content="결제 프로젝트를 소개해 주세요.", spoken_at=NOW,
    )
    log.append_answer("혼자 설계했습니다.", NOW)
    log.append_question(
        question_number=2, parent_question_number=2,
        question_type=QuestionType.TOPIC, content="협업 경험을 들려주세요.", spoken_at=NOW,
    )
    log.append_answer("팀 리드 설계를 제가 구현했습니다.", NOW)
    return log


def _deepen() -> Decision:
    return Decision(
        action=Action.FOLLOW_UP, source=DecisionSource.ORCHESTRATOR,
        follow_up_type=FollowUpType.DEEPEN, reason="구현 세부가 궁금함",
    )


def _generate(llm, decision, **kwargs):
    return asyncio.run(generate_question(llm, decision, _log(), **kwargs))


def test_follow_up_uses_current_branch_and_direction_block():
    llm = _StubLLM("구현하면서 가장 어려웠던 부분은 무엇이었나요?")
    result = _generate(llm, _deepen())
    assert not result.is_fallback
    assert result.text == "구현하면서 가장 어려웠던 부분은 무엇이었나요?"

    items = llm.calls[0]["chat_ctx"].items
    assert items[0].role == "system"
    instruction = items[-1].text_content
    assert "이유를 파고드세요" in instruction
    assert "파고들 지점: 구현 세부가 궁금함" in instruction
    # 현재 줄기(협업)만 role 메시지로 — 이전 줄기(결제)는 미포함
    conversation = [item.text_content for item in items[1:-1]]
    assert "협업 경험을 들려주세요." in conversation
    assert "결제 프로젝트를 소개해 주세요." not in conversation


def test_next_topic_carries_resume_and_dedupe_list():
    llm = _StubLLM("잘 들었습니다. 최근에 학습한 기술 이야기를 해볼까요?")
    result = _generate(llm, forced_next_topic(), resume_context="기술: Java, Redis")
    assert not result.is_fallback

    instruction = llm.calls[0]["chat_ctx"].items[-1].text_content
    assert "기술: Java, Redis" in instruction
    assert "- 협업 경험을 들려주세요." in instruction  # 중복 방지 목록


def test_consistency_injects_referenced_branch():
    decision = Decision(
        action=Action.FOLLOW_UP, source=DecisionSource.ORCHESTRATOR,
        follow_up_type=FollowUpType.CONSISTENCY, reason="역할 서술 상충",
        ref_question_number=1,
    )
    llm = _StubLLM("아까는 혼자 설계하셨다고 들었는데, 다시 설명해 주시겠어요?")
    result = _generate(llm, decision)
    assert not result.is_fallback

    instruction = llm.calls[0]["chat_ctx"].items[-1].text_content
    assert "[Q1|root1] 결제 프로젝트를 소개해 주세요." in instruction  # 참조 줄기 주입
    assert "추궁하거나 단정하지" in instruction


def test_abnormal_outputs_fall_back_to_vetted_pool():
    cases = (
        "",  # 빈 출력
        "   \n  ",  # 공백뿐
        "질" * (QUESTION_MAX_CHARS + 1),  # 길이 상한 초과
        "다음 중 하나를 골라주세요.\n- 첫째\n- 둘째",  # 리스트 마커
        "**중요한** 질문입니다.",  # 마크다운
    )
    for text in cases:
        result = _generate(_StubLLM(text), _deepen())
        assert result.is_fallback
        assert result.text in FALLBACK_QUESTIONS


def test_call_failure_falls_back():
    result = _generate(_FailingLLM(), _deepen())
    assert result.is_fallback
    assert result.text in FALLBACK_QUESTIONS


def test_normal_output_is_trimmed():
    result = _generate(_StubLLM("  왜 그렇게 판단하셨나요?  \n"), _deepen())
    assert result.text == "왜 그렇게 판단하셨나요?"
