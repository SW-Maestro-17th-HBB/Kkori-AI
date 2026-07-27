"""마무리 단계 Orchestrator 판단 단위 테스트 — 스키마 분기·폴백. docs/prd/interview-end.md §2.

스텁 LLM은 test_orchestrator와 같은 패턴 — 실호출 없이 스트림 형태만 흉내낸다.
"""

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from src.interview.conversation_log import Action, ConversationLog, QuestionType
from src.interview.orchestrator import Decision, DecisionSource, decide

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

    def chat(self, **kwargs):
        return _StubStream(self._text)


class _FailingLLM:
    def chat(self, **kwargs):
        raise RuntimeError("boom")


def _log() -> ConversationLog:
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.TOPIC, content="협업 경험을 들려주세요.", spoken_at=NOW,
    )
    log.append_answer("팀에서 백엔드를 맡아 구현했습니다.", NOW)
    return log


def _decide(payload, *, minutes: int | None = 4) -> Decision:
    text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return asyncio.run(
        decide(_StubLLM(text), _log(), wrap_up_remaining_minutes=minutes)
    )


def test_final_question_action_is_parsed_in_wrap_up():
    decision = _decide({"reason": "주제가 소진됨", "action": "FINAL_QUESTION"})
    assert decision.action is Action.FINAL_QUESTION
    assert decision.source is DecisionSource.ORCHESTRATOR
    assert decision.reason == "주제가 소진됨"


def test_end_action_is_parsed_in_wrap_up():
    decision = _decide({"reason": "시간이 소진됨", "action": "END"})
    assert decision.action is Action.END
    assert decision.source is DecisionSource.ORCHESTRATOR
    assert decision.reason == "시간이 소진됨"


def test_follow_up_still_allowed_in_wrap_up():
    decision = _decide(
        {"reason": "구체적 경험 언급", "action": "FOLLOW_UP", "followUpType": "DEEPEN"}
    )
    assert decision.action is Action.FOLLOW_UP


def test_next_topic_in_wrap_up_falls_back_to_final_question():
    # 마무리 단계 스키마에는 NEXT_TOPIC이 없다 — 검증 실패 → FINAL_QUESTION 폴백
    decision = _decide({"reason": "새 주제로 전환", "action": "NEXT_TOPIC"})
    assert decision.action is Action.FINAL_QUESTION
    assert decision.source is DecisionSource.FALLBACK


def test_wrap_up_actions_outside_wrap_up_fall_back_to_next_topic():
    # 비마무리 단계 스키마에는 FINAL_QUESTION·END가 없다 — 강등은 스키마가 수행
    decision = _decide({"reason": "시간이 소진됨", "action": "END"}, minutes=None)
    assert decision.action is Action.NEXT_TOPIC
    assert decision.source is DecisionSource.FALLBACK


def test_call_failure_in_wrap_up_falls_back_to_final_question():
    decision = asyncio.run(decide(_FailingLLM(), _log(), wrap_up_remaining_minutes=1))
    assert decision.action is Action.FINAL_QUESTION
    assert decision.source is DecisionSource.FALLBACK


def test_empty_reason_in_wrap_up_falls_back_to_final_question():
    decision = _decide({"reason": "   ", "action": "END"})
    assert decision.action is Action.FINAL_QUESTION
    assert decision.source is DecisionSource.FALLBACK
