"""Orchestrator 판단 단위 테스트 — 구조화 파싱·폴백·강등. docs/prd/follow-up-question.md §2.

스텁 LLM은 worker의 fake provider 패턴을 따른다 — 실호출 없이 스트림 형태만 흉내낸다.
"""

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from src.interview.conversation_log import Action, ConversationLog, FollowUpType, QuestionType
from src.interview.orchestrator import Decision, DecisionSource, OrchestratorDecision, decide

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
    """줄기 1(주제) → 줄기 2(주제, 현재) — ref=1은 현재 줄기 밖의 유효 참조."""
    log = ConversationLog()
    log.append_question(
        question_number=1, parent_question_number=1,
        question_type=QuestionType.TOPIC, content="결제 프로젝트를 소개해 주세요.", spoken_at=NOW,
    )
    log.append_answer("혼자 설계하고 구현했습니다.", NOW)
    log.append_question(
        question_number=2, parent_question_number=2,
        question_type=QuestionType.TOPIC, content="협업 경험을 들려주세요.", spoken_at=NOW,
    )
    log.append_answer("팀 리드가 설계한 걸 제가 구현했습니다.", NOW)
    return log


def _decide(payload, log: ConversationLog | None = None) -> Decision:
    text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return asyncio.run(decide(_StubLLM(text), log or _log()))


def test_valid_follow_up_decision_is_parsed():
    decision = _decide(
        {"reason": "설계 근거 설명이 없음", "action": "FOLLOW_UP", "followUpType": "DEEPEN"}
    )
    assert decision.action is Action.FOLLOW_UP
    assert decision.follow_up_type is FollowUpType.DEEPEN
    assert decision.source is DecisionSource.ORCHESTRATOR
    assert decision.reason == "설계 근거 설명이 없음"
    assert decision.ref_question_number is None


def test_valid_next_topic_decision_keeps_reason():
    decision = _decide({"reason": "주제 소진", "action": "NEXT_TOPIC"})
    assert decision.action is Action.NEXT_TOPIC
    assert decision.source is DecisionSource.ORCHESTRATOR
    assert decision.reason == "주제 소진"


def test_malformed_json_falls_back_without_reason():
    decision = _decide("그냥 꼬리질문 하세요")
    assert decision.action is Action.NEXT_TOPIC
    assert decision.source is DecisionSource.FALLBACK
    assert decision.reason is None


def test_unknown_action_falls_back():
    decision = _decide({"reason": "r", "action": "CLARIFY"})
    assert decision.source is DecisionSource.FALLBACK


def test_parse_failure_log_never_contains_answer_content(caplog):
    # malformed JSON이면 ValidationError의 input_value에 원문 전체(민감 값 포함)가
    # 담긴다 — exc_info 기록 시 그대로 새는 버그의 정확한 재현 픽스처
    sensitive = '{"reason":"990101-1234567","action":'
    with caplog.at_level("WARNING"):
        decision = _decide(sensitive)
    assert decision.source is DecisionSource.FALLBACK
    assert "990101-1234567" not in caplog.text


def test_blank_reason_falls_back():
    for reason in ("", "   "):
        decision = _decide({"reason": reason, "action": "FOLLOW_UP", "followUpType": "DEEPEN"})
        assert decision.source is DecisionSource.FALLBACK
        assert decision.reason is None


def test_reason_is_normalized():
    decision = _decide({"reason": "  근거입니다  ", "action": "NEXT_TOPIC"})
    assert decision.reason == "근거입니다"


def test_follow_up_without_type_falls_back():
    decision = _decide({"reason": "r", "action": "FOLLOW_UP"})
    assert decision.action is Action.NEXT_TOPIC
    assert decision.source is DecisionSource.FALLBACK


def test_llm_failure_falls_back():
    decision = asyncio.run(decide(_FailingLLM(), _log()))
    assert decision.action is Action.NEXT_TOPIC
    assert decision.source is DecisionSource.FALLBACK


class _HangingStream:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(3600)


class _HangingLLM:
    def chat(self, **kwargs):
        return _HangingStream()


def test_hanging_llm_times_out_to_fallback(monkeypatch):
    # 스트림이 매달리면 턴이 침묵으로 고정된다 — 타임아웃이 폴백 경로로 회수해야 한다
    import src.interview.llm_stream as llm_stream

    monkeypatch.setattr(llm_stream, "LLM_CALL_TIMEOUT_SECONDS", 0.05)
    decision = asyncio.run(decide(_HangingLLM(), _log()))
    assert decision.source is DecisionSource.FALLBACK


def test_consistency_with_valid_ref_is_kept():
    decision = _decide(
        {
            "reason": "역할 서술이 상충",
            "action": "FOLLOW_UP",
            "followUpType": "CONSISTENCY",
            "refQuestionNumber": 1,
        }
    )
    assert decision.follow_up_type is FollowUpType.CONSISTENCY
    assert decision.ref_question_number == 1


def test_consistency_ref_in_current_branch_demotes_to_deepen():
    decision = _decide(
        {
            "reason": "상충",
            "action": "FOLLOW_UP",
            "followUpType": "CONSISTENCY",
            "refQuestionNumber": 2,  # 현재 줄기 루트 — 무효
        }
    )
    assert decision.follow_up_type is FollowUpType.DEEPEN
    assert decision.ref_question_number is None
    assert decision.reason == "상충"  # 강등해도 reason은 유지


def test_consistency_ref_missing_or_unknown_demotes_to_deepen():
    for ref in (None, 99):
        payload = {
            "reason": "상충", "action": "FOLLOW_UP", "followUpType": "CONSISTENCY",
        }
        if ref is not None:
            payload["refQuestionNumber"] = ref
        assert _decide(payload).follow_up_type is FollowUpType.DEEPEN


def test_stray_ref_on_non_consistency_is_dropped():
    decision = _decide(
        {
            "reason": "r", "action": "FOLLOW_UP", "followUpType": "VERIFY",
            "refQuestionNumber": 1,
        }
    )
    assert decision.follow_up_type is FollowUpType.VERIFY
    assert decision.ref_question_number is None


def test_chat_is_called_with_response_format_schema():
    llm = _StubLLM(json.dumps({"reason": "r", "action": "NEXT_TOPIC"}))
    asyncio.run(decide(llm, _log()))
    assert llm.calls[0]["response_format"] is OrchestratorDecision
    prompt = llm.calls[0]["chat_ctx"].items[-1].text_content
    assert "[Q1|root1]" in prompt  # 번호 태그 직렬화가 지시에 포함된다
