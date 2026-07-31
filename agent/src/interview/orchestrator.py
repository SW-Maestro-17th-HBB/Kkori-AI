"""답변 평가·액션 판단 (Orchestrator) — 구조화 출력. docs/prd/follow-up-question.md §2.

LLM 응답 스키마(OrchestratorDecision)와 내부 결정 모델(Decision)을 분리한다 —
source(판단 경로)는 코드만 정하며, reason 존재 불변식("Orchestrator 판단 경로에만
존재")이 기계적으로 보장된다. 어떤 실패도 파이프라인을 멈추지 않는다: 구조화 실패·
미정의 값·호출 실패는 NEXT_TOPIC 폴백, CONSISTENCY의 ref 무효는 DEEPEN 강등.
답변 원문·reason은 운영 로그에 남기지 않는다(액션·유형 enum 값만 기록).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from livekit.agents import llm as agents_llm
from pydantic import BaseModel

from src.config import ORCHESTRATOR_INPUT_TOKEN_BUDGET, UTTERANCE_INJECTION_TOKEN_CAP
from src.interview.context import estimate_tokens, orchestrator_context
from src.interview.conversation_log import Action, ConversationLog, FollowUpType
from src.interview.llm_stream import collect_tool_call_arguments
from src.interview.prompts import orchestrator_instructions

logger = logging.getLogger(__name__)

# 구조화 출력·직렬화 오버헤드용 예약 토큰 — 대화 직렬화 예산에서 미리 뺀다
_OUTPUT_RESERVE_TOKENS = 300

# 구조화 출력은 강제 tool 호출로 보장한다 — response_format이 없는 프로바이더
# (Bedrock Converse)에서도 스키마가 tool 파라미터로 강제되는 공통 경로
_DECISION_TOOL_NAME = "save_decision"


def _decision_tool(schema: type[BaseModel]) -> agents_llm.Tool:
    async def _save(raw_arguments: dict) -> str:  # 실행되지 않는다 — 스키마 강제 전용
        return ""

    return agents_llm.function_tool(
        _save,
        raw_schema={
            "name": _DECISION_TOOL_NAME,
            "description": "면접 답변 평가 결과를 저장한다.",
            "parameters": schema.model_json_schema(),
        },
    )


class OrchestratorDecision(BaseModel):
    """LLM 응답 스키마 — reason을 먼저 서술하게 해 판단 품질을 높인다(필드 순서 유지)."""

    reason: str
    action: Literal["FOLLOW_UP", "NEXT_TOPIC"]
    followUpType: (
        Literal["DEEPEN", "CONCRETE", "VERIFY", "BOUNDARY", "CONSISTENCY"] | None
    ) = None
    refQuestionNumber: int | None = None


class WrapUpOrchestratorDecision(BaseModel):
    """마무리 단계 응답 스키마 — NEXT_TOPIC이 빠지고 FINAL_QUESTION·END가 들어간다.

    액션 집합은 스키마가 강제한다(docs/prd/interview-end.md §2) — 비허용 액션은
    검증 실패로 폴백에 수렴하므로 별도 강등 로직이 필요 없다.
    """

    reason: str
    action: Literal["FOLLOW_UP", "FINAL_QUESTION", "END"]
    followUpType: (
        Literal["DEEPEN", "CONCRETE", "VERIFY", "BOUNDARY", "CONSISTENCY"] | None
    ) = None
    refQuestionNumber: int | None = None


class DecisionSource(StrEnum):
    ORCHESTRATOR = "orchestrator"
    FORCED = "forced"  # 코드 강제 (첫 답변·상한)
    FALLBACK = "fallback"  # 구조화 실패·호출 실패


@dataclass(frozen=True)
class Decision:
    """내부 결정 모델 — reason은 ORCHESTRATOR source에만 존재한다."""

    action: Action
    source: DecisionSource
    follow_up_type: FollowUpType | None = None
    reason: str | None = None
    ref_question_number: int | None = None


def forced_next_topic() -> Decision:
    return Decision(action=Action.NEXT_TOPIC, source=DecisionSource.FORCED)


def fallback_next_topic() -> Decision:
    return Decision(action=Action.NEXT_TOPIC, source=DecisionSource.FALLBACK)


def forced_final_question() -> Decision:
    """마무리 단계의 코드 강제 — 상한 도달·주입 경계 예외가 수렴한다."""
    return Decision(action=Action.FINAL_QUESTION, source=DecisionSource.FORCED)


def fallback_final_question() -> Decision:
    """마무리 단계의 폴백 — 마지막 질문은 직전 답변 평가에 의존하지 않아 항상 안전하다."""
    return Decision(action=Action.FINAL_QUESTION, source=DecisionSource.FALLBACK)


async def decide(
    llm: agents_llm.LLM,
    log: ConversationLog,
    *,
    wrap_up_remaining_minutes: int | None = None,
) -> Decision:
    """직전 답변을 평가해 다음 액션을 결정한다.

    비마무리 단계 실패는 NEXT_TOPIC, 마무리 단계(wrap_up_remaining_minutes 존재)
    실패는 FINAL_QUESTION으로 폴백한다 — 각 단계에서 평가 불요·항상 안전한 방향.
    """
    wrap_up = wrap_up_remaining_minutes is not None
    schema = WrapUpOrchestratorDecision if wrap_up else OrchestratorDecision
    fallback = fallback_final_question if wrap_up else fallback_next_topic
    fallback_label = "FINAL_QUESTION" if wrap_up else "NEXT_TOPIC"
    reserve = (
        estimate_tokens(
            orchestrator_instructions("", wrap_up_remaining_minutes=wrap_up_remaining_minutes)
        )
        + _OUTPUT_RESERVE_TOKENS
    )
    conversation = orchestrator_context(
        log,
        token_budget=max(ORCHESTRATOR_INPUT_TOKEN_BUDGET - reserve, 1),
        utterance_token_cap=UTTERANCE_INJECTION_TOKEN_CAP,
    )
    try:
        chat_ctx = agents_llm.ChatContext.empty()
        chat_ctx.add_message(
            role="user",
            content=orchestrator_instructions(
                conversation, wrap_up_remaining_minutes=wrap_up_remaining_minutes
            ),
        )
        arguments = await collect_tool_call_arguments(
            llm, chat_ctx, tool=_decision_tool(schema), tool_name=_DECISION_TOOL_NAME
        )
        parsed = schema.model_validate_json(arguments)
    except Exception as exc:
        # 예외 객체를 기록하지 않는다 — ValidationError의 input_value 등에
        # 답변 원문·개인정보가 그대로 담길 수 있다 (타입명만)
        logger.warning(
            "Orchestrator 호출·구조화 파싱 실패(%s) — %s 폴백",
            type(exc).__name__,
            fallback_label,
        )
        return fallback()

    reason = parsed.reason.strip()
    if not reason:
        # "한 문장 판단 근거" 계약 위반 — reason 불변식을 지키기 위해 판단 자체를 폐기
        logger.warning("Orchestrator reason이 비어 있음 — %s 폴백", fallback_label)
        return fallback()

    if parsed.action == "NEXT_TOPIC":
        return Decision(
            action=Action.NEXT_TOPIC,
            source=DecisionSource.ORCHESTRATOR,
            reason=reason,
        )

    if parsed.action == "FINAL_QUESTION":
        return Decision(
            action=Action.FINAL_QUESTION,
            source=DecisionSource.ORCHESTRATOR,
            reason=reason,
        )

    if parsed.action == "END":
        return Decision(
            action=Action.END,
            source=DecisionSource.ORCHESTRATOR,
            reason=reason,
        )

    if parsed.followUpType is None:
        logger.warning("FOLLOW_UP인데 followUpType 없음 — %s 폴백", fallback_label)
        return fallback()

    follow_up_type = FollowUpType(parsed.followUpType)
    ref = parsed.refQuestionNumber
    if follow_up_type is FollowUpType.CONSISTENCY and not _is_valid_ref(log, ref):
        # 근거 없이 상충을 언급하는 환각 차단 — reason은 유지한 채 DEEPEN으로 강등 (PRD §2)
        logger.warning("CONSISTENCY ref 무효(%s) — DEEPEN 강등", "부재" if ref is None else ref)
        follow_up_type = FollowUpType.DEEPEN
        ref = None
    elif follow_up_type is not FollowUpType.CONSISTENCY:
        ref = None

    return Decision(
        action=Action.FOLLOW_UP,
        source=DecisionSource.ORCHESTRATOR,
        follow_up_type=follow_up_type,
        reason=reason,
        ref_question_number=ref,
    )


def _is_valid_ref(log: ConversationLog, ref: int | None) -> bool:
    """상충 참조는 실제로 존재하는, 현재 줄기 밖의 질문이어야 한다."""
    if ref is None:
        return False
    question = log.question_for(ref)
    if question is None:
        return False
    return question.parent_question_number != log.current_root()
