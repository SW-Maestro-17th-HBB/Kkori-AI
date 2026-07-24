"""본론 질문 생성 (Interview) — 액션·유형별 분기와 출력 검증. docs/prd/follow-up-question.md §3.

본론 질문은 직전 답변·대화 맥락에 의존하므로 LLM 자유 생성이다. 품질은 프롬프트
규칙과 출력 검증으로 확보한다 — 호출 실패뿐 아니라 정상 호출의 비정상 출력(빈 문자열·
길이 상한 초과·금지 형식)도 검수된 폴백 질문으로 대체해 발화 공백을 막는다.
LLM 출력 원문은 운영 로그에 남기지 않는다(길이만).
"""

from __future__ import annotations

import logging
import random
import re
from dataclasses import dataclass

from livekit.agents import llm as agents_llm

from src.config import (
    QUESTION_MAX_CHARS,
    RECENT_BRANCHES_FOR_TOPIC,
    UTTERANCE_INJECTION_TOKEN_CAP,
)
from src.interview.context import branch_text, follow_up_messages, recent_branch_messages
from src.interview.conversation_log import Action, ConversationLog, FollowUpType
from src.interview.llm_stream import collect_chat_text
from src.interview.orchestrator import Decision
from src.interview.prompts import (
    FALLBACK_QUESTIONS,
    INTERVIEWER_INSTRUCTIONS,
    follow_up_instructions,
    next_topic_instructions,
)

logger = logging.getLogger(__name__)

# 마크다운 조각 — 링크(]()까지 포함
_FORBIDDEN_MARKERS = ("#", "*", "`", "•", "](")

# 줄 시작의 목록·인용 마커 — CommonMark 불릿(-, +, *), 순서 목록(1. / 1)), 인용(>)
_LIST_LINE = re.compile(r"^(?:[-+*]|\d+[.)]|>)\s")


@dataclass(frozen=True)
class GeneratedQuestion:
    text: str
    is_fallback: bool  # True면 NEXT_TOPIC 취급(parent=self 새 루트)·reason 미기록


async def generate_question(
    llm: agents_llm.LLM,
    decision: Decision,
    log: ConversationLog,
    *,
    resume_context: str | None = None,
) -> GeneratedQuestion:
    """액션·유형별 프롬프트로 질문 텍스트를 생성한다. 실패·비정상 출력은 폴백."""
    chat_ctx = agents_llm.ChatContext.empty()
    chat_ctx.add_message(role="system", content=INTERVIEWER_INSTRUCTIONS)

    if decision.action is Action.FOLLOW_UP:
        messages = follow_up_messages(
            log, utterance_token_cap=UTTERANCE_INJECTION_TOKEN_CAP
        )
        instruction = follow_up_instructions(
            decision.follow_up_type,
            reason=decision.reason,
            ref_branch_text=_ref_branch_text(log, decision),
        )
    else:
        messages = recent_branch_messages(
            log,
            n=RECENT_BRANCHES_FOR_TOPIC,
            utterance_token_cap=UTTERANCE_INJECTION_TOKEN_CAP,
        )
        instruction = next_topic_instructions(
            resume_context=resume_context,
            previous_questions=log.all_question_contents(),
            reason=decision.reason,
        )

    for role, content in messages:
        chat_ctx.add_message(role=role, content=content)
    chat_ctx.add_message(role="user", content=instruction)

    try:
        text = await collect_chat_text(llm, chat_ctx)
    except Exception as exc:
        # 예외 객체는 기록하지 않는다 — 요청 페이로드(답변 원문)가 담길 수 있다
        logger.warning("Interview 호출 실패(%s) — 폴백 질문 발화", type(exc).__name__)
        return _fallback()

    question = text.strip()
    if not question:
        logger.warning("Interview 출력이 비어 있음 — 폴백 질문 발화")
        return _fallback()
    if len(question) > QUESTION_MAX_CHARS:
        logger.warning("Interview 출력 길이 초과(%d자) — 폴백 질문 발화", len(question))
        return _fallback()
    if _has_forbidden_format(question):
        logger.warning("Interview 출력에 금지 형식 — 폴백 질문 발화")
        return _fallback()
    return GeneratedQuestion(text=question, is_fallback=False)


def _ref_branch_text(log: ConversationLog, decision: Decision) -> str | None:
    """CONSISTENCY 참조 질문이 속한 줄기를 직렬화한다 (Orchestrator가 ref를 검증했음)."""
    if (
        decision.follow_up_type is not FollowUpType.CONSISTENCY
        or decision.ref_question_number is None
    ):
        return None
    referenced = log.question_for(decision.ref_question_number)
    if referenced is None:
        return None
    return branch_text(
        log,
        referenced.parent_question_number,
        utterance_token_cap=UTTERANCE_INJECTION_TOKEN_CAP,
    )


def _has_forbidden_format(text: str) -> bool:
    """음성 발화 불가 형식 — 마크다운·목록·인용 마커 휴리스틱."""
    if any(marker in text for marker in _FORBIDDEN_MARKERS):
        return True
    return any(_LIST_LINE.match(line.lstrip()) for line in text.splitlines())


def _fallback() -> GeneratedQuestion:
    return GeneratedQuestion(text=random.choice(FALLBACK_QUESTIONS), is_fallback=True)
