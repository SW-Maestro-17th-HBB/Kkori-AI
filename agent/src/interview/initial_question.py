"""초기 질문 선택 — LLM은 번호만 고르고 발화는 목록 원문으로 조립한다. docs/prd/interview.md §2.

LLM 출력이 유효한 번호가 아니거나 호출이 실패해도 목록에서 랜덤 폴백하므로,
발화되는 질문은 어떤 경우에도 검수된 목록 원문이다(목록 밖 질문·요약 내용 유입 차단).
"""

from __future__ import annotations

import logging
import random
import re

from livekit.agents import llm as agents_llm

from src.interview.prompts import INITIAL_GREETING, _question_pool, selection_instructions

logger = logging.getLogger(__name__)


def _parse_selection(text: str, pool_size: int) -> int | None:
    """LLM 출력에서 질문 번호를 파싱해 0-based 인덱스로 반환한다. 유효하지 않으면 None."""
    match = re.search(r"\d+", text)
    if match is None:
        return None
    number = int(match.group())
    if not 1 <= number <= pool_size:
        return None
    return number - 1


def initial_utterance(question: str) -> str:
    """고정 인사말 + 선택된 질문으로 첫 발화를 조립한다."""
    return f"{INITIAL_GREETING} {question}"


async def select_initial_question(
    llm: agents_llm.LLM, *, position: str | None = None, resume_context: str | None = None
) -> str:
    """목록에서 초기 질문 하나를 고른다. 항상 목록 원문을 반환한다."""
    pool = _question_pool(position)
    try:
        chat_ctx = agents_llm.ChatContext.empty()
        chat_ctx.add_message(
            role="user",
            content=selection_instructions(position=position, resume_context=resume_context),
        )
        text = ""
        async with llm.chat(chat_ctx=chat_ctx) as stream:
            async for chunk in stream:
                if chunk.delta and chunk.delta.content:
                    text += chunk.delta.content
    except Exception:
        logger.exception("초기 질문 선택 LLM 호출 실패 — 목록에서 랜덤 폴백")
        return random.choice(pool)

    index = _parse_selection(text, len(pool))
    if index is None:
        # 출력 원문은 기록하지 않는다 — 모델이 요약 내용을 되돌려줄 수 있음 (PRD §1 기타 요구사항)
        logger.warning("질문 번호 파싱 실패 — 목록에서 랜덤 폴백 (출력 길이 %d자)", len(text))
        return random.choice(pool)
    return pool[index]
