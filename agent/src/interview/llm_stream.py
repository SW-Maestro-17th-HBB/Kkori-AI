"""LLM 스트림 수집 공통 헬퍼 — 타임아웃 포함.

Orchestrator·Interview 호출이 같은 수집 패턴을 공유한다. 스트림이 응답 없이
매달리면 턴이 침묵으로 고정되므로(폴백조차 못 탄다), 호출 전체에 상한을 건다 —
초과 시 TimeoutError가 각 호출자의 폴백 경로로 흘러간다.
"""

from __future__ import annotations

import asyncio

from livekit.agents import llm as agents_llm

from src.config import LLM_CALL_TIMEOUT_SECONDS


async def collect_chat_text(
    llm: agents_llm.LLM,
    chat_ctx: agents_llm.ChatContext,
    *,
    timeout_seconds: float | None = None,
    **chat_kwargs,
) -> str:
    if timeout_seconds is None:
        timeout_seconds = LLM_CALL_TIMEOUT_SECONDS

    async def _collect() -> str:
        text = ""
        async with llm.chat(chat_ctx=chat_ctx, **chat_kwargs) as stream:
            async for chunk in stream:
                if chunk.delta and chunk.delta.content:
                    text += chunk.delta.content
        return text

    return await asyncio.wait_for(_collect(), timeout=timeout_seconds)
