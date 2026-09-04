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


async def collect_tool_call_arguments(
    llm: agents_llm.LLM,
    chat_ctx: agents_llm.ChatContext,
    *,
    tool: agents_llm.Tool,
    tool_name: str,
    timeout_seconds: float | None = None,
) -> str:
    """단일 tool을 강제 호출시켜 인자 JSON 문자열을 수집한다.

    구조화 출력을 프로바이더 공통으로 보장하는 경로 — response_format이 없는
    Bedrock(Converse)에서도 스키마가 tool 파라미터로 강제된다. 인자가 여러
    프래그먼트로 나뉘어 올 수 있어 이름이 일치하는 호출의 인자를 이어 붙인다.
    """
    if timeout_seconds is None:
        timeout_seconds = LLM_CALL_TIMEOUT_SECONDS

    async def _collect() -> str:
        arguments = ""
        async with llm.chat(
            chat_ctx=chat_ctx,
            tools=[tool],
            tool_choice={"type": "function", "function": {"name": tool_name}},
        ) as stream:
            async for chunk in stream:
                if chunk.delta is None:
                    continue
                for call in chunk.delta.tool_calls or []:
                    if call.name == tool_name and call.arguments:
                        arguments += call.arguments
        return arguments

    return await asyncio.wait_for(_collect(), timeout=timeout_seconds)
