"""초기 질문 텍스트 미리보기 — 음성 없이 LLM 레벨에서 프롬프트를 검증한다.

실행 (agent 디렉토리에서):
    uv run python scripts/preview_initial_question.py

환경 변수:
    KKORI_POSITION_FIXTURE        지원 직무 (기본: 백엔드, 빈 문자열이면 미지정)
    KKORI_RESUME_CONTEXT_FIXTURE  이력서 요약 (미설정이면 요약 없이 실행)
    KKORI_PREVIEW_N               샘플 수 (기본: 3)

LiveKit Inference를 실제 호출하므로 agent/.env의 LiveKit 자격증명이 필요하다.
"""

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv()

from livekit.agents import inference
from livekit.agents.llm import ChatContext

from src.main import LLM_MODEL
from src.interview.prompts import INTERVIEWER_INSTRUCTIONS, initial_question_instructions


async def sample(llm: inference.LLM, position: str | None, resume_context: str | None) -> str:
    chat_ctx = ChatContext.empty()
    chat_ctx.add_message(role="system", content=INTERVIEWER_INSTRUCTIONS)
    chat_ctx.add_message(
        role="user",
        content=initial_question_instructions(position=position, resume_context=resume_context),
    )
    text = ""
    async with llm.chat(chat_ctx=chat_ctx) as stream:
        async for chunk in stream:
            if chunk.delta and chunk.delta.content:
                text += chunk.delta.content
    return text.strip()


async def main() -> None:
    position = os.getenv("KKORI_POSITION_FIXTURE", "백엔드") or None
    resume_context = os.getenv("KKORI_RESUME_CONTEXT_FIXTURE") or None
    n = int(os.getenv("KKORI_PREVIEW_N", "3"))

    print(f"model={LLM_MODEL} / position={position} / 요약={'있음' if resume_context else '없음'}")
    llm = inference.LLM(model=LLM_MODEL)
    try:
        for i in range(n):
            print(f"--- #{i + 1}")
            print(await sample(llm, position, resume_context), "\n")
    finally:
        await llm.aclose()


if __name__ == "__main__":
    asyncio.run(main())
