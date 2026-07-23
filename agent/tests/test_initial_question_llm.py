"""초기 질문 LLM 스모크 테스트 — LiveKit Inference 실호출(유료).

기본은 skip이며, KKORI_LIVE_LLM=1 + LiveKit 자격증명이 있을 때만 실행된다
(일반 `uv run pytest`가 자동으로 과금 호출을 하지 않도록 명시적 opt-in).

    KKORI_LIVE_LLM=1 uv run pytest tests/test_initial_question_llm.py

선택 결과는 목록 원문과 정확 일치를 검증한다. 선택 분포(다양성)는 확률적 특성상
게이트로 두지 않고 scripts/preview_initial_question.py로 수동 관찰한다.
"""

import asyncio
import os

import pytest

_REQUIRED_ENV = ("LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET")

pytestmark = pytest.mark.skipif(
    os.getenv("KKORI_LIVE_LLM") != "1" or any(not os.getenv(key) for key in _REQUIRED_ENV),
    reason="KKORI_LIVE_LLM=1 + LiveKit 자격증명 설정 시에만 실호출",
)

POSITION = "백엔드"
RESUME_CONTEXT = (
    "역할: 백엔드 (프로젝트: Kkori 결제 시스템, 채팅 서버) / "
    "기술: Java, Spring, Redis / 수상: 교내 해커톤 대상"
)


async def _select(position: str | None, resume_context: str | None) -> str:
    from livekit.agents import inference

    from src.config import LLM_MODEL
    from src.interview.initial_question import select_initial_question

    llm = inference.LLM(model=LLM_MODEL)
    try:
        return await select_initial_question(
            llm, position=position, resume_context=resume_context
        )
    finally:
        await llm.aclose()


def test_selected_question_is_exactly_from_pool():
    from src.interview.prompts import question_pool

    async def collect() -> list[str]:
        return list(
            await asyncio.gather(*(_select(POSITION, RESUME_CONTEXT) for _ in range(3)))
        )

    for question in asyncio.run(collect()):
        assert question in question_pool(POSITION)


def test_fallback_without_context_selects_from_neutral_pool():
    from src.interview.prompts import question_pool

    question = asyncio.run(_select(None, None))
    assert question in question_pool(None)
