"""초기 질문 LLM 스모크 테스트 — LiveKit Inference 실호출.

자격증명(LIVEKIT_*)이 없으면 전체 skip 된다(CI 포함 — worker의 인프라 없음 skip 관례와 동일).
어투 다듬기를 허용하므로 정확 일치 대신 유형별 키워드로 목록 일치를 판별한다.
"""

import asyncio
import os

import pytest

_REQUIRED_ENV = ("LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET")

pytestmark = pytest.mark.skipif(
    any(not os.getenv(key) for key in _REQUIRED_ENV),
    reason="LiveKit 자격증명 없음 — LLM 스모크 테스트는 로컬에서 실행",
)

POSITION = "백엔드"
RESUME_CONTEXT = (
    "역할: 백엔드 (프로젝트: Kkori 결제 시스템, 채팅 서버) / "
    "기술: Java, Spring, Redis / 수상: 교내 해커톤 대상"
)
SAMPLE_COUNT = 6

# 목록 유형별 매칭 키워드 (docs/prd/interview.md §2 — 자기소개/지원동기/경험개괄/강점)
_CATEGORY_KEYWORDS = {
    "자기소개": ("소개",),
    "지원동기": ("계기", "진로", "이유"),
    "경험개괄": ("프로젝트", "몰입", "작업"),
    "강점": ("강점",),
}

# 이력서 요약의 세부 경험 — 질문에 언급되면 안 된다 (요약은 선택 판단 재료로만 사용)
_RESUME_DETAILS = ("결제", "채팅", "해커톤", "Java", "Spring", "Redis")


def _matched_categories(text: str) -> set[str]:
    return {
        category
        for category, keywords in _CATEGORY_KEYWORDS.items()
        if any(keyword in text for keyword in keywords)
    }


async def _sample(position: str | None, resume_context: str | None) -> str:
    from livekit.agents import inference
    from livekit.agents.llm import ChatContext

    from src.main import LLM_MODEL
    from src.interview.prompts import INTERVIEWER_INSTRUCTIONS, initial_question_instructions

    llm = inference.LLM(model=LLM_MODEL)
    try:
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
    finally:
        await llm.aclose()


@pytest.fixture(scope="module")
def samples() -> list[str]:
    async def collect() -> list[str]:
        return list(
            await asyncio.gather(
                *(_sample(POSITION, RESUME_CONTEXT) for _ in range(SAMPLE_COUNT))
            )
        )

    return asyncio.run(collect())


def test_question_matches_pool_category(samples):
    for text in samples:
        assert _matched_categories(text), f"목록 유형과 매칭되지 않는 발화: {text}"


def test_no_resume_detail_leak(samples):
    for text in samples:
        leaked = [d for d in _RESUME_DETAILS if d in text]
        assert not leaked, f"이력서 세부 경험 유출 {leaked}: {text}"


def test_voice_friendly_format(samples):
    for text in samples:
        assert len(text) <= 250, f"발화가 너무 길다({len(text)}자): {text}"
        assert not any(marker in text for marker in ("**", "```", "#", "- ")), (
            f"plain text 위반: {text}"
        )


def test_selection_diversity(samples):
    categories = set().union(*(_matched_categories(t) for t in samples))
    assert len(categories) >= 2, f"선택이 한 유형에 고착됨: {categories}"


def test_fallback_without_context():
    text = asyncio.run(_sample(None, None))
    assert POSITION not in text, f"직무 미지정인데 직무 언급: {text}"
    assert _matched_categories(text), f"목록 유형과 매칭되지 않는 발화: {text}"
