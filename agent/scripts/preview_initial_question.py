"""초기 질문 선택 미리보기 — 음성 없이 선택 결과·분포를 수동 관찰한다.

선택 다양성은 확률적 특성상 자동 테스트 게이트가 아니라 이 스크립트로 관찰한다
(docs/prd/interview.md §2 검증 기준).

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

from src.config import BEDROCK_LLM_MODEL, LLM_MODEL
from src.interview.initial_question import initial_utterance, select_initial_question
from src.llm_factory import build_llm


async def main() -> None:
    position = os.getenv("KKORI_POSITION_FIXTURE", "백엔드") or None
    resume_context = os.getenv("KKORI_RESUME_CONTEXT_FIXTURE") or None
    n = int(os.getenv("KKORI_PREVIEW_N", "3"))

    llm = build_llm(LLM_MODEL, BEDROCK_LLM_MODEL)
    print(f"llm={llm.model} / position={position} / 요약={'있음' if resume_context else '없음'}")
    counts: dict[str, int] = {}
    try:
        for i in range(n):
            question = await select_initial_question(
                llm, position=position, resume_context=resume_context
            )
            counts[question] = counts.get(question, 0) + 1
            print(f"--- #{i + 1}")
            print(initial_utterance(question), "\n")
    finally:
        await llm.aclose()

    print("=== 선택 분포")
    for question, count in sorted(counts.items(), key=lambda item: -item[1]):
        print(f"{count}회  {question}")


if __name__ == "__main__":
    asyncio.run(main())
