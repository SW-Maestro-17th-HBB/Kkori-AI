"""초기 질문 선택·폴백 테스트 — LLM 없이 스텁으로 검증. docs/prd/interview.md §2."""

import asyncio
from types import SimpleNamespace

from src.interview.initial_question import (
    _parse_selection,
    initial_utterance,
    select_initial_question,
)
from src.interview.prompts import INITIAL_GREETING, question_pool


class _StubStream:
    def __init__(self, text: str) -> None:
        self._chunks = iter([SimpleNamespace(delta=SimpleNamespace(content=text))])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._chunks)
        except StopIteration:
            raise StopAsyncIteration


class _StubLLM:
    """정해진 텍스트를 출력하는 가짜 LLM (worker의 fake provider 패턴)."""

    def __init__(self, text: str) -> None:
        self._text = text

    def chat(self, **kwargs):
        return _StubStream(self._text)


class _FailingLLM:
    def chat(self, **kwargs):
        raise RuntimeError("boom")


def test_parse_selection_accepts_number_variants():
    assert _parse_selection("3", 8) == 2
    assert _parse_selection(" 3.", 8) == 2
    assert _parse_selection("3번이 적절합니다", 8) == 2


def test_parse_selection_rejects_invalid_output():
    assert _parse_selection("", 8) is None
    assert _parse_selection("자기소개가 좋겠습니다", 8) is None
    assert _parse_selection("0", 8) is None
    assert _parse_selection("9", 8) is None


def test_initial_utterance_is_greeting_plus_question():
    question = question_pool("백엔드")[0]
    utterance = initial_utterance(question)
    assert utterance == f"{INITIAL_GREETING} {question}"


def test_valid_selection_returns_exact_pool_question():
    question = asyncio.run(select_initial_question(_StubLLM("4"), position="백엔드"))
    assert question == question_pool("백엔드")[3]


def test_out_of_pool_output_falls_back_to_pool():
    # LLM이 목록 밖 질문을 지어내도 발화는 목록 원문으로 강제된다
    question = asyncio.run(
        select_initial_question(_StubLLM("학교를 선택한 이유가 무엇인가요? 9"), position="백엔드")
    )
    assert question in question_pool("백엔드")


def test_non_numeric_output_falls_back_to_pool():
    question = asyncio.run(select_initial_question(_StubLLM("자기소개요"), position=None))
    assert question in question_pool(None)


def test_llm_failure_falls_back_to_pool():
    question = asyncio.run(select_initial_question(_FailingLLM(), position="백엔드"))
    assert question in question_pool("백엔드")
