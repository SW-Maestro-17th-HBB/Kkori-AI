"""턴 흐름 제어 단위 테스트 — single-flight·커밋 규칙·폴백. docs/prd/follow-up-question.md §1.

모든 의존성(판단·생성·발화·전사 writer·shutdown)을 스텁으로 주입해 LiveKit 없이 검증한다.
"""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from src.interview.conversation_log import (
    Action,
    ConversationLog,
    FollowUpType,
    QuestionType,
    Speaker,
)
from src.interview.orchestrator import Decision, DecisionSource
from src.interview.prompts import FALLBACK_QUESTIONS
from src.interview.question_generation import GeneratedQuestion
from src.interview.turn_pipeline import SpeechResult, TurnPipeline

NOW = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)
SPOKE_AT = datetime(2026, 7, 24, 9, 0, 7, tzinfo=timezone.utc)

_DEEPEN = Decision(
    action=Action.FOLLOW_UP, source=DecisionSource.ORCHESTRATOR,
    follow_up_type=FollowUpType.DEEPEN, reason="근거 설명이 없음",
)


class _OrchSpy:
    def __init__(self, decisions=(), gate=None, error=False):
        self.calls = 0
        self._decisions = list(decisions)
        self._gate = gate
        self._error = error

    async def __call__(self, log):
        self.calls += 1
        if self._gate is not None:
            await self._gate.wait()
        if self._error:
            raise RuntimeError("boom")
        return self._decisions.pop(0) if self._decisions else _DEEPEN


class _GenSpy:
    def __init__(self, results=(), error=False):
        self.calls: list[tuple] = []  # (decision, 호출 시점의 발화 수)
        self._results = list(results)
        self._error = error

    async def __call__(self, decision, log):
        self.calls.append((decision, len(log.utterances)))
        if self._error:
            raise RuntimeError("boom")
        if self._results:
            return self._results.pop(0)
        return GeneratedQuestion(text="생성된 질문에 대해 어떻게 생각하세요?", is_fallback=False)


class _SaySpy:
    def __init__(self, results=()):
        self.calls: list[str] = []
        self._results = list(results)

    async def __call__(self, text):
        self.calls.append(text)
        if self._results:
            result = self._results.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        return SpeechResult(ok=True, started_at=SPOKE_AT)


class _WriterSpy:
    def __init__(self):
        self.items: list[dict] = []
        self.closed = False

    def enqueue(self, data):
        self.items.append(data)

    async def aclose(self):
        self.closed = True


def _make(**kwargs):
    env = SimpleNamespace(
        log=ConversationLog(),
        orch=kwargs.pop("orch", _OrchSpy()),
        gen=kwargs.pop("gen", _GenSpy()),
        say=kwargs.pop("say", _SaySpy()),
        writer=_WriterSpy(),
        shutdowns=[],
    )
    env.pipeline = TurnPipeline(
        log=env.log,
        orchestrator_fn=env.orch,
        generate_fn=env.gen,
        say_fn=env.say,
        shutdown_fn=lambda: env.shutdowns.append(True),
        writer=env.writer,
        clock=lambda: NOW,
        **kwargs,
    )
    return env


async def _drain(pipeline):
    while pipeline._tasks:
        await asyncio.gather(*list(pipeline._tasks), return_exceptions=True)


async def _bootstrap(env):
    """초기 질문 발화 + 첫 답변(코드 강제 전환) → 본론 topic 질문까지 진행한 상태."""
    await env.pipeline.speak_initial("안녕하세요, 자기소개 부탁드립니다.")
    env.pipeline.on_user_turn_completed("백엔드 지망입니다.")
    await _drain(env.pipeline)


# --- 초기 발화·첫 답변 ---

def test_initial_utterance_is_committed_as_number_one():
    env = _make()

    async def scenario():
        await env.pipeline.speak_initial("안녕하세요, 자기소개 부탁드립니다.")

    asyncio.run(scenario())
    first = env.log.utterances[0]
    assert (first.question_number, first.parent_question_number) == (1, 1)
    assert first.question_type is QuestionType.INITIAL
    assert first.spoken_at == SPOKE_AT  # 발화 시각 = say 관측 시각


def test_first_answer_forces_next_topic_without_orchestrator():
    env = _make()
    asyncio.run(_bootstrap(env))

    assert env.orch.calls == 0  # 첫 답변은 Orchestrator 미호출
    answer, question = env.log.utterances[1], env.log.utterances[2]
    assert answer.speaker is Speaker.CANDIDATE
    assert (answer.question_number, answer.spoken_at) == (1, NOW)
    assert question.question_type is QuestionType.TOPIC
    assert (question.question_number, question.parent_question_number) == (2, 2)
    assert question.reason is None  # 코드 강제 경로에는 reason 없음


# --- 본론 판단·커밋 규칙 ---

def test_follow_up_commits_with_metadata():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("Redis 캐시를 도입했습니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.orch.calls == 1
    question = env.log.utterances[-1]
    assert question.question_type is QuestionType.FOLLOW_UP
    assert question.parent_question_number == 2  # 현재 줄기 루트
    assert question.follow_up_type is FollowUpType.DEEPEN
    assert question.reason == "근거 설명이 없음"
    assert question.spoken_at == SPOKE_AT


def test_m_cap_allows_mth_and_forces_next():
    env = _make(max_followups_per_branch=1)

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("첫 본론 답변입니다.")  # 꼬리 1개째 — 허용
        await _drain(env.pipeline)
        env.pipeline.on_user_turn_completed("꼬리에 대한 답변입니다.")  # M+1 — 강제 전환
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.orch.calls == 1  # 상한 도달 턴은 Orchestrator 미호출
    last = env.log.utterances[-1]
    assert last.question_type is QuestionType.TOPIC
    assert last.parent_question_number == last.question_number


def test_fallback_generation_is_treated_as_new_topic_without_reason():
    env = _make(gen=_GenSpy(results=(
        GeneratedQuestion(text="첫 전환 질문은 무엇인가요?", is_fallback=False),  # bootstrap 소비
        GeneratedQuestion(text=FALLBACK_QUESTIONS[0], is_fallback=True),
    )))

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    question = env.log.utterances[-1]
    assert question.question_type is QuestionType.TOPIC
    assert question.parent_question_number == question.question_number
    assert question.follow_up_type is None and question.reason is None


# --- single-flight·낡은 턴 폐기 ---

def test_new_turn_discards_stale_task_without_consuming_numbers():
    gate = asyncio.Event()
    env = _make(orch=_OrchSpy(gate=gate))

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await asyncio.sleep(0)  # task1이 게이트에서 블록될 때까지 양보
        env.pipeline.on_user_turn_completed("아, 그리고 추가로 말씀드리면요.")
        gate.set()
        await _drain(env.pipeline)

    asyncio.run(scenario())
    questions = [u for u in env.log.utterances if u.speaker is Speaker.INTERVIEWER]
    numbers = [u.question_number for u in questions]
    assert numbers == list(range(1, len(questions) + 1))  # 공백·역전 없음
    # 두 답변 모두 즉시 커밋됐고(유실 없음), 낡은 실행은 질문을 중복 커밋하지 않았다
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert len(answers) == 3
    assert len(questions) == 3  # initial + 첫 전환 + 승자 질문 1개
    # 승자 generate는 추가 답변까지 포함된 로그를 봤다 (초기Q·답변·전환Q·본론 답변 2건 = 5)
    assert env.gen.calls[-1][1] == 5


# --- TTS 실패 경로 ---

def test_tts_failure_retries_same_question_from_start():
    env = _make(say=_SaySpy(results=(
        SpeechResult(ok=True, started_at=SPOKE_AT),  # 초기 발화
        SpeechResult(ok=True, started_at=SPOKE_AT),  # 첫 전환 질문
        SpeechResult(ok=False),  # 본론 질문 1차 실패
        SpeechResult(ok=True, started_at=SPOKE_AT),  # 재시도 성공
    )))

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.say.calls[-1] == env.say.calls[-2]  # 같은 질문을 처음부터 재시도
    assert env.log.utterances[-1].speaker is Speaker.INTERVIEWER  # 성공 후 커밋
    assert not env.shutdowns


def test_tts_exhaustion_shuts_down_without_commit():
    env = _make(say=_SaySpy(results=(
        SpeechResult(ok=True, started_at=SPOKE_AT),
        SpeechResult(ok=True, started_at=SPOKE_AT),
        SpeechResult(ok=False),
        SpeechResult(ok=False),
    )))

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.shutdowns == [True]
    assert env.log.last_question_number() == 2  # 미커밋 — 번호 미소모
    assert env.log.utterances[-1].speaker is Speaker.CANDIDATE


def test_say_exception_is_treated_as_failure_and_retried():
    env = _make(say=_SaySpy(results=(
        SpeechResult(ok=True, started_at=SPOKE_AT),  # 초기 발화
        SpeechResult(ok=True, started_at=SPOKE_AT),  # 첫 전환 질문
        RuntimeError("session closed"),  # 본론 질문 1차 — 예외
        SpeechResult(ok=True, started_at=SPOKE_AT),  # 재시도 성공
    )))

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.say.calls[-1] == env.say.calls[-2]  # 예외도 실패로 보고 같은 질문 재시도
    assert env.log.utterances[-1].speaker is Speaker.INTERVIEWER  # 성공 후 커밋
    assert not env.shutdowns


def test_say_exception_twice_shuts_down_without_commit():
    env = _make(say=_SaySpy(results=(
        SpeechResult(ok=True, started_at=SPOKE_AT),
        SpeechResult(ok=True, started_at=SPOKE_AT),
        RuntimeError("session closed"),
        RuntimeError("session closed"),
    )))

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.shutdowns == [True]  # 예외가 잡 종료 경로를 우회하지 않는다
    assert env.log.last_question_number() == 2  # 미커밋·번호 미소모


# --- LLM 실패 경로에서도 발화 ---

def test_orchestrator_exception_still_speaks():
    env = _make(orch=_OrchSpy(error=True))

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.log.utterances[-1].question_type is QuestionType.TOPIC


def test_generate_exception_speaks_vetted_fallback():
    env = _make(gen=_GenSpy(error=True))

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    last = env.log.utterances[-1]
    assert last.speaker is Speaker.INTERVIEWER
    assert last.content in FALLBACK_QUESTIONS


# --- 전사 enqueue 공통 경로 ---

def test_writer_receives_every_utterance_in_memory_order():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.writer.items == [u.to_json_dict() for u in env.log.utterances]
    assert len(env.writer.items) == 5  # 초기·답변·전환·답변·꼬리 전 경로


# --- 방어 경로·수명 관리 ---

def test_speech_before_any_question_is_not_an_answer():
    env = _make()
    env.pipeline.on_user_turn_completed("질문 전 발화입니다.")
    assert env.log.utterances == ()


def test_blank_answer_is_ignored():
    env = _make()

    async def scenario():
        await env.pipeline.speak_initial("자기소개 부탁드립니다.")
        env.pipeline.on_user_turn_completed("   ")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert len(env.log.utterances) == 1


def test_aclose_is_idempotent_and_blocks_new_turns():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        await env.pipeline.aclose()
        await env.pipeline.aclose()
        env.pipeline.on_user_turn_completed("종료 후 발화입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.writer.closed
    assert env.log.utterances[-1].speaker is Speaker.INTERVIEWER  # 종료 후 답변 미적재
