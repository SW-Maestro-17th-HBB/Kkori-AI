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
from src.interview.end_state import EndCause, EndPhase
from src.interview.interview_clock import InterviewClock
from src.interview.orchestrator import Decision, DecisionSource
from src.interview.prompts import (
    CLOSING_STATEMENTS_GENERAL,
    CLOSING_STATEMENTS_TIME_UP,
    FALLBACK_QUESTIONS,
    FINAL_QUESTIONS,
)
from src.interview.question_generation import GeneratedQuestion
from src.interview.turn_pipeline import SpeechResult, TurnPipeline

NOW = datetime(2026, 7, 24, 9, 0, 0, tzinfo=timezone.utc)
SPOKE_AT = datetime(2026, 7, 24, 9, 0, 7, tzinfo=timezone.utc)

_DEEPEN = Decision(
    action=Action.FOLLOW_UP, source=DecisionSource.ORCHESTRATOR,
    follow_up_type=FollowUpType.DEEPEN, reason="근거 설명이 없음",
)
_FINAL = Decision(
    action=Action.FINAL_QUESTION, source=DecisionSource.ORCHESTRATOR, reason="주제가 소진됨"
)
_END = Decision(
    action=Action.END, source=DecisionSource.ORCHESTRATOR, reason="시간이 소진됨"
)


class _OrchSpy:
    def __init__(self, decisions=(), gate=None, error=False):
        self.calls = 0
        self.wrap_ups: list = []  # 턴별로 전달된 wrap_up_minutes 기록
        self._decisions = list(decisions)
        self._gate = gate
        self._error = error

    async def __call__(self, log, wrap_up_minutes=None):
        self.calls += 1
        self.wrap_ups.append(wrap_up_minutes)
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
        shutdown_fn=lambda reason: env.shutdowns.append(reason),
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
    assert env.shutdowns == ["tts playout failure"]
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
    assert env.shutdowns == ["tts playout failure"]  # 예외가 잡 종료 경로를 우회하지 않는다
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


# --- 종료 국면 (docs/prd/interview-end.md §1·§2) ---

class _CleanupSpy:
    """cleanup_fn 스텁 — 클로징 재생 이후(CLEANING)에 호출된다."""

    def __init__(self):
        self.causes: list[EndCause] = []

    async def __call__(self, cause):
        self.causes.append(cause)


def _enter_waiting(env) -> int:
    """정상 경로(마지막 질문 커밋 후)로 WAITING_FINAL_ANSWER에 진입시킨다."""
    number = env.log.last_question_number() + 1
    env.log.append_question(
        question_number=number,
        parent_question_number=number,
        question_type=QuestionType.FINAL,
        content=FINAL_QUESTIONS[0],
        spoken_at=SPOKE_AT,
    )
    env.pipeline.end_state.try_advance(EndPhase.WAITING_FINAL_ANSWER)
    return number


def test_waiting_final_answer_commits_exactly_once_then_closes():
    cleanup = _CleanupSpy()
    env = _make(cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        number = _enter_waiting(env)
        env.pipeline.on_user_turn_completed("마지막으로 한마디 드리겠습니다.")
        await _drain(env.pipeline)
        env.pipeline.on_user_turn_completed("클로징 이후 추가 발화입니다.")
        await _drain(env.pipeline)
        return number

    final_number = asyncio.run(scenario())
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].content == "마지막으로 한마디 드리겠습니다."  # 1회만 커밋
    assert answers[-1].question_number == final_number  # 마지막 답변이 final 번호 승계
    closing = env.log.utterances[-1]
    assert closing.question_type is QuestionType.CLOSING
    assert closing.question_number is None  # 인사는 번호 없음
    assert closing.content in CLOSING_STATEMENTS_GENERAL  # FINAL_QUESTION 경유 = 일반형
    assert cleanup.causes == [EndCause.FINAL_QUESTION]
    assert env.pipeline.end_state.phase is EndPhase.CLEANING


def test_direct_closing_path_does_not_commit_later_turns():
    cleanup = _CleanupSpy()
    env = _make(cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        assert env.pipeline.begin_closing(EndCause.USER_REQUEST) is True
        env.pipeline.on_user_turn_completed("종료 요청 이후 발화입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    # 전이 이후 turn은 커밋되지 않고, 마지막 발화는 클로징 인사다 (일반형)
    assert all(u.content != "종료 요청 이후 발화입니다." for u in env.log.utterances)
    assert env.log.utterances[-1].question_type is QuestionType.CLOSING
    assert env.log.utterances[-1].content in CLOSING_STATEMENTS_GENERAL
    assert cleanup.causes == [EndCause.USER_REQUEST]


def test_hard_promotion_beats_final_answer_and_transcript_matches_winner():
    cleanup = _CleanupSpy()
    env = _make(cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        _enter_waiting(env)
        # hard가 마지막 답변 대기를 CLOSING으로 승격 — 이후 도착한 답변은 폐기
        assert env.pipeline.begin_closing(EndCause.HARD_TIMEOUT) is True
        env.pipeline.on_user_turn_completed("뒤늦게 도착한 마지막 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert all(
        u.content != "뒤늦게 도착한 마지막 답변입니다." for u in env.log.utterances
    )  # transcript가 전이 승자와 일치 — 답변 없음
    assert env.log.utterances[-1].content in CLOSING_STATEMENTS_TIME_UP  # hard = 시간 소진형
    assert cleanup.causes == [EndCause.HARD_TIMEOUT]
    assert env.pipeline.end_state.cause is EndCause.HARD_TIMEOUT


def test_begin_closing_is_first_wins_with_single_closing_run():
    cleanup = _CleanupSpy()
    env = _make(cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        assert env.pipeline.begin_closing(EndCause.USER_REQUEST) is True
        assert env.pipeline.begin_closing(EndCause.HARD_TIMEOUT) is False
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert cleanup.causes == [EndCause.USER_REQUEST]  # 부수효과 정확히 1회
    closings = [u for u in env.log.utterances if u.question_type is QuestionType.CLOSING]
    assert len(closings) == 1  # 클로징 인사도 1회만


def test_leaving_running_discards_inflight_generation():
    gate = asyncio.Event()
    cleanup = _CleanupSpy()
    env = _make(orch=_OrchSpy(gate=gate), cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await asyncio.sleep(0)  # 파이프라인이 게이트에서 블록될 때까지 양보
        env.pipeline.begin_closing(EndCause.USER_REQUEST)
        gate.set()
        await _drain(env.pipeline)

    asyncio.run(scenario())
    # 답변은 RUNNING에서 이미 커밋됐고, 진행 중이던 질문 생성 결과는 폐기됐다
    numbered_questions = [
        u
        for u in env.log.utterances
        if u.speaker is Speaker.INTERVIEWER and u.question_number is not None
    ]
    assert len(numbered_questions) == 2  # 초기 + 첫 전환 — 새 질문 없음
    assert cleanup.causes == [EndCause.USER_REQUEST]


def test_closing_waits_for_inflight_playout_before_running():
    say_gate = asyncio.Event()
    cleanup = _CleanupSpy()

    class _BlockingSay:
        def __init__(self):
            self.calls: list[str] = []

        async def __call__(self, text):
            self.calls.append(text)
            if len(self.calls) == 3:  # 본론 질문 재생만 게이트에서 블록
                await say_gate.wait()
            return SpeechResult(ok=True, started_at=SPOKE_AT)

    env = _make(say=_BlockingSay(), cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        for _ in range(10):  # 질문 재생(say)이 블록될 때까지 양보
            await asyncio.sleep(0)
        env.pipeline.begin_closing(EndCause.HARD_TIMEOUT)
        for _ in range(10):
            await asyncio.sleep(0)
        assert cleanup.causes == []  # 재생 완료 전에는 클로징이 시작되지 않는다
        say_gate.set()
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert cleanup.causes == [EndCause.HARD_TIMEOUT]
    # 재생이 완료된 질문은 실제로 들었으므로 커밋되고, 클로징이 그 뒤를 잇는다
    assert env.log.utterances[-2].speaker is Speaker.INTERVIEWER
    assert env.log.utterances[-2].question_number == 3
    assert env.log.utterances[-1].question_type is QuestionType.CLOSING


def test_cleanup_exception_falls_back_to_shutdown():
    class _FailingCleanup:
        async def __call__(self, cause):
            raise RuntimeError("cleanup boom")

    env = _make(cleanup_fn=_FailingCleanup())

    async def scenario():
        await _bootstrap(env)
        env.pipeline.begin_closing(EndCause.USER_REQUEST)
        await _drain(env.pipeline)

    asyncio.run(scenario())
    # 방치 금지 — 최후 fallback은 잡 종료, reason이 TTS 장애로 오기록되지 않는다
    assert env.shutdowns == ["cleanup failure"]


def test_time_guard_forces_hard_closing_when_deadline_passed():
    cleanup = _CleanupSpy()
    now = {"t": 0.0}
    clock = InterviewClock(
        duration_seconds=1800,
        wrap_up_remaining_seconds=300,
        hard_grace_seconds=180,
        monotonic=lambda: now["t"],
    )
    clock.start()
    env = _make(cleanup_fn=cleanup, interview_clock=clock)

    async def scenario():
        await _bootstrap(env)
        now["t"] = 2000.0  # 예정 종료(1800) + 유예(180) 초과
        env.pipeline.start_time_guard()
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert cleanup.causes == [EndCause.HARD_TIMEOUT]
    assert env.log.utterances[-1].content in CLOSING_STATEMENTS_TIME_UP
    assert env.pipeline.end_state.phase is EndPhase.CLEANING


# --- 마무리 판단·마지막 질문·클로징 (docs/prd/interview-end.md §2) ---

def test_end_decision_speaks_time_up_closing_with_reason():
    cleanup = _CleanupSpy()
    env = _make(orch=_OrchSpy(decisions=(_END,)), cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    closing = env.log.utterances[-1]
    assert closing.question_type is QuestionType.CLOSING
    assert closing.content in CLOSING_STATEMENTS_TIME_UP  # END 판단 = 시간 소진형
    assert closing.reason == "시간이 소진됨"  # Orchestrator 판단 경로 — reason 기록
    assert cleanup.causes == [EndCause.LLM_END]


def test_final_question_decision_speaks_final_waits_then_closes():
    cleanup = _CleanupSpy()
    env = _make(orch=_OrchSpy(decisions=(_FINAL,)), cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)
        # playout 성공·커밋 이후에만 마지막 답변 대기로 전이한다 (전이 트리거 계약)
        assert env.pipeline.end_state.phase is EndPhase.WAITING_FINAL_ANSWER
        env.pipeline.on_user_turn_completed("마지막 한마디입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    final = env.log.question_for(3)
    assert final.question_type is QuestionType.FINAL
    assert final.parent_question_number == 3  # parent=self 루트
    assert final.content in FINAL_QUESTIONS  # 검수 목록 원문
    assert final.reason == "주제가 소진됨"  # Orchestrator 판단 경로 — reason 기록
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].question_number == 3  # 마지막 답변이 final 질문 번호 승계
    assert env.log.utterances[-1].content in CLOSING_STATEMENTS_GENERAL
    assert cleanup.causes == [EndCause.FINAL_QUESTION]


def test_final_question_tts_exhaustion_closes_without_commit():
    cleanup = _CleanupSpy()
    env = _make(
        orch=_OrchSpy(decisions=(_FINAL,)),
        say=_SaySpy(results=(
            SpeechResult(ok=True, started_at=SPOKE_AT),  # 초기 발화
            SpeechResult(ok=True, started_at=SPOKE_AT),  # 첫 전환 질문
            SpeechResult(ok=False),  # 마지막 질문 1차 실패
            SpeechResult(ok=False),  # 재시도 실패
        )),
        cleanup_fn=cleanup,
    )

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert all(u.question_type is not QuestionType.FINAL for u in env.log.utterances)
    assert env.log.utterances[-1].content in CLOSING_STATEMENTS_GENERAL  # 일반형 문구
    assert cleanup.causes == [EndCause.FINAL_QUESTION]
    assert not env.shutdowns  # 잡 종료가 아니라 클로징으로 진행


def test_closing_tts_exhaustion_still_runs_cleanup():
    cleanup = _CleanupSpy()
    env = _make(
        say=_SaySpy(results=(
            SpeechResult(ok=True, started_at=SPOKE_AT),
            SpeechResult(ok=True, started_at=SPOKE_AT),
            SpeechResult(ok=False),  # 클로징 1차 실패
            SpeechResult(ok=False),  # 재시도 실패
        )),
        cleanup_fn=cleanup,
    )

    async def scenario():
        await _bootstrap(env)
        env.pipeline.begin_closing(EndCause.USER_REQUEST)
        await _drain(env.pipeline)

    asyncio.run(scenario())
    # 재생 안 된 클로징은 커밋하지 않고, 종료 시퀀스는 계속된다
    assert all(u.question_type is not QuestionType.CLOSING for u in env.log.utterances)
    assert cleanup.causes == [EndCause.USER_REQUEST]
    assert not env.shutdowns


def test_wrap_up_minutes_injected_at_turn_boundary():
    now = {"t": 0.0}
    clock = InterviewClock(
        duration_seconds=1800,
        wrap_up_remaining_seconds=300,
        hard_grace_seconds=180,
        monotonic=lambda: now["t"],
    )
    clock.start()
    env = _make(interview_clock=clock)

    async def scenario():
        await _bootstrap(env)  # 첫 답변은 코드 강제 — Orchestrator 미호출
        env.pipeline.on_user_turn_completed("본론 답변입니다.")  # 비마무리 단계
        await _drain(env.pipeline)
        now["t"] = 1560.0  # 남은 240초 = 약 4분 — 마무리 단계 진입
        env.pipeline.on_user_turn_completed("추가 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.orch.wrap_ups == [None, 4]  # 임계치 이후 턴부터만 남은 시간 주입


def test_wrap_up_cap_forces_final_question_without_orchestrator():
    cleanup = _CleanupSpy()
    now = {"t": 0.0}
    clock = InterviewClock(
        duration_seconds=1800,
        wrap_up_remaining_seconds=300,
        hard_grace_seconds=180,
        monotonic=lambda: now["t"],
    )
    clock.start()
    env = _make(interview_clock=clock, max_followups_per_branch=0, cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        now["t"] = 1560.0  # 마무리 단계
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.orch.calls == 0  # 상한 강제 — Orchestrator 미호출
    final = env.log.utterances[-1]
    assert final.question_type is QuestionType.FINAL  # NEXT_TOPIC 대신 FINAL_QUESTION
    assert final.reason is None  # 코드 강제 경로 — reason 없음
    assert env.pipeline.end_state.phase is EndPhase.WAITING_FINAL_ANSWER


def test_first_answer_completed_in_wrap_up_forces_final_question():
    cleanup = _CleanupSpy()
    now = {"t": 0.0}
    clock = InterviewClock(
        duration_seconds=1800,
        wrap_up_remaining_seconds=300,
        hard_grace_seconds=180,
        monotonic=lambda: now["t"],
    )
    clock.start()
    env = _make(interview_clock=clock, cleanup_fn=cleanup)

    async def scenario():
        await env.pipeline.speak_initial("안녕하세요, 자기소개 부탁드립니다.")
        now["t"] = 1560.0  # 첫 답변이 길어져 마무리 단계에서야 완료된 엣지
        env.pipeline.on_user_turn_completed("아주 길었던 자기소개입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.orch.calls == 0  # 첫 답변은 여전히 Orchestrator 미호출
    final = env.log.utterances[-1]
    assert final.question_type is QuestionType.FINAL  # 마무리 단계에 새 주제 없음
    assert env.pipeline.end_state.phase is EndPhase.WAITING_FINAL_ANSWER


def test_marker_recorded_exactly_once_on_closing_entry():
    markers: list[EndCause] = []

    async def marker(cause):
        markers.append(cause)

    cleanup = _CleanupSpy()
    env = _make(cleanup_fn=cleanup, marker_fn=marker)

    async def scenario():
        await _bootstrap(env)
        env.pipeline.begin_closing(EndCause.USER_REQUEST)
        env.pipeline.begin_closing(EndCause.HARD_TIMEOUT)  # 패자 — 부수효과 없음
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert markers == [EndCause.USER_REQUEST]


def test_marker_failure_does_not_block_closing():
    async def marker(cause):
        raise RuntimeError("marker boom")

    cleanup = _CleanupSpy()
    env = _make(cleanup_fn=cleanup, marker_fn=marker)

    async def scenario():
        await _bootstrap(env)
        env.pipeline.begin_closing(EndCause.USER_REQUEST)
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.log.utterances[-1].question_type is QuestionType.CLOSING  # 클로징 계속
    assert cleanup.causes == [EndCause.USER_REQUEST]


def test_waiting_without_final_question_is_defended():
    cleanup = _CleanupSpy()
    env = _make(cleanup_fn=cleanup)

    async def scenario():
        await _bootstrap(env)
        # 전이 트리거 계약 위반 상태 — 마지막 질문(final) 없이 WAITING 진입
        env.pipeline.end_state.try_advance(EndPhase.WAITING_FINAL_ANSWER)
        env.pipeline.on_user_turn_completed("이 발화는 마지막 답변이 아닙니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    # 이전 질문의 답변으로 오귀속하지 않고 폐기한다 — hard 안전망이 수렴
    assert all(
        u.content != "이 발화는 마지막 답변이 아닙니다." for u in env.log.utterances
    )
    assert cleanup.causes == []
