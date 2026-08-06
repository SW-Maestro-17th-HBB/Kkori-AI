"""파이프라인의 재연결·복원 동작 단위 테스트 — docs/prd/interview-recovery.md §1·§2.

청자 게이트(이탈 후 완료 turn 폐기), 재개 앵커(재낭독 무커밋 / 다음 질문),
클로징 발화 생략(창 소진·복원·부재), orphan 강제 전환을 검증한다.
"""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from src.interview.conversation_log import ConversationLog, QuestionType, Speaker
from src.interview.end_state import EndCause, EndPhase
from src.interview.question_generation import GeneratedQuestion
from src.interview.turn_pipeline import SpeechResult, TurnPipeline

NOW = datetime(2026, 8, 6, 9, 0, 0, tzinfo=timezone.utc)
SPOKE_AT = datetime(2026, 8, 6, 9, 0, 7, tzinfo=timezone.utc)
RESUME_NOTICE = "연결이 복구되었습니다. 면접을 이어가겠습니다."


class _OrchSpy:
    def __init__(self):
        self.calls = 0

    async def __call__(self, log, wrap_up_minutes=None):
        self.calls += 1
        from src.interview.orchestrator import forced_next_topic

        return forced_next_topic()


class _GenSpy:
    async def __call__(self, decision, log):
        return GeneratedQuestion(text="다음 질문입니다. 어떻게 생각하세요?", is_fallback=False)


class _SaySpy:
    def __init__(self):
        self.calls: list[str] = []

    async def __call__(self, text):
        self.calls.append(text)
        return SpeechResult(ok=True, started_at=SPOKE_AT)


def _make(**kwargs):
    env = SimpleNamespace(
        log=ConversationLog(),
        orch=_OrchSpy(),
        gen=_GenSpy(),
        say=_SaySpy(),
        shutdowns=[],
        cleanups=[],
        markers=[],
        present=True,
    )

    async def cleanup(cause):
        env.cleanups.append(cause)

    async def marker(cause):
        env.markers.append(cause)

    env.pipeline = TurnPipeline(
        log=env.log,
        orchestrator_fn=env.orch,
        generate_fn=env.gen,
        say_fn=env.say,
        shutdown_fn=lambda reason: env.shutdowns.append(reason),
        clock=lambda: NOW,
        cleanup_fn=cleanup,
        marker_fn=marker,
        listener_present_fn=lambda: env.present,
        **kwargs,
    )
    return env


async def _drain(pipeline):
    while pipeline._tasks:
        await asyncio.gather(*list(pipeline._tasks), return_exceptions=True)


async def _bootstrap(env):
    """초기 질문 + 첫 답변 → 본론 topic 질문까지 진행한 상태."""
    await env.pipeline.speak_initial("안녕하세요, 자기소개 부탁드립니다.")
    env.pipeline.on_user_turn_completed("백엔드 지망입니다.")
    await _drain(env.pipeline)


# --- 청자 게이트 (recovery §1 입력 경계) ---

def test_turn_completed_after_departure_is_discarded():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        env.present = False  # 이탈 관측 — 이후 완료되는 turn은 폐기
        before = len(env.log.utterances)
        env.pipeline.on_user_turn_completed("끊기기 직전 잘린 부분 답변")
        await _drain(env.pipeline)
        return before

    before = asyncio.run(scenario())
    assert len(env.log.utterances) == before  # 커밋 없음
    assert env.orch.calls == 0  # 파이프라인 미기동


# --- 재개 앵커 (recovery §1 확정 규칙) ---

def test_resume_respeaks_unanswered_question_without_commit():
    env = _make()

    async def scenario():
        await _bootstrap(env)  # 마지막 발화 = 미답변 topic 질문
        before = len(env.log.utterances)
        await env.pipeline.resume_after_reconnect(RESUME_NOTICE)
        await _drain(env.pipeline)
        return before

    before = asyncio.run(scenario())
    question = env.log.utterances[-1].content
    assert env.say.calls[-2:] == [RESUME_NOTICE, question]  # 안내 → 같은 질문 재낭독
    assert len(env.log.utterances) == before  # 재커밋 없음 — 번호 미소모


def test_resume_after_answer_generates_next_question():
    env = _make()

    async def scenario():
        await env.pipeline.speak_initial("안녕하세요, 자기소개 부탁드립니다.")
        env.log.append_answer("답변까지 마친 상태입니다.", NOW)  # 마지막 발화 = 답변
        await env.pipeline.resume_after_reconnect(RESUME_NOTICE)
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.say.calls[1] == RESUME_NOTICE
    last = env.log.utterances[-1]
    assert last.speaker is Speaker.INTERVIEWER  # 일반 파이프라인 — 다음 질문 생성·커밋
    assert last.question_type is QuestionType.TOPIC


def test_resume_is_noop_after_closing():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        env.pipeline.begin_closing(EndCause.USER_REQUEST)
        await _drain(env.pipeline)
        said = len(env.say.calls)
        await env.pipeline.resume_after_reconnect(RESUME_NOTICE)
        await _drain(env.pipeline)
        return said

    said = asyncio.run(scenario())
    assert len(env.say.calls) == said  # 종료 국면 — 재개 없음(first-wins)


def test_resume_in_waiting_final_answer_respeaks_final_and_commits_once():
    env = _make()

    async def scenario():
        await env.pipeline.speak_initial("안녕하세요, 자기소개 부탁드립니다.")
        env.log.append_answer("자기소개 답변입니다.", NOW)
        env.log.append_question(
            question_number=2,
            parent_question_number=2,
            question_type=QuestionType.FINAL,
            content="마지막으로 하고 싶은 말씀 있으신가요?",
            spoken_at=NOW,
        )
        # 복원: 마지막 발화가 미답변 final — 국면 복원 후 재개
        env.pipeline.end_state.try_advance(EndPhase.WAITING_FINAL_ANSWER)
        await env.pipeline.resume_after_reconnect(RESUME_NOTICE)
        env.pipeline.on_user_turn_completed("마지막 어필입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.say.calls[-2] == "마지막으로 하고 싶은 말씀 있으신가요?"  # final 재낭독
    assert env.pipeline.end_state.cause is EndCause.FINAL_QUESTION  # 답변 1회 → 클로징
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].content == "마지막 어필입니다."
    assert env.cleanups == [EndCause.FINAL_QUESTION]


# --- 클로징 발화 생략 (recovery §1·§2) ---

def test_reconnect_timeout_closing_skips_speech_but_marks_and_cleans():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        env.present = False
        said = len(env.say.calls)
        env.pipeline.begin_closing(EndCause.RECONNECT_TIMEOUT)
        await _drain(env.pipeline)
        return said

    said = asyncio.run(scenario())
    assert len(env.say.calls) == said  # 클로징 발화 생략 — 청자 없음
    assert all(u.question_type is not QuestionType.CLOSING for u in env.log.utterances)
    assert env.markers == [EndCause.RECONNECT_TIMEOUT]  # 표식은 기록 — 재디스패치 차단
    assert env.cleanups == [EndCause.RECONNECT_TIMEOUT]


def test_recovered_closing_skips_speech_even_with_listener():
    env = _make()

    async def scenario():
        env.pipeline.begin_closing(EndCause.RECOVERED_CLOSING)
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.say.calls == []  # 클로징은 이미 재생·커밋된 세션 — 재발화 없음
    assert env.markers == [EndCause.RECOVERED_CLOSING]  # 표식 재기록 — 루프 차단
    assert env.cleanups == [EndCause.RECOVERED_CLOSING]


def test_hard_timeout_with_absent_candidate_skips_speech_but_cleans():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        env.present = False  # 창 도중 hard 선소진 — 청자 없음
        said = len(env.say.calls)
        env.pipeline.begin_closing(EndCause.HARD_TIMEOUT)
        await _drain(env.pipeline)
        return said

    said = asyncio.run(scenario())
    assert len(env.say.calls) == said  # 발화만 생략 — flush 등 정상 종료는 end_sequence가 수행
    assert env.cleanups == [EndCause.HARD_TIMEOUT]


# --- orphan 줄기 강제 전환 (recovery §2) ---

def test_force_topic_shift_skips_orchestrator_once():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        env.pipeline.force_topic_shift()
        env.pipeline.on_user_turn_completed("orphan 줄기의 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.orch.calls == 0  # 코드 우선 결정 — Orchestrator 미호출
    last = env.log.utterances[-1]
    assert last.question_type is QuestionType.TOPIC  # FOLLOW_UP 커밋 실패 원천 차단


def test_invalidate_inflight_discards_pending_generation():
    env = _make()

    async def scenario():
        await _bootstrap(env)
        before = len(env.log.utterances)
        env.pipeline.on_user_turn_completed("이탈 직전의 답변입니다.")
        env.pipeline.invalidate_inflight()  # 이탈 관측 — 진행 중 실행 폐기
        await _drain(env.pipeline)
        return before

    before = asyncio.run(scenario())
    # 답변 커밋은 유지되지만(이탈 전 완료), 새 질문 발화·커밋은 폐기된다
    assert len(env.log.utterances) == before + 1
    assert env.log.utterances[-1].speaker is Speaker.CANDIDATE


# --- connection epoch 입력 경계 (리뷰 반영 — 이전 epoch 입력의 지연 도착) ---

def _make_with_epoch():
    env = _make()
    env.epoch = 0
    env.pipeline._epoch_fn = lambda: env.epoch
    return env


def test_answer_started_in_previous_epoch_is_discarded_after_reentry():
    env = _make_with_epoch()

    async def scenario():
        await _bootstrap(env)
        env.pipeline.mark_user_speech_started()  # 발화 시작 관측 — epoch 0 보존
        env.epoch = 1  # 이탈 → 재입장 (재실 상태로 되돌아옴)
        before = len(env.log.utterances)
        env.pipeline.on_user_turn_completed("끊기며 잘린 답변의 지연 완료")
        await _drain(env.pipeline)
        return before

    before = asyncio.run(scenario())
    assert len(env.log.utterances) == before  # 재실이어도 이전 epoch 입력은 폐기


def test_answer_started_in_current_epoch_commits_normally():
    env = _make_with_epoch()

    async def scenario():
        await _bootstrap(env)
        env.epoch = 1
        env.pipeline.mark_user_speech_started()  # 재입장 후 새 발화 — epoch 1
        env.pipeline.on_user_turn_completed("재입장 후의 정상 답변")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].content == "재입장 후의 정상 답변"


def test_unobserved_speech_start_falls_back_to_presence_gate():
    env = _make_with_epoch()

    async def scenario():
        await _bootstrap(env)
        env.epoch = 1  # 발화 시작 미관측(mark 없음) — 재실 검사로만 판정
        env.pipeline.on_user_turn_completed("epoch 미관측 답변")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].content == "epoch 미관측 답변"  # 오배선이 입력 전체를 막지 않는다


# --- 재생 완료 후 커밋 직전 재검사 (리뷰 반영 — 빈 룸 재생 완료) ---

class _LeavingSay:
    """지정한 회차의 재생 도중 candidate가 이탈하는 say 스텁."""

    def __init__(self, env, leave_on_call: int):
        self.env = env
        self.calls: list[str] = []
        self._leave_on = leave_on_call

    async def __call__(self, text):
        self.calls.append(text)
        if len(self.calls) == self._leave_on:
            self.env.present = False  # 재생 중 이탈 — 빈 룸으로 재생은 완료된다
        return SpeechResult(ok=True, started_at=SPOKE_AT)


def test_question_finished_in_empty_room_is_not_committed():
    env = _make()
    env.say = _LeavingSay(env, leave_on_call=2)  # 2번째 재생(본론 질문) 도중 이탈
    env.pipeline._say_fn = env.say

    async def scenario():
        await env.pipeline.speak_initial("안녕하세요, 자기소개 부탁드립니다.")
        env.pipeline.on_user_turn_completed("자기소개 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    # 빈 룸에 재생을 마친 질문은 "실제 들은 발화"가 아니다 — 커밋 폐기
    assert env.log.utterances[-1].speaker is Speaker.CANDIDATE
    questions = [u for u in env.log.utterances if u.speaker is Speaker.INTERVIEWER]
    assert len(questions) == 1  # 초기 질문뿐


def test_final_question_finished_in_empty_room_skips_commit_and_transition():
    from src.interview.conversation_log import Action
    from src.interview.orchestrator import Decision, DecisionSource

    env = _make()
    env.say = _LeavingSay(env, leave_on_call=2)  # final 재생 도중 이탈
    env.pipeline._say_fn = env.say

    async def final_decision(log, wrap_up_minutes=None):
        return Decision(
            action=Action.FINAL_QUESTION,
            source=DecisionSource.ORCHESTRATOR,
            reason="마무리",
        )

    env.pipeline._orchestrator_fn = final_decision

    async def scenario():
        await _bootstrap(env)
        env.pipeline.on_user_turn_completed("본론 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.pipeline.end_state.phase is EndPhase.RUNNING  # WAITING 전이 없음
    assert all(
        u.question_type is not QuestionType.FINAL for u in env.log.utterances
    )  # final 미커밋 — 창 소진 또는 재입장 앵커가 수렴


def test_stale_callback_after_new_speech_started_is_still_discarded():
    """재입장 후 새 발화가 시작된 뒤에 이전 연결의 완료가 도착하는 경합 —
    최초 기록 우선 + 완료 시 소진으로 이전 답변은 폐기되고 새 답변은 커밋된다."""
    env = _make_with_epoch()

    async def scenario():
        await _bootstrap(env)
        env.pipeline.mark_user_speech_started()  # 이전 연결에서 발화 시작 — epoch 0
        env.epoch = 1  # 이탈 → 재입장
        env.pipeline.mark_user_speech_started()  # 새 발화 시작 — 최초 기록(0)은 안 덮인다
        before = len(env.log.utterances)
        env.pipeline.on_user_turn_completed("이전 연결에서 잘린 답변의 지연 완료")
        await _drain(env.pipeline)
        dropped = len(env.log.utterances) == before
        env.pipeline.on_user_turn_completed("재입장 후의 새 답변")
        await _drain(env.pipeline)
        return dropped

    dropped = asyncio.run(scenario())
    assert dropped  # 이전 epoch 입력 폐기
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].content == "재입장 후의 새 답변"  # 소진 후 완료 — 재실 폴백 커밋
