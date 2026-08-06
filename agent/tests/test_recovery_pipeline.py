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
        log=kwargs.pop("log", ConversationLog()),
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

def _orphan_log():
    """루트(2)가 유실된 꼬리질문(3)이 현재 줄기인 재구성 로그."""
    from src.interview.conversation_log import rebuild_conversation_log

    items = [
        {
            "questionNumber": 1, "parentQuestionNumber": 1,
            "speaker": "INTERVIEWER", "questionType": "initial",
            "content": "자기소개 부탁드립니다.", "spokenAt": "2026-08-06T09:00:00Z",
        },
        {
            "questionNumber": 1, "parentQuestionNumber": 1,
            "speaker": "CANDIDATE", "content": "답변입니다.",
            "spokenAt": "2026-08-06T09:00:30Z",
        },
        {
            "questionNumber": 3, "parentQuestionNumber": 2,
            "speaker": "INTERVIEWER", "questionType": "followup",
            "followUpType": "DEEPEN", "content": "orphan 꼬리질문입니다.",
            "spokenAt": "2026-08-06T09:01:00Z",
        },
    ]
    log, dropped = rebuild_conversation_log(items)
    assert dropped == 0
    return log


def test_orphan_branch_forces_topic_shift_without_orchestrator():
    env = _make(log=_orphan_log())

    async def scenario():
        env.pipeline.on_user_turn_completed("orphan 질문의 답변입니다.")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    assert env.orch.calls == 0  # 코드 우선 결정 — Orchestrator 미호출
    last = env.log.utterances[-1]
    assert last.question_type is QuestionType.TOPIC  # FOLLOW_UP 커밋 실패 원천 차단


def test_orphan_forcing_survives_discarded_execution():
    """강제된 실행이 폐기(재생 중 이탈)돼도 다음 판단에서 다시 강제된다 —
    소비형 플래그가 아니라 판단 시점의 로그 관측이므로 상태가 소실되지 않는다."""
    env = _make(log=_orphan_log())
    env.say = _LeavingSay(env, leave_on_call=1)  # 강제 전환 질문 재생 도중 이탈
    env.pipeline._say_fn = env.say

    async def scenario():
        env.pipeline.on_user_turn_completed("orphan 질문의 답변입니다.")
        await _drain(env.pipeline)  # 강제 topic 재생 완료 — 청자 부재로 커밋 폐기
        discarded = all(
            u.question_type is not QuestionType.TOPIC for u in env.log.utterances
        )
        env.present = True  # 재입장
        env.pipeline.on_user_turn_completed("재입장 후의 답변입니다.")
        await _drain(env.pipeline)
        return discarded

    discarded = asyncio.run(scenario())
    assert discarded
    assert env.orch.calls == 0  # 두 번째 판단도 다시 강제 — FOLLOW_UP 커밋 실패 없음
    assert env.log.utterances[-1].question_type is QuestionType.TOPIC


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


# --- 입력 경계 (리뷰 반영 — 이탈 전 시작된 입력의 지연 완료) ---
# 경계 = 직전 이탈 관측 시각. 판정 근거는 turn 완료에 실려 오는 발화 시작 시각이다.

DISCONNECT_AT = 1_000.0  # 직전 이탈 관측 시각(unix 초 — 테스트 고정값)


def _make_with_boundary():
    env = _make()
    env.boundary = None  # 이탈 관측 전 — 경계 없음
    env.pipeline._input_boundary_fn = lambda: env.boundary
    return env


def test_answer_started_before_disconnect_is_discarded_after_reentry():
    env = _make_with_boundary()

    async def scenario():
        await _bootstrap(env)
        env.boundary = DISCONNECT_AT  # 이탈 → 재입장 (재실 상태로 되돌아옴)
        before = len(env.log.utterances)
        env.pipeline.on_user_turn_completed(
            "끊기며 잘린 답변의 지연 완료", speech_started_at=DISCONNECT_AT - 5
        )
        await _drain(env.pipeline)
        return before

    before = asyncio.run(scenario())
    assert len(env.log.utterances) == before  # 재실이어도 이탈 전 시작 입력은 폐기


def test_answer_started_after_reentry_commits_even_if_old_never_completes():
    """리뷰 지적 경합 — 이전 입력의 완료가 끝내 도착하지 않아도, 새 답변은
    자기 시작 시각이 경계 이후라 즉시 커밋된다(페어링 상태 없음)."""
    env = _make_with_boundary()

    async def scenario():
        await _bootstrap(env)
        env.boundary = DISCONNECT_AT  # 이전 발화는 완료 없이 소실된 상황
        env.pipeline.on_user_turn_completed(
            "재입장 후의 정상 답변", speech_started_at=DISCONNECT_AT + 30
        )
        await _drain(env.pipeline)

    asyncio.run(scenario())
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].content == "재입장 후의 정상 답변"


def test_missing_speech_start_falls_back_to_presence_gate():
    env = _make_with_boundary()

    async def scenario():
        await _bootstrap(env)
        env.boundary = DISCONNECT_AT  # 시작 시각 미제공 — 재실 검사로만 판정
        env.pipeline.on_user_turn_completed("시작 시각 없는 답변")
        await _drain(env.pipeline)

    asyncio.run(scenario())
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].content == "시작 시각 없는 답변"  # 미제공이 입력 전체를 막지 않는다


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
    """재입장 후 새 발화가 진행 중일 때 이전 연결의 완료가 뒤늦게 도착하는 경합 —
    완료별 시작 시각 판정이라 이전 답변만 폐기되고 새 답변은 커밋된다."""
    env = _make_with_boundary()

    async def scenario():
        await _bootstrap(env)
        env.boundary = DISCONNECT_AT  # 이탈 → 재입장, 새 발화도 이미 시작된 상태
        before = len(env.log.utterances)
        env.pipeline.on_user_turn_completed(
            "이전 연결에서 잘린 답변의 지연 완료", speech_started_at=DISCONNECT_AT - 5
        )
        await _drain(env.pipeline)
        dropped = len(env.log.utterances) == before
        env.pipeline.on_user_turn_completed(
            "재입장 후의 새 답변", speech_started_at=DISCONNECT_AT + 30
        )
        await _drain(env.pipeline)
        return dropped

    dropped = asyncio.run(scenario())
    assert dropped  # 이탈 전 시작 입력만 폐기
    answers = [u for u in env.log.utterances if u.speaker is Speaker.CANDIDATE]
    assert answers[-1].content == "재입장 후의 새 답변"
