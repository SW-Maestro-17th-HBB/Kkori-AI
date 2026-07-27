"""본론 턴 흐름 제어 — single-flight 파이프라인. docs/prd/follow-up-question.md §1.

turn 훅에서는 답변 커밋과 턴 순번(turn_seq) 증가만 하고 즉시 반환한다(프레임워크는
이전 훅이 끝나기를 기다린다). Orchestrator→Interview는 훅 밖의 독립 task로 실행하고,
발화 직전 최신 턴 검사로 낡은 턴의 결과를 폐기한다(무효화 = 결과 폐기). 질문 번호는
예약하지 않고 playout 성공 후 commit lock 안에서 부여한다 — 폐기된 실행이 번호를
소모하지 않아 공백·역전이 없다. 어떤 LLM 실패에서도 턴이 침묵으로 끝나지 않으며,
TTS 재시도 소진 시에만 잡을 종료한다(침묵 방치 금지).
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from src.config import MAX_FOLLOWUPS_PER_BRANCH
from src.interview.conversation_log import (
    Action,
    ConversationLog,
    FollowUpType,
    QuestionType,
    Utterance,
)
from src.interview.end_state import EndCause, EndPhase, EndState
from src.interview.interview_clock import InterviewClock
from src.interview.orchestrator import Decision, DecisionSource, forced_next_topic
from src.interview.prompts import FALLBACK_QUESTIONS
from src.interview.question_generation import GeneratedQuestion

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SpeechResult:
    """say_fn의 결과 — ok는 playout 성공 3조건, started_at은 speaking 전환 관측 시각."""

    ok: bool
    started_at: datetime | None = None


class TurnPipeline:
    """의존성 주입 기반 턴 파이프라인 — LiveKit 없이 단위 테스트 가능."""

    def __init__(
        self,
        *,
        log: ConversationLog,
        orchestrator_fn: Callable[[ConversationLog], Awaitable[Decision]],
        generate_fn: Callable[[Decision, ConversationLog], Awaitable[GeneratedQuestion]],
        say_fn: Callable[[str], Awaitable[SpeechResult]],
        shutdown_fn: Callable[[], None],
        writer=None,
        max_followups_per_branch: int = MAX_FOLLOWUPS_PER_BRANCH,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        end_state: EndState | None = None,
        interview_clock: InterviewClock | None = None,
        closing_fn: Callable[[EndCause], Awaitable[None]] | None = None,
    ) -> None:
        self._log = log
        self._orchestrator_fn = orchestrator_fn
        self._generate_fn = generate_fn
        self._say_fn = say_fn
        self._shutdown_fn = shutdown_fn
        self._writer = writer
        self._max_followups = max_followups_per_branch
        self._clock = clock
        self._end_state = end_state or EndState()
        self._interview_clock = interview_clock
        self._closing_fn = closing_fn
        self._turn_seq = 0
        self._tasks: set[asyncio.Task] = set()
        self._commit_lock = asyncio.Lock()
        self._speech_lock = asyncio.Lock()  # 발화 직렬화 — 클로징이 진행 중 재생을 자르지 않는다
        self._closed = False

    @property
    def end_state(self) -> EndState:
        return self._end_state

    # --- turn 훅 경로 (최소 작업 후 즉시 반환) ---

    def on_user_turn_completed(self, answer_text: str) -> None:
        """답변 커밋 + 턴 순번 증가 + 독립 task 시작. 훅은 여기서 끝난다.

        이 메서드는 await 없는 동기 실행이라 상태 확인→커밋→전이가 하나의
        임계구역이다 — 확인과 커밋 사이에 다른 task(hard 타이머 등)가 끼어들
        수 없다(docs/prd/interview-end.md §1 커밋 정책).
        """
        if self._closed:
            return
        phase = self._end_state.phase
        if phase >= EndPhase.CLOSING:
            # CLOSING 진입 이후 발화는 어떤 경로에서도 커밋하지 않는다
            logger.info("종료 국면(%s) 중 발화 — 답변으로 처리하지 않음", phase.name)
            return
        text = answer_text.strip()
        if not text:
            logger.warning("빈 답변 텍스트 — 턴 무시")
            return
        if self._log.last_question_number() == 0:
            # 초기 질문 발화 전의 발화는 답변이 아니다 (경쟁 창 방어)
            logger.warning("질문 전 발화 수신 — 답변으로 처리하지 않음")
            return
        if phase is EndPhase.WAITING_FINAL_ANSWER:
            # 전이 트리거 계약(HBB1-285): 이 상태는 마지막 질문의 playout 성공·커밋
            # 이후에만 진입한다 — 따라서 이 turn은 마지막 질문에 대한 답변이다.
            # 마지막 답변 1회만 커밋하고 CLOSING으로 — 커밋과 전이가 같은 임계구역
            self._commit(self._log.append_answer(text, self._clock()))
            if self._end_state.try_advance(EndPhase.CLOSING, EndCause.FINAL_QUESTION):
                self._spawn(self._closing_task())
            return
        self._commit(self._log.append_answer(text, self._clock()))
        self._turn_seq += 1
        self._spawn(self._run(self._turn_seq))

    # --- 종료 국면 (docs/prd/interview-end.md §1) ---

    def begin_closing(self, cause: EndCause) -> bool:
        """CLOSING 직행 수렴점(END 판단·hard·사용자 종료) — 승자만 클로징을 1회 시작한다."""
        if self._closed:
            return False
        if self._end_state.try_advance(EndPhase.CLOSING, cause):
            self._spawn(self._closing_task())
            return True
        return False

    def start_time_guard(self) -> None:
        """hard 안전망 타이머 시작 — 면접 시계 start() 이후 호출한다."""
        if self._interview_clock is None:
            logger.warning("면접 시계 미주입 — hard 안전망 비활성")
            return
        self._spawn(self._guard_hard_limit())

    async def _guard_hard_limit(self) -> None:
        while not self._closed and self._end_state.phase < EndPhase.CLOSING:
            delay = self._interview_clock.hard_deadline_in()
            if delay <= 0:
                logger.warning("hard 시간 초과 — 강제 클로징")
                self.begin_closing(EndCause.HARD_TIMEOUT)
                return
            await asyncio.sleep(delay)

    async def _closing_task(self) -> None:
        """CLOSING 진입 부수효과 — 클로징 발화(HBB1-285)·종료 시퀀스(HBB1-286)가
        closing_fn을 채운다. 원인은 전이 승자가 이미 확정한 값이다."""
        cause = self._end_state.cause
        logger.info("종료 국면 진입 — 원인=%s", cause)
        # 진행 중 발화(질문 재생)가 있으면 완료를 기다린다 — 재생을 자르지 않는다
        # (PRD §1 hard). lock을 기다리던 다른 발화 실행은 stale 검사로 폐기된다.
        async with self._speech_lock:
            pass
        if self._closing_fn is None:
            return
        try:
            await self._closing_fn(cause)
        except Exception as exc:
            # 클로징 실패로 세션이 침묵에 머물지 않게 한다 — 최후 fallback은 잡 종료
            logger.error("클로징 처리 예외(%s) — 잡 종료 fallback", type(exc).__name__)
            self._shutdown_fn()

    # --- 초기 발화 (같은 재생·커밋 경로 — 발화 객체 #1) ---

    async def speak_initial(self, text: str) -> None:
        await self._speak_and_commit(
            turn_seq=self._turn_seq,
            text=text,
            question_type=QuestionType.INITIAL,
            follow_up_type=None,
            reason=None,
            ref_question_number=None,
        )

    # --- 본론 파이프라인 (독립 task) ---

    async def _run(self, turn_seq: int) -> None:
        decision = await self._decide()
        if self._is_stale(turn_seq):
            return
        generated = await self._generate(decision)
        if self._is_stale(turn_seq):
            return  # 폐기 — 번호 미소모 (답변은 이미 커밋돼 유실 없음)

        is_followup = decision.action is Action.FOLLOW_UP and not generated.is_fallback
        from_orchestrator = (
            decision.source is DecisionSource.ORCHESTRATOR and not generated.is_fallback
        )
        await self._speak_and_commit(
            turn_seq=turn_seq,
            text=generated.text,
            question_type=QuestionType.FOLLOW_UP if is_followup else QuestionType.TOPIC,
            follow_up_type=decision.follow_up_type if is_followup else None,
            reason=decision.reason if from_orchestrator else None,
            ref_question_number=decision.ref_question_number if is_followup else None,
        )

    async def _decide(self) -> Decision:
        """코드 우선 결정 — 첫 답변·상한 강제는 Orchestrator를 호출하지 않는다."""
        if not self._log.has_topic_or_followup_question():
            return forced_next_topic()
        if self._log.followup_count_in_current_branch() >= self._max_followups:
            logger.info("줄기 상한 도달 — NEXT_TOPIC 강제")
            return forced_next_topic()
        try:
            return await self._orchestrator_fn(self._log)
        except Exception as exc:
            # decide()가 내부 폴백을 갖지만, 주입 경계의 예외도 침묵으로 잇지 않는다.
            # 예외 객체는 기록하지 않는다 — 답변 원문이 담길 수 있다 (타입명만)
            logger.warning("orchestrator_fn 예외(%s) — NEXT_TOPIC 폴백", type(exc).__name__)
            return forced_next_topic()

    async def _generate(self, decision: Decision) -> GeneratedQuestion:
        try:
            return await self._generate_fn(decision, self._log)
        except Exception as exc:
            logger.warning("generate_fn 예외(%s) — 검수 폴백 질문", type(exc).__name__)
            return GeneratedQuestion(text=random.choice(FALLBACK_QUESTIONS), is_fallback=True)

    async def _speak_and_commit(
        self,
        *,
        turn_seq: int,
        text: str,
        question_type: QuestionType,
        follow_up_type: FollowUpType | None,
        reason: str | None,
        ref_question_number: int | None,
    ) -> None:
        """say 직전 최신 턴 검사 → playout 성공 후 lock 안에서 번호 부여·커밋.

        playout이 성공하면 지원자가 실제로 들은 질문이므로 이후에는 무조건 커밋한다
        (transcript = 실제 들은 발화). TTS 실패는 같은 질문을 처음부터 1회 재시도하고,
        소진되면 잡을 종료한다.
        """
        if self._is_stale(turn_seq):
            return
        async with self._speech_lock:
            # lock 대기 중 낡아진 실행(새 턴·종료 국면 진입)은 발화 없이 폐기한다 —
            # 클로징이 재생 완료를 기다린 뒤 새 질문이 이어서 재생되는 것을 막는다
            if self._is_stale(turn_seq):
                return
            result = await self._try_say(text)
            if not result.ok:
                if self._is_stale(turn_seq):
                    return
                logger.warning("질문 재생 실패 — 같은 질문 처음부터 재시도")
                result = await self._try_say(text)
                if not result.ok:
                    logger.error("TTS 재시도 소진 — 세션 진행 불가, 잡을 종료한다")
                    self._shutdown_fn()
                    return

        async with self._commit_lock:
            number = self._log.last_question_number() + 1
            parent = (
                self._log.current_root()
                if question_type is QuestionType.FOLLOW_UP
                else number
            )
            self._commit(
                self._log.append_question(
                    question_number=number,
                    parent_question_number=parent,
                    question_type=question_type,
                    content=text,
                    spoken_at=result.started_at or self._clock(),
                    follow_up_type=follow_up_type,
                    reason=reason,
                    ref_question_number=ref_question_number,
                )
            )

    async def _try_say(self, text: str) -> SpeechResult:
        """say_fn 예외도 재생 실패와 동일하게 처리한다 — 예외가 재시도·잡 종료
        경로를 우회해 세션이 침묵으로 방치되는 것을 막는다(세션 종료·미시작 등)."""
        try:
            return await self._say_fn(text)
        except Exception as exc:
            logger.warning("질문 재생 호출 예외(%s) — 실패로 처리", type(exc).__name__)
            return SpeechResult(ok=False)

    # --- 커밋·수명 관리 ---

    def _commit(self, utterance: Utterance) -> None:
        """메모리 append 반환값의 공통 Redis enqueue 경로 — 모든 발화가 여기를 지난다."""
        if self._writer is not None:
            try:
                self._writer.enqueue(utterance.to_json_dict())
            except Exception as exc:
                logger.warning("전사 enqueue 실패(%s) — 면접은 계속", type(exc).__name__)

    def _is_stale(self, turn_seq: int) -> bool:
        """최신 턴 검사 — 새 답변 턴 도착(순번 증가) 또는 `RUNNING` 이탈이면 낡은 실행이다.

        질문 생성 파이프라인은 RUNNING에서만 유효하다 — 종료 국면 진입과 함께
        진행 중 실행의 결과는 폐기된다(docs/prd/interview-end.md §1).
        """
        return (
            self._closed
            or turn_seq != self._turn_seq
            or self._end_state.phase is not EndPhase.RUNNING
        )

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            # 예외 객체 미기록 — 대화 내용이 담길 수 있다 (타입명만)
            logger.error(
                "턴 파이프라인 task 예외(%s)", type(task.exception()).__name__
            )

    async def aclose(self) -> None:
        """턴 순번 무효화 → task 정리 → writer drain·종료. 멱등 — job shutdown 시점에 호출."""
        if self._closed:
            return
        self._closed = True
        self._turn_seq += 1
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._writer is not None:
            await self._writer.aclose()
