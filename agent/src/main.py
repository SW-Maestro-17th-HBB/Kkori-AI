import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from livekit import agents, api
from livekit.agents import (
    Agent,
    AgentServer,
    AgentSession,
    RoomInputOptions,
    TurnHandlingOptions,
    inference,
)

from src.config import (
    AGENT_NAME,
    BEDROCK_INTERVIEW_LLM_MODEL,
    BEDROCK_LLM_MODEL,
    BEDROCK_ORCHESTRATOR_LLM_MODEL,
    HARD_OVERRUN_GRACE_SECONDS,
    INTERVIEW_DURATION_SECONDS,
    INTERVIEW_END_TOPIC,
    INTERVIEW_LLM_MODEL,
    LLM_MODEL,
    ORCHESTRATOR_LLM_MODEL,
    PARTICIPANT_WAIT_TIMEOUT_SECONDS,
    RECONNECT_WINDOW_SECONDS,
    RESUME_NOTICE,
    STT_LANGUAGE,
    STT_MODEL,
    TTS_LANGUAGE,
    TTS_MODEL,
    TTS_VOICE,
    WRAP_UP_REMAINING_SECONDS,
)
from src.interview.conversation_log import ConversationLog
from src.interview.end_sequence import EndSequence
from src.interview.end_signal import EndSignalReceiver
from src.interview.end_state import EndCause, EndPhase
from src.interview.initial_question import initial_utterance, select_initial_question
from src.interview.interview_clock import InterviewClock
from src.interview.orchestrator import decide
from src.interview.prompts import INTERVIEWER_INSTRUCTIONS
from src.interview.question_generation import generate_question
from src.interview.reconnect import PresenceMonitor
from src.interview.redis_sink import (
    REDIS_URL_ENV,
    create_transcript_writer,
    write_termination_marker,
)
from src.interview.report_request import publish_report_request
from src.interview.restore import ResumeMode, build_restore_plan
from src.interview.session_store import (
    claim_owner,
    clear_reconnect_deadline,
    init_session_meta,
    owner_allows,
    purge_session_state,
    read_restore_state,
    record_reconnect_deadline,
    release_owner,
)
from src.interview.transcript_store import DATABASE_URL_ENV, flush_transcript
from src.interview.turn_pipeline import SpeechResult, TurnPipeline
from src.llm_factory import build_llm
from src.log_privacy import install_privacy_filter
from src.session_context import parse_job_metadata

load_dotenv()

# 프레임워크 내부 로그의 STT 원문 extra 마스킹 — 답변 원문 운영 로그 금지 (PRD)
install_privacy_filter()

logger = logging.getLogger(__name__)


class InterviewerAgent(Agent):
    def __init__(self, pipeline: TurnPipeline) -> None:
        super().__init__(instructions=INTERVIEWER_INSTRUCTIONS)
        self._pipeline = pipeline

    async def on_user_turn_completed(self, turn_ctx, new_message) -> None:
        # 훅은 답변 커밋·턴 순번 증가만 하고 즉시 반환한다 — 프레임워크가 이전 훅 완료를
        # 기다리므로, 파이프라인(판단→생성→발화)은 훅 밖의 독립 task로 실행된다.
        # 발화 시작 시각(metrics)은 이탈 전 시작된 입력의 지연 완료 판정 재료 (recovery §1)
        metrics = getattr(new_message, "metrics", None) or {}
        self._pipeline.on_user_turn_completed(
            new_message.text_content or "",
            speech_started_at=metrics.get("started_speaking_at"),
        )


def _make_say_fn(session: AgentSession):
    async def say(text: str) -> SpeechResult:
        # spokenAt = agent가 speaking 상태로 전환된 관측 시각 (공개 이벤트만 사용).
        # 재생 시도별로 future를 새로 만들고 완료 후 구독을 해제한다 — 첫 시도의
        # 이벤트가 재시도의 시각으로 잘못 연결되지 않게.
        started: asyncio.Future = asyncio.get_running_loop().create_future()

        def on_state_changed(ev) -> None:
            if getattr(ev, "new_state", None) == "speaking" and not started.done():
                started.set_result(datetime.now(timezone.utc))

        session.on("agent_state_changed", on_state_changed)
        try:
            # add_to_chat_ctx=False — llm 미사용이어도 프레임워크 내부 ChatContext에
            # 면접관 발화가 축적되지 않게 한다(컨텍스트는 대화 로그가 단일 원천)
            handle = session.say(text, allow_interruptions=False, add_to_chat_ctx=False)
            await handle  # SpeechHandle은 await으로 예외를 던지지 않는다
        finally:
            session.off("agent_state_changed", on_state_changed)
        ok = handle.done() and not handle.interrupted and handle.exception() is None
        return SpeechResult(
            ok=ok, started_at=started.result() if started.done() else None
        )

    return say


server = AgentServer()


# agent_name 등록 = 자동 디스패치 종료 — 입장은 Spring createDispatch(agentName, metadata)
# 또는 로컬 lk dispatch create가 담당한다 (docs/prd/interview.md §1)
@server.rtc_session(agent_name=AGENT_NAME)
async def entrypoint(ctx: agents.JobContext) -> None:
    # cli.run_app이 늦게 구성한 핸들러에도 마스킹 필터를 적용한다 (멱등)
    install_privacy_filter()

    session_id = None
    if ctx.job.metadata:
        session_context = parse_job_metadata(ctx.job.metadata)
        session_id = session_context.session_id
        position = session_context.position
        resume_context = session_context.resume_context
    else:
        # metadata 없는 dispatch(콘솔·lk dispatch create) 로컬 테스트용 —
        # metadata가 아예 없을 때만 환경 변수로 주입
        position = os.getenv("KKORI_POSITION_FIXTURE")
        resume_context = os.getenv("KKORI_RESUME_CONTEXT_FIXTURE")

    # 운영 fail-fast — sessionId 있는 잡의 저장 인프라 미구성은 시작 거부한다.
    # 면접을 끝까지 진행해놓고 저장을 통째로 잃는 것보다 배포 시점에 드러나는 게 낫다.
    # 생략 폴백(경고 후 진행)은 sessionId 부재(콘솔·픽스처 로컬)에만 허용 (PRD §3)
    if session_id and not os.getenv(REDIS_URL_ENV):
        logger.error("sessionId 있는 잡에 %s 미구성 — 시작 거부(fail-fast)", REDIS_URL_ENV)
        ctx.shutdown(reason="missing runtime config: redis")
        return
    if session_id and not os.getenv(DATABASE_URL_ENV):
        logger.error("sessionId 있는 잡에 %s 미구성 — 시작 거부(fail-fast)", DATABASE_URL_ENV)
        ctx.shutdown(reason="missing runtime config: database")
        return

    # 사용자 명시 종료 수신 — 리스너는 룸 이벤트가 흐르기 전(초기화 전)에 먼저 등록한다.
    # LiveKit data 메시지는 재전달되지 않으므로, 파이프라인 준비 전에 도착한 신호는
    # receiver가 보관했다가 bind 시점에 전달된다 (초기화 구간 유실 방지).
    # sessionId 없는 로컬·콘솔은 외부 종료 신호 대상이 아니다.
    end_receiver = None
    if session_id:
        end_receiver = EndSignalReceiver(
            expected_topic=INTERVIEW_END_TOPIC, session_id=session_id
        )
        ctx.room.on("data_received", end_receiver.on_data)

    # --- 신규/복원 판별 — metadata가 아니라 agent 소유 Redis 상태의 존재로 가른다
    # (디스패치 metadata 4필드 계약 불변, docs/prd/interview-recovery.md §2)
    restore_plan = None
    if session_id:
        state = await read_restore_state(session_id)
        if state.terminated:
            # Spring 계약상 재디스패치 대상이 아닌 세션 — 아무 상태도 훼손하지
            # 않고 잡만 종료한다(룸 삭제·표식 갱신 없음)
            logger.error("종료 표식 있는 세션에 재디스패치 — 방어적 즉시 종료")
            ctx.shutdown(reason="terminated session redispatch")
            return
        if state.restorable:
            restore_plan = build_restore_plan(
                state, now=datetime.now(timezone.utc)
            )
        # 종결 단계 가드 재료 — last-wins, 원자성 없는 관측·완화 계층
        await claim_owner(session_id, ctx.job.id)

    log = restore_plan.log if restore_plan is not None else ConversationLog()
    writer = create_transcript_writer(session_id)

    interview_clock = InterviewClock(
        duration_seconds=INTERVIEW_DURATION_SECONDS,
        wrap_up_remaining_seconds=WRAP_UP_REMAINING_SECONDS,
        hard_grace_seconds=HARD_OVERRUN_GRACE_SECONDS,
    )
    if restore_plan is not None:
        # 경과 = 벽시계 − startedAt (끊김·재디스패치 지연 포함 — 시계는 계속 흐른다)
        interview_clock.start_with_elapsed(restore_plan.elapsed_seconds)
        logger.info(
            "면접 복원 — 모드=%s, 경과 %.0f초(근사=%s), 재구성 드롭 %d건, orphan=%s",
            restore_plan.mode.name,
            max(0.0, restore_plan.elapsed_seconds),
            restore_plan.started_at_approximated,
            restore_plan.dropped,
            restore_plan.orphan_branch,
        )

    async def delete_room() -> None:
        await ctx.api.room.delete_room(api.DeleteRoomRequest(room=ctx.room.name))

    async def flush() -> bool:
        # flush 원본 = 메모리 완전본 (PRD §4) — 복원 세션은 재구성본+신규 발화가
        # 원본이다(gap 감수 — recovery §2의 명시적 예외)
        return await flush_transcript(
            session_id, [u.to_json_dict() for u in log.utterances]
        )

    guard_fn = None
    if session_id:

        async def guard() -> bool:
            return await owner_allows(session_id, ctx.job.id)

        guard_fn = guard

    # 종료 시퀀스 — flush·정리·발행·룸 삭제는 운영 경로(sessionId 존재)에서만 수행한다.
    end_sequence = EndSequence(
        shutdown_fn=lambda reason: ctx.shutdown(reason=reason),
        writer=writer,
        flush_fn=flush if session_id else None,
        purge_fn=(
            (lambda: purge_session_state(session_id)) if session_id else None
        ),
        publish_fn=(
            (lambda: publish_report_request(session_id)) if session_id else None
        ),
        delete_room_fn=(
            delete_room if session_id and not ctx.is_fake_job() else None
        ),
        guard_fn=guard_fn,
    )

    async def on_marker(cause: EndCause) -> None:
        if not session_id:
            return
        if guard_fn is not None and not await guard_fn():
            logger.warning("owner 불일치 — 종료 표식 기록 생략(완화 계층)")
            return
        await write_termination_marker(session_id, str(cause))

    async def base_cleanup() -> None:
        # 터미널 경로 포함 공통 정리 — writer aclose는 멱등, owner는 자기 소유만 DEL
        if writer is not None:
            await writer.aclose()
        if session_id:
            await release_owner(session_id, ctx.job.id)

    ctx.add_shutdown_callback(base_cleanup)

    async def converge_without_session(cause: EndCause) -> None:
        """음성 세션 없이 종료 수렴 — 발화가 불가능(candidate 부재)·불필요한 복원
        종결 경로. CLOSING 진입 부수효과(표식)와 종료 시퀀스만 수행한다."""
        await on_marker(cause)
        await end_sequence.run(cause)

    # --- 복원: 세션 없이 종결로 직행하는 모드 (docs/prd/interview-recovery.md §2)
    if restore_plan is not None:
        if restore_plan.mode is ResumeMode.CLOSE_RECOVERED:
            # closing까지 재생됐으나 flush 전 소실 — 표식 재기록(RECOVERED_CLOSING)이
            # 재디스패치 루프 차단이다. 클로징 재발화 없이 종료 시퀀스를 재개한다
            await converge_without_session(EndCause.RECOVERED_CLOSING)
            return
        if restore_plan.candidate_identity is None:
            # fail-closed — identity 판정 기준 없이 재개하지 않는다(시간 기준 이원화)
            cause = (
                EndCause.HARD_TIMEOUT
                if interview_clock.hard_exceeded()
                else EndCause.RECONNECT_TIMEOUT
            )
            logger.error("candidateIdentity 유실 — fail-closed 종료 수렴(원인=%s)", cause)
            await converge_without_session(cause)
            return

    candidate_identity = (
        restore_plan.candidate_identity if restore_plan is not None else None
    )

    # 종결 직행 모드(복원) — 입장 대기·초기 질문 선택이 불필요하다. 클로징은
    # 즉시 진입하고, 발화 여부는 재실 게이트가 정한다 (recovery §2 판별표)
    heading_to_close = restore_plan is not None and (
        restore_plan.mode is ResumeMode.CLOSE_FINAL_ANSWERED
        or interview_clock.hard_exceeded()
    )

    def candidate_present() -> bool:
        """identity 일치 기반 재실 판정 — 파이프라인의 커밋·클로징 발화 게이트."""
        if ctx.is_fake_job() or candidate_identity is None:
            return True  # 콘솔·식별 확정 전 — 게이트 없음
        return any(
            p.identity == candidate_identity
            for p in ctx.room.remote_participants.values()
        )

    # --- candidate 입장 관측 (LLM·세션 구축 전 — 무입장 잡의 유료 호출 방지) ---
    if not ctx.is_fake_job():
        if restore_plan is not None:
            await ctx.connect()
            if not heading_to_close:
                if not candidate_present():
                    # 잔여 창 대기 = min(reconnectDeadline, hard 시한) − now.
                    # deadline 부재(이탈 관측자가 없던 소실)는 복원 시점부터 새 창 (PRD §2)
                    now = datetime.now(timezone.utc)
                    deadline = restore_plan.reconnect_deadline
                    if deadline is None:
                        deadline = now + timedelta(seconds=RECONNECT_WINDOW_SECONDS)
                        if session_id:
                            await record_reconnect_deadline(session_id, deadline)
                    wait_timeout = min(
                        (deadline - now).total_seconds(),
                        interview_clock.hard_deadline_in(),
                    )
                    if wait_timeout > 0:
                        try:
                            await asyncio.wait_for(
                                ctx.wait_for_participant(identity=candidate_identity),
                                timeout=wait_timeout,
                            )
                        except TimeoutError:
                            pass
                if not candidate_present():
                    cause = (
                        EndCause.HARD_TIMEOUT
                        if interview_clock.hard_exceeded()
                        else EndCause.RECONNECT_TIMEOUT
                    )
                    logger.warning("복원 입장 대기 소진 — 종료 수렴(원인=%s)", cause)
                    await converge_without_session(cause)
                    return
                if session_id:
                    await clear_reconnect_deadline(session_id)
        else:
            try:
                participant = await asyncio.wait_for(
                    ctx.wait_for_participant(),
                    timeout=PARTICIPANT_WAIT_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                logger.warning(
                    "candidate 입장 타임아웃(%d초) — 잡을 종료한다",
                    PARTICIPANT_WAIT_TIMEOUT_SECONDS,
                )
                ctx.shutdown(reason="participant wait timeout")
                return
            # candidate 식별 = 최초 identity 고정 — 이후 재입장 판정은 일치로만 (PRD §1)
            candidate_identity = participant.identity

    if restore_plan is None:
        # 면접 시작 기준 시각 = candidate 입장 관측 시점 (docs/prd/interview-end.md §1)
        interview_clock.start()
        if session_id and candidate_identity:
            # 시작 시각·identity 내구 저장 — 원자 초기화(HSETNX+EXPIRE, recovery §2)
            await init_session_meta(
                session_id,
                started_at=datetime.now(timezone.utc),
                candidate_identity=candidate_identity,
            )

    # 초기 질문 — 신규 또는 빈 로그 복원만 (recovery §2: 재구성 로그가 비어 있지
    # 않으면 생략, 빈 로그는 신규와 같은 진행 경로 — 초기 질문 없인 진행 불능)
    question = None
    if not heading_to_close and not log.utterances:
        selection_llm = build_llm(LLM_MODEL, BEDROCK_LLM_MODEL)
        try:
            question = await select_initial_question(
                selection_llm, position=position, resume_context=resume_context
            )
        finally:
            await selection_llm.aclose()

    orchestrator_llm = build_llm(ORCHESTRATOR_LLM_MODEL, BEDROCK_ORCHESTRATOR_LLM_MODEL)
    try:
        interview_llm = build_llm(INTERVIEW_LLM_MODEL, BEDROCK_INTERVIEW_LLM_MODEL)
    except BaseException:
        # 부분 초기화 실패 — shutdown 콜백 등록 전이라 여기서 직접 정리한다
        await orchestrator_llm.aclose()
        raise

    # llm 미지정 — 훅 실행 후 프레임워크가 기본 응답 생성을 건너뛴다(이중 발화 차단).
    # 본론 질문은 TurnPipeline이 세션 밖 LLM으로 생성해 say()로 발화한다.
    # TurnDetector는 VAD가 없으면 비활성화되므로 vad를 반드시 전달한다.
    session = AgentSession(
        stt=inference.STT(model=STT_MODEL, language=STT_LANGUAGE),
        tts=inference.TTS(model=TTS_MODEL, voice=TTS_VOICE, language=TTS_LANGUAGE),
        vad=inference.VAD(),
        turn_handling=TurnHandlingOptions(turn_detection=inference.TurnDetector()),
    )

    pipeline = TurnPipeline(
        log=log,
        writer=writer,
        orchestrator_fn=lambda log, wrap_up_minutes: decide(
            orchestrator_llm, log, wrap_up_remaining_minutes=wrap_up_minutes
        ),
        generate_fn=lambda decision, log: generate_question(
            interview_llm, decision, log, resume_context=resume_context
        ),
        say_fn=_make_say_fn(session),
        shutdown_fn=lambda reason: ctx.shutdown(reason=reason),
        interview_clock=interview_clock,
        cleanup_fn=end_sequence.run,
        marker_fn=on_marker if session_id else None,
        listener_present_fn=candidate_present,
        # 이탈 전에 시작된 입력의 지연 완료 차단 — 경계 = 직전 이탈 관측 시각.
        # monitor 무장 전(이탈 관측 없음)은 경계 없음(None)이라 무해
        input_boundary_fn=lambda: (
            monitor.last_disconnect_at if monitor is not None else None
        ),
    )

    monitor: PresenceMonitor | None = None

    async def cleanup() -> None:
        # entrypoint는 초기 발화 후 반환해도 세션은 계속 동작한다 —
        # 정리는 실제 job shutdown 시점에만 수행한다(aclose는 멱등)
        await pipeline.aclose()
        if monitor is not None:
            await monitor.aclose()
        await asyncio.gather(
            orchestrator_llm.aclose(),
            interview_llm.aclose(),
            return_exceptions=True,
        )

    ctx.add_shutdown_callback(cleanup)

    # candidate 이탈 시 세션 즉시 종료(기본값)를 끈다 — 이탈·재입장은 agent가
    # 재연결 창으로 관장한다 (docs/prd/interview-recovery.md §1)
    await session.start(
        room=ctx.room,
        agent=InterviewerAgent(pipeline),
        room_input_options=RoomInputOptions(close_on_disconnect=False),
    )

    # 파이프라인·세션 준비 완료 — 초기화 중 보류된 종료 신호가 있으면 여기서
    # 클로징으로 이어지고, 아래 초기 발화는 stale 검사로 폐기된다
    if end_receiver is not None:
        end_receiver.bind(lambda: pipeline.begin_closing(EndCause.USER_REQUEST))

    # --- 복원: 종결 직행 (세션 시작 후 — candidate 재실이면 클로징이 재생된다)
    if restore_plan is not None and heading_to_close:
        if restore_plan.mode is ResumeMode.CLOSE_FINAL_ANSWERED:
            # 마지막 답변까지 완료된 세션 — 클로징부터 재개 (recovery §2 판별표)
            pipeline.begin_closing(EndCause.FINAL_QUESTION)
        else:
            # 복원 시점에 시간 이미 소진 — 사실상 완주, 정상 종료 수렴(시간 소진형)
            pipeline.begin_closing(EndCause.HARD_TIMEOUT)
        return

    async def resume_interview() -> None:
        # 재입장 재개 — 초기 질문조차 발화되지 못한 빈 로그(초기화 구간 이탈)는
        # 신규와 같은 진행 경로로 초기 질문을 발화한다(재개 안내 없음). 빈 로그로
        # 이 경로에 오는 잡은 항상 초기 질문을 선택해 둔 상태다
        if log.utterances:
            await pipeline.resume_after_reconnect(RESUME_NOTICE)
        else:
            await pipeline.speak_initial(initial_utterance(question))

    # --- 재연결 모니터 무장 + 이벤트 배선 — candidate 재실 상태에서만 (recovery §1)
    if not ctx.is_fake_job() and candidate_identity:
        monitor = PresenceMonitor(
            candidate_identity=candidate_identity,
            window_seconds=RECONNECT_WINDOW_SECONDS,
            begin_closing_fn=pipeline.begin_closing,
            resume_fn=resume_interview,
            invalidate_fn=pipeline.invalidate_inflight,
            record_deadline_fn=(
                (lambda deadline: record_reconnect_deadline(session_id, deadline))
                if session_id
                else None
            ),
            clear_deadline_fn=(
                (lambda: clear_reconnect_deadline(session_id))
                if session_id
                else None
            ),
            hard_exceeded_fn=interview_clock.hard_exceeded,
        )
        ctx.room.on(
            "participant_disconnected",
            lambda p: monitor.on_participant_disconnected(p.identity),
        )
        ctx.room.on(
            "participant_connected",
            lambda p: monitor.on_participant_connected(p.identity),
        )
        # 초기화 구간(입장 관측 → 리스너 등록 사이)의 퇴장 이벤트는 유실된다 —
        # 무장 직후 룸 상태와 대조해 이미 이탈한 상태면 창을 즉시 시작한다.
        # 리스너 등록 이후 확인이므로 이 사이 이벤트와 겹쳐도 멱등(no-op)이다
        if not candidate_present():
            monitor.on_participant_disconnected(candidate_identity)

    # --- 국면 복원 — 발화 여부와 무관하게 먼저 세운다. orphan 줄기 강제 전환은
    # 별도 상태가 아니라 파이프라인이 판단마다 로그에서 관측한다 (recovery §2)
    if restore_plan is not None and log.utterances:
        if restore_plan.mode is ResumeMode.WAITING_FINAL_ANSWER:
            # 마지막 발화가 미답변 final — 국면 복원(재개 앵커가 final을 재낭독하고,
            # 답변 1회 커밋 후 클로징으로 수렴한다 — 기존 커밋 정책 그대로)
            pipeline.end_state.try_advance(EndPhase.WAITING_FINAL_ANSWER)

    # --- 재개/시작 발화 — 초기화 구간 이탈이 관측됐으면 연기한다: 빈 룸 발화는
    # 커밋 폐기로 유실될 뿐이다. 재입장 콜백(resume_interview)이 이어받는다
    if not candidate_present():
        logger.info("candidate 부재 — 시작·재개 발화 연기(재입장 시 수행)")
    elif restore_plan is not None and log.utterances:
        await pipeline.resume_after_reconnect(RESUME_NOTICE)
    else:
        # 신규 세션 또는 transcript만 유실된 복원(빈 로그 = 신규와 같은 진행 경로)
        await pipeline.speak_initial(initial_utterance(question))
    pipeline.start_time_guard()


if __name__ == "__main__":
    agents.cli.run_app(server)
