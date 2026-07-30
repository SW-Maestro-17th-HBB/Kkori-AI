import asyncio
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from livekit import agents, api
from livekit.agents import Agent, AgentServer, AgentSession, TurnHandlingOptions, inference

from src.config import (
    HARD_OVERRUN_GRACE_SECONDS,
    INTERVIEW_DURATION_SECONDS,
    INTERVIEW_END_TOPIC,
    INTERVIEW_LLM_MODEL,
    LLM_MODEL,
    ORCHESTRATOR_LLM_MODEL,
    PARTICIPANT_WAIT_TIMEOUT_SECONDS,
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
from src.interview.end_state import EndCause
from src.interview.initial_question import initial_utterance, select_initial_question
from src.interview.interview_clock import InterviewClock
from src.interview.orchestrator import decide
from src.interview.prompts import INTERVIEWER_INSTRUCTIONS
from src.interview.question_generation import generate_question
from src.interview.redis_sink import (
    REDIS_URL_ENV,
    create_transcript_writer,
    purge_transcript_copy,
    write_termination_marker,
)
from src.interview.report_request import publish_report_request
from src.interview.transcript_store import DATABASE_URL_ENV, flush_transcript
from src.interview.turn_pipeline import SpeechResult, TurnPipeline
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
        self._pipeline.on_user_turn_completed(new_message.text_content or "")


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


@server.rtc_session()
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
        # Spring 연동 전 픽스처 검증용 — metadata가 아예 없을 때만 환경 변수로 주입
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

    # 룸 연결과 candidate 입장 확인을 가장 먼저 한다(wait_for_participant가 미연결 시 자동 연결) —
    # 참가자가 끝내 입장하지 않으면 유료 선택 호출도 발생하지 않고, LLM 지연이 룸 연결을 막지 않는다.
    # 콘솔 모드는 fake room이라 대기 없이 진행.
    if not ctx.is_fake_job():
        try:
            await asyncio.wait_for(
                ctx.wait_for_participant(), timeout=PARTICIPANT_WAIT_TIMEOUT_SECONDS
            )
        except TimeoutError:
            logger.warning(
                "candidate 입장 타임아웃(%d초) — 잡을 종료한다", PARTICIPANT_WAIT_TIMEOUT_SECONDS
            )
            ctx.shutdown(reason="participant wait timeout")
            return

    # 면접 시작 기준 시각 = candidate 입장 관측 시점 (docs/prd/interview-end.md §1)
    interview_clock = InterviewClock(
        duration_seconds=INTERVIEW_DURATION_SECONDS,
        wrap_up_remaining_seconds=WRAP_UP_REMAINING_SECONDS,
        hard_grace_seconds=HARD_OVERRUN_GRACE_SECONDS,
    )
    interview_clock.start()

    selection_llm = inference.LLM(model=LLM_MODEL)

    # 초기 질문은 세션 시작(마이크 입력·턴 처리 활성화) 전에 확정한다 —
    # LLM은 목록에서 번호만 고르고, 발화는 인사말 + 목록 원문으로 조립한다.
    question = await select_initial_question(
        selection_llm, position=position, resume_context=resume_context
    )

    orchestrator_llm = inference.LLM(model=ORCHESTRATOR_LLM_MODEL)
    interview_llm = inference.LLM(model=INTERVIEW_LLM_MODEL)

    # llm 미지정 — 훅 실행 후 프레임워크가 기본 응답 생성을 건너뛴다(이중 발화 차단).
    # 본론 질문은 TurnPipeline이 세션 밖 LLM으로 생성해 say()로 발화한다.
    # TurnDetector는 VAD가 없으면 비활성화되므로 vad를 반드시 전달한다.
    session = AgentSession(
        stt=inference.STT(model=STT_MODEL, language=STT_LANGUAGE),
        tts=inference.TTS(model=TTS_MODEL, voice=TTS_VOICE, language=TTS_LANGUAGE),
        vad=inference.VAD(),
        turn_handling=TurnHandlingOptions(turn_detection=inference.TurnDetector()),
    )

    writer = create_transcript_writer(session_id)
    log = ConversationLog()

    async def delete_room() -> None:
        await ctx.api.room.delete_room(api.DeleteRoomRequest(room=ctx.room.name))

    async def flush() -> bool:
        # flush 원본 = 메모리 완전본 (PRD §4) — 종료 시퀀스가 로그 확정 후 호출한다
        return await flush_transcript(
            session_id, [u.to_json_dict() for u in log.utterances]
        )

    # 종료 시퀀스 — flush·정리·발행·룸 삭제는 운영 경로(sessionId 존재)에서만 수행한다.
    end_sequence = EndSequence(
        shutdown_fn=lambda reason: ctx.shutdown(reason=reason),
        writer=writer,
        flush_fn=flush if session_id else None,
        purge_fn=(lambda: purge_transcript_copy(session_id)) if session_id else None,
        publish_fn=(
            (lambda: publish_report_request(session_id)) if session_id else None
        ),
        delete_room_fn=(
            delete_room if session_id and not ctx.is_fake_job() else None
        ),
    )

    async def on_marker(cause: EndCause) -> None:
        if session_id:
            await write_termination_marker(session_id, str(cause))

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
    )

    async def cleanup() -> None:
        # entrypoint는 초기 발화 후 반환해도 세션은 계속 동작한다 —
        # 정리는 실제 job shutdown 시점에만 수행한다(aclose는 멱등)
        await pipeline.aclose()
        await asyncio.gather(
            selection_llm.aclose(),
            orchestrator_llm.aclose(),
            interview_llm.aclose(),
            return_exceptions=True,
        )

    ctx.add_shutdown_callback(cleanup)

    await session.start(room=ctx.room, agent=InterviewerAgent(pipeline))

    # 파이프라인·세션 준비 완료 — 초기화 중 보류된 종료 신호가 있으면 여기서
    # 클로징으로 이어지고, 아래 초기 발화는 stale 검사로 폐기된다
    if end_receiver is not None:
        end_receiver.bind(lambda: pipeline.begin_closing(EndCause.USER_REQUEST))

    await pipeline.speak_initial(initial_utterance(question))
    pipeline.start_time_guard()


if __name__ == "__main__":
    agents.cli.run_app(server)
