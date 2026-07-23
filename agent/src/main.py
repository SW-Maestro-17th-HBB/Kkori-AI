import asyncio
import logging
import os

from dotenv import load_dotenv
from livekit import agents
from livekit.agents import Agent, AgentServer, AgentSession, TurnHandlingOptions, inference

from src.config import (
    LLM_MODEL,
    PARTICIPANT_WAIT_TIMEOUT_SECONDS,
    STT_LANGUAGE,
    STT_MODEL,
    TTS_LANGUAGE,
    TTS_MODEL,
    TTS_VOICE,
)
from src.interview.initial_question import initial_utterance, select_initial_question
from src.interview.prompts import INTERVIEWER_INSTRUCTIONS
from src.session_context import parse_job_metadata

load_dotenv()

logger = logging.getLogger(__name__)


class InterviewerAgent(Agent):
    def __init__(self) -> None:
        super().__init__(instructions=INTERVIEWER_INSTRUCTIONS)


server = AgentServer()


@server.rtc_session()
async def entrypoint(ctx: agents.JobContext) -> None:
    if ctx.job.metadata:
        session_context = parse_job_metadata(ctx.job.metadata)
        position = session_context.position
        resume_context = session_context.resume_context
    else:
        # Spring 연동 전 픽스처 검증용 — metadata가 아예 없을 때만 환경 변수로 주입
        position = os.getenv("KKORI_POSITION_FIXTURE")
        resume_context = os.getenv("KKORI_RESUME_CONTEXT_FIXTURE")

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

    llm = inference.LLM(model=LLM_MODEL)

    # 초기 질문은 세션 시작(마이크 입력·자동 턴 처리 활성화) 전에 확정한다 —
    # 선택 LLM 호출이 걸리는 동안 자동 턴 응답과 첫 질문이 경쟁하지 않도록.
    # LLM은 목록에서 번호만 고르고, 발화는 인사말 + 목록 원문으로 조립한다.
    question = await select_initial_question(
        llm, position=position, resume_context=resume_context
    )

    session = AgentSession(
        stt=inference.STT(model=STT_MODEL, language=STT_LANGUAGE),
        llm=llm,
        tts=inference.TTS(model=TTS_MODEL, voice=TTS_VOICE, language=TTS_LANGUAGE),
        turn_handling=TurnHandlingOptions(turn_detection=inference.TurnDetector()),
    )
    await session.start(room=ctx.room, agent=InterviewerAgent())

    await session.say(initial_utterance(question))


if __name__ == "__main__":
    agents.cli.run_app(server)
