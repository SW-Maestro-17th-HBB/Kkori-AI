import os

from dotenv import load_dotenv
from livekit import agents
from livekit.agents import Agent, AgentServer, AgentSession, TurnHandlingOptions, inference

from src.interview.prompts import INTERVIEWER_INSTRUCTIONS, initial_question_instructions
from src.session_context import parse_job_metadata

load_dotenv()

# LiveKit Inference 경유 모델 — 별도 프로바이더 키 없이 LiveKit 자격증명만으로 동작
STT_MODEL = "deepgram/nova-3"
STT_LANGUAGE = "ko"
LLM_MODEL = "openai/gpt-4.1-mini"
TTS_MODEL = "cartesia/sonic-3"
TTS_VOICE = "9626c31c-bec5-4cca-baa8-f8ba9e84c8bc"
TTS_LANGUAGE = "ko"


class InterviewerAgent(Agent):
    def __init__(self) -> None:
        super().__init__(instructions=INTERVIEWER_INSTRUCTIONS)


server = AgentServer()


@server.rtc_session()
async def entrypoint(ctx: agents.JobContext) -> None:
    session_context = parse_job_metadata(ctx.job.metadata)

    # Spring 연동 전 픽스처 검증용 — metadata가 없을 때만 환경 변수로 주입
    position = session_context.position or os.getenv("KKORI_POSITION_FIXTURE")
    resume_context = session_context.resume_context or os.getenv("KKORI_RESUME_CONTEXT_FIXTURE")

    session = AgentSession(
        stt=inference.STT(model=STT_MODEL, language=STT_LANGUAGE),
        llm=inference.LLM(model=LLM_MODEL),
        tts=inference.TTS(model=TTS_MODEL, voice=TTS_VOICE, language=TTS_LANGUAGE),
        turn_handling=TurnHandlingOptions(turn_detection=inference.TurnDetector()),
    )
    await session.start(room=ctx.room, agent=InterviewerAgent())

    # candidate 입장 전까지 첫 질문 보류 — 콘솔 모드는 fake room이라 대기 없이 진행
    if not ctx.is_fake_job():
        await ctx.wait_for_participant()

    await session.generate_reply(
        instructions=initial_question_instructions(position=position, resume_context=resume_context)
    )


if __name__ == "__main__":
    agents.cli.run_app(server)
