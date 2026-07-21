from dotenv import load_dotenv
from livekit import agents
from livekit.agents import Agent, AgentServer, AgentSession, TurnHandlingOptions, inference

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
        super().__init__(
            instructions=(
                "당신은 AI 모의 면접관 '꼬리'입니다. 항상 한국어로 대화합니다. "
                "지금은 음성 연결을 검증하는 단계이므로, 사용자의 말을 듣고 "
                "짧고 자연스럽게 응답하세요. 이모지나 특수문자 없이 말하듯 답합니다."
            )
        )


server = AgentServer()


@server.rtc_session()
async def entrypoint(ctx: agents.JobContext) -> None:
    session = AgentSession(
        stt=inference.STT(model=STT_MODEL, language=STT_LANGUAGE),
        llm=inference.LLM(model=LLM_MODEL),
        tts=inference.TTS(model=TTS_MODEL, voice=TTS_VOICE, language=TTS_LANGUAGE),
        turn_handling=TurnHandlingOptions(turn_detection=inference.TurnDetector()),
    )
    await session.start(room=ctx.room, agent=InterviewerAgent())
    await session.generate_reply(
        instructions="사용자에게 인사하고, 음성이 잘 들리는지 확인해 달라고 요청하세요."
    )


if __name__ == "__main__":
    agents.cli.run_app(server)
