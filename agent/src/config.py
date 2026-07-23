"""에이전트 구성값 — main 임포트 부수효과(AgentServer 생성 등) 없이 참조 가능한 상수 모음."""

# LiveKit Inference 경유 모델 — 별도 프로바이더 키 없이 LiveKit 자격증명만으로 동작
STT_MODEL = "deepgram/nova-3"
STT_LANGUAGE = "ko"
LLM_MODEL = "openai/gpt-4.1-mini"
TTS_MODEL = "cartesia/sonic-3"
TTS_VOICE = "9626c31c-bec5-4cca-baa8-f8ba9e84c8bc"
TTS_LANGUAGE = "ko"

# candidate 입장 대기 상한(초) — 토큰 입장 윈도우(3분)보다 여유 있게.
# Spring 준비 타임아웃(PENDING→ABORTED) 도입 전까지 잡이 무기한 점유되지 않도록 하는 로컬 안전망
PARTICIPANT_WAIT_TIMEOUT_SECONDS = 300
