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

# 본론 질문 파이프라인 — docs/prd/follow-up-question.md
# 역할별 모델 분리(교체 대비) — 현행은 초기 질문과 같은 모델 공유로 시작
ORCHESTRATOR_LLM_MODEL = "openai/gpt-4.1-mini"
INTERVIEW_LLM_MODEL = "openai/gpt-4.1-mini"

# 아래 수치는 전부 [미확정 — 실측 조정] 기본값 (preview 스크립트로 관찰 후 조정)
MAX_FOLLOWUPS_PER_BRANCH = 3  # 줄기당 꼬리질문 상한 M — M개째 허용, M+1번째 강제 전환
RECENT_BRANCHES_FOR_TOPIC = 3  # 주제 전환 컨텍스트에 넣는 최근 줄기 수 N
ORCHESTRATOR_INPUT_TOKEN_BUDGET = 8000  # Orchestrator 입력 상한(시스템 지시·출력 예약 포함)
UTTERANCE_INJECTION_TOKEN_CAP = 800  # 개별 발화 주입 상한 — 로그 원문은 보존, 주입 시에만 절단
QUESTION_MAX_CHARS = 250  # Interview 출력 길이 상한 — 초과 시 폴백 질문
LLM_CALL_TIMEOUT_SECONDS = 20  # 판단·생성 호출 상한 — 초과 시 각 폴백 경로(침묵 고정 방지)
REDIS_TRANSCRIPT_TTL_SECONDS = 86400  # 마지막 발화 이후 Redis 사본 잔존 상한(매 append 갱신)

# 면접 종료 — docs/prd/interview-end.md §1. soft 5분·hard 3분은 기본값 확정(실측 조정 여지)
INTERVIEW_DURATION_SECONDS = 30 * 60  # THIRTY_MIN 총 면접 시간
WRAP_UP_REMAINING_SECONDS = 5 * 60  # soft — 남은 시간이 이하로 내려가면 마무리 단계 진입
HARD_OVERRUN_GRACE_SECONDS = 3 * 60  # hard — 예정 종료 초과 유예, 소진 시 코드 강제 클로징

# 세션 정리·퇴장 — docs/prd/interview-end.md §3
INTERVIEW_END_TOPIC = "interview:end"  # 사용자 종료 SendData topic [미확정 — Spring 계약 합의]
END_STEP_TIMEOUT_SECONDS = 10  # 종료 시퀀스 외부 호출(DB·Redis·LiveKit) 단계별 타임아웃
ROOM_DELETE_MAX_ATTEMPTS = 3  # 룸 삭제 bounded retry — best-effort 단계가 아니다
ROOM_DELETE_RETRY_BACKOFF_SECONDS = 2  # 재시도 간격 — 연속 재시도는 같은 일시 장애 창에서 전부 실패한다

# 리포트 생성 요청 발행 — docs/prd/interview-end.md §5
REPORT_REQUEST_STREAM_KEY = "report.generation.requested"
