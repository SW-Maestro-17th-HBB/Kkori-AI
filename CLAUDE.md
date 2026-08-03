# CLAUDE.md

## 프로젝트 개요

Kkori — AI 면접 준비 서비스의 AI 컴포넌트 (SW마에스트로 팀 HBB). Python 3.13 / uv 모노레포 — **서비스별 독립 uv 프로젝트** (각자 pyproject.toml·uv.lock·.venv·.python-version).

- `agent/` (`kkori-agent`) — LiveKit 음성 에이전트. 면접 실시간 루프(STT→LLM→TTS) 담당
- `worker/` (`kkori-worker`) — 이력서 분석 워커. FastStream(Redis) + Bedrock(Claude·Cohere) + pgvector (다른 팀원 작업 영역)
- `shared/` — 공용 코드. 아직 미사용 — 실제 import가 생기는 시점에 각 프로젝트의 path 의존성으로 참조

## 명령어

```bash
cd agent && uv sync                            # agent 의존성 동기화 (agent/.venv, 개발 전 1회 — worker도 동일)
cd agent && uv run python -m src.main console  # 터미널 마이크로 에이전트와 직접 대화 (로컬 검증)
cd agent && uv run python -m src.main dev      # LiveKit Cloud에 워커 등록 (명시 디스패치 — 아래 lk dispatch로 입장)
lk dispatch create --room kkori-local --agent-name kkori-interviewer --metadata '{"sessionId":"123","interviewType":"THIRTY_MIN","position":"BACKEND"}'  # dev 모드 실행 중 룸 생성·에이전트 입장 (--room 필수, --metadata 생략 시 픽스처 폴백)
uv add --project <서비스> <패키지>              # 의존성 추가 (해당 서비스의 uv.lock 자동 갱신)
cd agent && uv run pytest                      # agent 테스트 (LLM 스모크는 KKORI_LIVE_LLM=1 설정 시에만 실호출)
uv run --project worker pytest worker          # worker 테스트 (일부는 로컬 인프라 없으면 skip)
cd worker && uv run faststream run src.main:app         # 이력서 분석 워커 실행
cd worker && uv run faststream run src.report.main:app  # 리포트 생성 워커 실행 (별개 프로세스)
docker build -f agent/Dockerfile -t kkori-agent .    # 이미지 빌드 — 컨텍스트는 반드시 레포 루트
docker build -f worker/Dockerfile -t kkori-worker .
```

- 에이전트 실행에는 `agent/.env`에 LiveKit Cloud 자격증명(STT·TTS·룸)과 AWS 자격증명(Bedrock LLM) 필요 (`agent/.env.example` 참조, Git 비추적)
- agent는 `agent/tests/` 운용 중 — 단위 테스트(파싱·프롬프트 조립·선택 폴백)는 항상 실행, LLM 스모크 테스트(유료 실호출)는 `KKORI_LIVE_LLM=1` + 프로바이더 자격증명(bedrock 기본 → AWS 키, inference → LiveKit 키)이 있을 때만 실행(기본 skip — CI 포함). worker는 `worker/tests/` 운용 중 — 일부 테스트는 로컬 인프라(Postgres/Redis/MinIO) 없으면 skip, CI에서는 서비스 컨테이너로 전부 실행

## 작업 규칙

- 의존성 변경은 pyproject.toml 직접 편집 대신 `uv add/remove --project <서비스>` 사용, `uv.lock` 커밋 필수
- 코드 변경 후 최소한 import 검증(`cd <서비스> && uv run python -c "import src.main"`)을 통과할 것 — CI 첫 관문과 동일
- 커밋 메시지 타입은 `feat`, `fix`, `chore`, `docs`, `refactor`, `test`를 사용하고 지라 키는 넣지 않는다 (예: `feat: bootstrap livekit voice agent`) — 지라 연동은 브랜치 키와 PR 제목이 담당

## 기술적 결정사항

- **worker 는 파이프라인(도메인)별 독립 프로세스** — 한 코드베이스(worker/)지만 이력서(`src.main:app`)와 리포트(`src.report.main:app`)를 별개 앱으로 실행한다. 리포트는 LLM 의존이 크고 배포가 잦아 장애·배포를 격리하기 위함. 리포트 코드는 `src/report/` 도메인 패키지에 모으고, 공유는 `contract/`·`ai/`·`config.py` 같은 라이브러리 계층뿐. 회수·발행 모듈은 이력서 계약에 묶여 있어 리포트 몫을 복제했다 — 셋째 도메인이 생기면 공용화 검토
- **서비스별 독립 uv 프로젝트 (워크스페이스 아님)** — 두 서비스는 상호 import 없이 독립 배포되므로 단일 락파일 워크스페이스 대신 서비스마다 자체 pyproject.toml·uv.lock을 둔다(서비스 간 잠금 결합 제거, 팀원 간 작업 충돌 방지). 각 Dockerfile이 자기 락파일로 `uv sync --locked --no-dev` 후 소스 복사. pyproject에 build-system 없이 `[tool.uv] package = false`로 의존성만 설치(현행 `src/` 레이아웃 유지)
- **livekit-agents + LiveKit Inference(STT·TTS) + Bedrock(LLM)** — STT·TTS·VAD는 LiveKit Cloud Inference, LLM은 `livekit-plugins-aws`(`aws.LLM`, Converse Stream)로 Bedrock Claude를 사용(worker와 동일 AWS 계정). 리전은 서울 + `global.` 크로스리전 프로파일(2026-07-31 실호출 탐침 — 서울은 global만 제공, 도쿄 jp.는 SCP 명시 거부, 지연은 us-east-1과 동급). 모델 구성은 `agent/src/config.py` 상수로, 교체 시 상수만 변경. 전환기 한정 `KKORI_LLM_PROVIDER=inference` 토글로 구 LiveKit Inference LLM 경로 병행(안정화 후 토글·inference LLM 경로 제거 예정, 팩토리는 `src/llm_factory.py`)
- **Orchestrator 구조화 출력은 강제 tool 호출** — `response_format`은 프로바이더 공통 지원이 아니어서(Bedrock Converse 미지원), 판단 스키마를 tool 파라미터로 강제하고(`tool_choice`로 해당 tool 지정) 인자 JSON을 파싱한다. 실패는 기존과 동일하게 폴백 수렴
- **명시 디스패치** — `agent_name="kkori-interviewer"`(`config.AGENT_NAME`, Spring `createDispatch`와 동일 값)로 등록해 자동 입장을 껐다. 운영 입장은 Spring이 세션 생성 시 `createDispatch(agentName, metadata)`로 수행. 로컬 검증: console 모드는 무영향, dev 모드는 `lk dispatch create --room <룸> --agent-name kkori-interviewer --metadata '...'`로 입장시킨다(픽스처 `KKORI_*_FIXTURE`는 metadata 없는 dispatch 전용)
- **재연결 미처리(현재)** — 참가자 퇴장 시 세션 즉시 종료(기본값). `close_on_disconnect=False` + 재연결 창 처리는 INTERRUPTED 상태 스토리 범위

## 브랜치 / PR 규칙

- **기본 브랜치는 `develop`** (통합 지점), `main`은 배포 전용
- 작업은 `feature/HBB1-<지라번호>-<영문 요약>` 브랜치 → develop PR — 브랜치는 **스토리(상위 이슈)** 키를 사용(Jira 자동 전환이 스토리에 걸림), PR은 구현 단위인 서브 이슈를 `Closes #<이슈번호>`로 연결. 브랜치 키와 PR 이슈 키가 다를 수 있는 게 정상
- 브랜치 접두사는 축약형이 아닌 전체 단어 사용 (`feat/` ❌ → `feature/` ✅)
- **PR은 항상 draft로 생성**, 준비되면 ready 전환
- PR 제목은 `<타입>: [HBB1-<지라번호>] <요약>` 형식 (예: `feat: [HBB1-263] livekit-agents 및 기본 플러그인 연동`) — 지라 키가 제목에 있으면 GitHub for Atlassian이 티켓에 자동 연결
- PR 본문은 템플릿(관련 이슈 / 실행 검증 / PRD 경로 / 완료 조건) 준수 — 완료 조건은 PRD에서 발췌한 검증 가능한 문장으로 작성하고, 체크는 검증된 후에만
- CodeRabbit이 develop 대상 PR을 자동 리뷰 (draft는 제외 — ready 전환 시점에 리뷰 시작, 이후 커밋은 증분 리뷰). 재리뷰가 필요하면 `@coderabbitai review` 코멘트
- CI(GitHub Actions)는 agent/worker 경로별 워크플로우가 main/develop 대상 push·PR에서 실행 — agent는 `uv sync --locked` + import 검증 + pytest, worker는 `uv sync --locked` + 서비스 컨테이너(Postgres/Redis/MinIO) 기동 후 pytest 전체 실행

## 문서 참조 맵

| 필요한 정보 | 참조 문서 |
|---|---|
| 도메인 기능 요구사항, 정책, 검증 기준 | `docs/prd/<도메인>.md` (예정 — 폴더 아직 미생성) |
| worker(이력서 분석·리포트 생성) 요구사항·평가 기준 | `worker/docs/requirements/<도메인>/` (예: `resume-analysis/pipeline.md`, `report-evaluation/evaluation-criteria.md`) |

- `docs/drafts/` — 확정 전 초안 (gitignore, 커밋 금지). PRD가 생기기 전까지 설계 맥락이 필요하면 이 초안을 참조하되, 확정 문서가 아님을 전제할 것
- 이슈/PR에서 PRD 참조 시 섹션 번호까지 명시 (예: `docs/prd/interview.md §7.1`)
- PRD는 CodeRabbit도 리뷰 컨텍스트로 참조하므로 요구사항 변경 시 반드시 문서를 먼저 갱신할 것
