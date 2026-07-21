# CLAUDE.md

## 프로젝트 개요

Kkori — AI 면접 준비 서비스의 AI 컴포넌트 (SW마에스트로 팀 HBB). Python 3.13 / uv workspace 모노레포.

- `agent/` (`kkori-agent`) — LiveKit 음성 에이전트. 면접 실시간 루프(STT→LLM→TTS) 담당
- `worker/` (`kkori-worker`) — 비동기 평가 워커. 리포트용 상세 평가 (구현 예정)
- `shared/` — 공용 코드. 아직 미사용 — 실제 import가 생기는 시점에 워크스페이스 멤버로 승격

## 명령어

```bash
uv sync                                        # 루트 .venv 생성/동기화 (개발 전 1회)
cd agent && uv run python -m src.main console  # 터미널 마이크로 에이전트와 직접 대화 (로컬 검증)
cd agent && uv run python -m src.main dev      # LiveKit Cloud에 워커 등록 (Agents Playground로 테스트)
uv add --project agent <패키지>                 # 의존성 추가 (uv.lock 자동 갱신)
docker build -f agent/Dockerfile -t kkori-agent .    # 이미지 빌드 — 컨텍스트는 반드시 레포 루트
docker build -f worker/Dockerfile -t kkori-worker .
```

- 에이전트 실행에는 `agent/.env`에 LiveKit Cloud 자격증명 필요 (`agent/.env.example` 참조, Git 비추적)
- 테스트 프레임워크는 아직 미도입 — CI는 pytest의 "수집된 테스트 없음"(exit 5)을 통과로 처리 중이며, 테스트 추가 시 워크플로우의 해당 처리를 제거할 것

## 작업 규칙

- 의존성 변경은 pyproject.toml 직접 편집 대신 `uv add/remove --project <서비스>` 사용, `uv.lock` 커밋 필수
- 코드 변경 후 최소한 import 검증(`cd <서비스> && uv run python -c "import src.main"`)을 통과할 것 — CI 첫 관문과 동일
- 커밋 메시지 타입은 `feat`, `fix`, `chore`, `docs`, `refactor`, `test`를 사용 (예: `feat: HBB1-263 bootstrap livekit voice agent`)

## 기술적 결정사항

- **uv workspace 단일 락파일** — 개발은 루트 venv 하나로 하되, 배포는 서비스별 독립 이미지. 각 Dockerfile이 `uv sync --frozen --package <이름>`으로 그 서비스 의존성만 설치하므로 배포 결합 없음. 멤버 pyproject에 build-system을 두지 않아 uv는 의존성만 설치(현행 `src/` 레이아웃 유지)
- **livekit-agents + LiveKit Inference** — 최종적으로 self-host SFU 예정이라 STT·LLM·TTS built-in이 없어 직접 연동이 필요하지만, 개발 단계에서는 LiveKit Cloud + Inference를 사용해 별도 프로바이더 API 키 없이 LiveKit 자격증명 하나로 동작. 모델 구성은 `agent/src/main.py` 상단 상수로, 교체 시 상수만 변경
- **자동 디스패치(임시)** — `agent_name` 미지정으로 모든 신규 룸에 에이전트가 자동 입장(테스트 편의). Spring이 `createDispatch(agentName)`로 명시 디스패치하는 방식은 세션 생성 스토리에서 전환
- **재연결 미처리(현재)** — 참가자 퇴장 시 세션 즉시 종료(기본값). `close_on_disconnect=False` + 재연결 창 처리는 INTERRUPTED 상태 스토리 범위

## 브랜치 / PR 규칙

- **기본 브랜치는 `develop`** (통합 지점), `main`은 배포 전용
- 작업은 `feature/HBB1-<지라번호>-<영문 요약>` 브랜치 → develop PR — 브랜치는 **스토리(상위 이슈)** 키를 사용(Jira 자동 전환이 스토리에 걸림), PR은 구현 단위인 서브 이슈를 `Closes #<이슈번호>`로 연결. 브랜치 키와 PR 이슈 키가 다를 수 있는 게 정상
- 브랜치 접두사는 축약형이 아닌 전체 단어 사용 (`feat/` ❌ → `feature/` ✅)
- **PR은 항상 draft로 생성**, 준비되면 ready 전환
- PR 제목은 `<타입>: [HBB1-<지라번호>] <요약>` 형식 (예: `feat: [HBB1-263] livekit-agents 및 기본 플러그인 연동`) — 지라 키가 제목에 있으면 GitHub for Atlassian이 티켓에 자동 연결
- PR 본문은 템플릿(관련 이슈 / 실행 검증 / PRD 경로 / 완료 조건) 준수 — 완료 조건은 PRD에서 발췌한 검증 가능한 문장으로 작성하고, 체크는 검증된 후에만
- CodeRabbit이 develop 대상 PR을 자동 리뷰 (draft는 제외 — ready 전환 시점에 리뷰 시작, 이후 커밋은 증분 리뷰). 재리뷰가 필요하면 `@coderabbitai review` 코멘트
- CI(GitHub Actions)는 agent/worker 경로별 워크플로우가 main/develop 대상 push·PR에서 `uv sync --frozen` + import 검증 + pytest 실행

## 문서 참조 맵

| 필요한 정보 | 참조 문서 |
|---|---|
| 도메인 기능 요구사항, 정책, 검증 기준 | `docs/prd/<도메인>.md` (예정 — 폴더 아직 미생성) |

- `docs/drafts/` — 확정 전 초안 (gitignore, 커밋 금지). PRD가 생기기 전까지 설계 맥락이 필요하면 이 초안을 참조하되, 확정 문서가 아님을 전제할 것
- 이슈/PR에서 PRD 참조 시 섹션 번호까지 명시 (예: `docs/prd/interview.md §7.1`)
- PRD는 CodeRabbit도 리뷰 컨텍스트로 참조하므로 요구사항 변경 시 반드시 문서를 먼저 갱신할 것
