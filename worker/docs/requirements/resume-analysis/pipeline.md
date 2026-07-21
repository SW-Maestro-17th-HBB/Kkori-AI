# 이력서 분석 파이프라인 — Worker PRD

> Kkori AI Worker의 이력서 분석 파이프라인 요구사항. 이 문서는 **자기완결**이다 —
> 워커 개발자·리뷰어는 백엔드 레포를 열지 않고도 이 문서만으로 계약과 동작을 이해할 수 있어야 한다.
> 계약(스트림 메시지·상태·StructuredData)의 **변경 권한은 백엔드**에 있다. 이 문서의 계약 서술이
> 백엔드(`Kkori-Backend` `docs/requirements/resume/resume.md` + 계약 record)와 어긋나면 **백엔드가 우선**한다.
> 계약이 어긋나는 것은 §7 계약 예시 대조 테스트로 막는다.

## Overview

Worker는 Spring 백엔드가 발행한 이력서 분석 요청을 Redis Stream에서 소비하여, PDF를 구조화
데이터·검색 색인으로 변환하는 비동기 처리기다. Spring은 업로드 검증·저장·상태 조회·SSE 중계를
담당하고, 실제 분석(텍스트 추출 → LLM 구조화 → 청킹 → 임베딩 → pgvector 색인)은 Worker가 수행한다.

```
[Spring] --XADD--> (resume.parse.requested) --XREADGROUP--> [Worker]
                                                              │  S3/MinIO 다운로드
                                                              │  AI 파이프라인
                                                              │  PostgreSQL/pgvector 직접 기록
[Spring] <--소비-- (resume.parse.status.changed) <--XADD-- (단계마다)
   └─ SSE로 프론트에 실시간 push
```

- 스택: **FastStream(Redis 브로커) + uv + Python 3.12**
- 연동 인프라: Redis 스트림 2개, PostgreSQL(pgvector), S3/MinIO, AWS Bedrock(LLM·임베딩)
- 불변 원칙: **모든 분석 요청은 반드시 `EMBEDDED` 또는 `FAILED`로 종결한다.**

---

## 1. 계약 (자기완결 사본)

Redis Stream 필드는 **전부 문자열**이다. 아래 인코딩 규칙이 Java 계약 record와의 유일한 접점이다.

### 1.1 요청 메시지 — `resume.parse.requested` (소비)

| 필드 | 논리 타입 | 직렬화 | 비고 |
|---|---|---|---|
| `resumeId` | Long | `str(long)` | |
| `userId` | Long | `str(long)` | 상태 메시지에 **에코** (SSE 사용자 라우팅 근거) |
| `bucket` | String | 그대로 | S3/MinIO 버킷. REINDEX에선 무시 |
| `objectKey` | String | 그대로 | 객체 키. REINDEX에선 무시 |
| `mode` | enum | `FULL` \| `REINDEX` | 5필드 전부 필수 — 모드 무관 |

- 5필드는 mode와 무관하게 **전부 필수**(조건부 스키마 회피). REINDEX에서 bucket/objectKey는 존재는
  하되 Worker가 무시한다.

### 1.2 상태 메시지 — `resume.parse.status.changed` (발행)

| 필드 | 논리 타입 | 직렬화 | 비고 |
|---|---|---|---|
| `resumeId` | Long | `str(long)` | |
| `userId` | Long | `str(long)` | 요청 메시지의 값을 **그대로 에코** |
| `status` | enum | 아래 8종 | |
| `message` | String? | **null → 빈 문자열 `""`** | 실패 사유 등 status로 유도 불가한 정보 |

### 1.3 상태 8종 (`AnalysisStatus`)

`UPLOADED → PARSING → TEXT_EXTRACTING → STRUCTURING → PARSED → EMBEDDING → EMBEDDED`, 실패 시 `FAILED`.

- `UPLOADED`는 **Spring이 업로드 시점에** 기록한다. 이후 전이는 **전부 Worker**가 기록·발행한다.
- 각 단계 **진입 시** 상태를 갱신하고 상태 이벤트를 발행한다.

### 1.4 StructuredData 스키마 (`resumes.structured_data` jsonb)

```
StructuredData {
  profile:     { name, email }
  skills:      [ { category, items: [string] } ]
  projects:    [ { name, role, description, techStacks: [string] } ]
  experiences: [ { title, description } ]
}
```

- 쓰기: Worker의 LLM 구조화 결과. 읽기: Spring 조회·수정 API, **Worker의 REINDEX 입력**.
- 검증 방침 (형태 엄격·내용 관대): unknown 필드 무시, 필드 누락·빈 배열 허용, 단 **배열 내 null 요소는
  거부**(청킹 단계에서 오류를 유발하므로).

### 1.5 계약 권한

계약을 바꿀 권한은 **백엔드**(발행자·상태 엔티티 소유자)에 있다. Worker는 소비자로서 따른다.
백엔드가 필드를 바꾸면 Worker가 맞춘다(역방향 불가). 어긋나면 백엔드 record + `resume.md`가 우선.

---

## 2. 파이프라인 (FULL / REINDEX)

진입 지점은 **DB 상태가 결정**하고(§3 재개 표), `mode`는 **cross-check**로만 쓴다.

### 2.1 FULL — 전체 분석
S3 원본 PDF를 기준(source of truth)으로 삼아 처음부터 전부 수행. 신규 업로드와 FAILED 복구가 여기 해당.

| 단계 | 진입 시 발행 status | 작업 |
|---|---|---|
| 1 | `PARSING` | 요청 수신·멱등 체크 후 시작 |
| 2 | `TEXT_EXTRACTING` | S3 다운로드 → **PyMuPDF** 텍스트 추출 (**원문 저장 안 함**) |
| 3 | `STRUCTURING` → `PARSED` | **LLM 구조화** → `structured_data` 저장 → (커밋 후) `PARSED` |
| 4 | `EMBEDDING` | 엔티티 기반 청킹(§2.5) + 청크 metadata → **임베딩** → `resume_chunks` 저장 |
| 5 | `EMBEDDED` | 최종 |

- **빈 텍스트 추출은 `FAILED`로 종결한다.** PyMuPDF는 텍스트 레이어만 읽으므로 **이미지-only(스캔) PDF**는
  빈 문자열이 나온다. 이를 빈 `structured_data`로 그대로 진행하면 정상적인 "0청크 EMBEDDED"(§2.5)와 구분되지
  않아 **오분석을 걸러내지 못한다.** 추출 텍스트가 비면(공백 제거 후 길이 0) 구조화로 진행하지 말고 `FAILED`
  (error_message = `"텍스트 추출 실패(이미지-only PDF 가능성, OCR 미지원)"`)로 명확히 실패 처리한다. OCR 지원은 추후(§10).

### 2.2 REINDEX — 재색인만
DB의 `structured_data`(사용자 수정본)를 기준(source of truth)으로 삼아 **청킹 → 임베딩 → 색인만** 재수행. 텍스트 추출·구조화는
**건너뛴다**(재수행하면 LLM이 사용자 수정을 덮어씀). 상태는 `EMBEDDING → EMBEDDED`.

### 2.3 mode cross-check 및 진입 규칙

기본 규칙: **진입은 DB 상태가 결정, mode는 cross-check.**

- **런의 초기 상태는 Spring이 발행과 같은 트랜잭션에서 세팅한다 (확정)** — FULL→`UPLOADED`, REINDEX→`EMBEDDING`
  (백엔드 `restartFor()` 구현). 따라서 Worker는 순수 상태 기준을 유지한다: `EMBEDDED`는 **항상 "완료 → 스킵"**
  이고, REINDEX는 `EMBEDDING`으로 진입하므로 §3.1 표의 EMBEDDING 행(청크 정리 후 재임베딩)이 그대로 적용된다.
  - 이 설계 덕분에 **REINDEX 중복 전달도 자연히 멱등** — 완료되면 `EMBEDDED`가 되어 이후 중복은 스킵된다.
  - "Spring은 UPLOADED만 기록" 원칙은 "Spring은 **런의 초기 상태**를 기록"으로 정리됨. UPLOADED 이후의
    파이프라인 전이는 여전히 전부 Worker.

- **계약 위반은 묵인하지 않는다 (cross-check):** mode와 상태가 규약과 어긋나면(예: `mode=REINDEX`인데
  상태가 `UPLOADED` = structured_data 없음, 또는 `mode=FULL`인데 진입 상태가 `EMBEDDING`) FULL로 진행하면
  LLM 구조화가 **사용자 수정을 덮어쓰는** 등 데이터 오염이 생긴다. 경고만 남기고 진행하지 말고
  **`FAILED`로 종결**(error_message에 계약 위반 사유 명시)하여 문제를 명확히 드러낸다.

### 2.4 재개 불변식 2개 (필수)

1. **산출물 커밋 후 상태 갱신** — `structured_data`를 저장한 **뒤에** 상태를 `PARSED`로. 순서가 반대면
   "PARSED인데 데이터 없음"이 생겨 체크포인트 재개가 깨진다. (청크도 동일: 저장 후 `EMBEDDED`.)
2. **재개 분기는 상태 컬럼으로만, 산출물 존재로 판단 금지** — FULL 재분석 시 백엔드가 상태를 `UPLOADED`로
   리셋하지만 **이전 런의 `structured_data`는 DB에 남아 있다.** 산출물을 보고 "임베딩부터"로 판단하면
   **낡은 데이터로 색인하는 버그**. 항상 상태 컬럼만 본다.

### 2.5 청킹 전략 — 성과 단위 세분화 (2026-07-21 갱신)

> **갱신**: 엔티티(프로젝트)=청크에서 **성과 단위 청킹**으로 세분화. 실제 이력서 A/B 실험에서
> 프로젝트 하나에 성과 여러 개가 뭉치면 임베딩이 평균으로 뭉개져 검색 변별력이 떨어짐을 확인
> (1·2위 격차: 엔티티 0.049 vs 성과 단위 0.070, +43%). 프로젝트 description 을 **LLM(§2.6 호출에
> 통합)이 소개/성과로 분리**하고, **성과 1개 = 청크 1개**(헤더+소개를 문맥으로 부착)로 만든다.
> 분할이 비면(짧은 설명) 기존 엔티티=청크로 폴백. 경력·스킬은 기존 방식 유지.
> 전체 프로젝트 문맥이 필요하면 `metadata.source_index` 로 `structured_data` 원문을 조회한다
> (계층형 저장 없이 부모 문맥 확보). `chunk_version = 3`.

청킹 입력은 **raw text가 아니라 `structured_data`**다 — 이미 profile/skills[]/projects[]/experiences[]로
의미 단위 구조화된 JSON(원문 텍스트는 저장하지 않음, §2.1). 따라서 고정 길이 슬라이딩 윈도우(구조를
버리는 raw text용 기법)가 아니라 **엔티티 경계를 그대로 청크 경계로 삼는다.**

- **1엔티티 = 1청크**: 프로젝트당 1개, 경력(experience)당 1개, 스킬 카테고리당 1개.
  `profile`(name/email)은 **임베딩 제외** — 면접 질문 소스가 아니라 이력서 식별 metadata일 뿐.
- **자기완결 content**: 각 청크는 단독으로 읽혀도 뜻이 통하게 라벨을 앞에 붙여 직렬화한다.
  예: `[프로젝트] {name} · 역할 {role}\n{description}\n기술: {techStacks join ', '}`.
- **오버플로 분할**: 목표 크기(Cohere 권장 길이 ~512토큰) 초과 엔티티만 **문장 경계**로 나누고,
  각 조각 앞에 **엔티티 헤더를 다시 붙임** + 조각 간 **소량 오버랩(≈1문장)**. 엔티티 **간** 오버랩은 없음.
- **빈 것은 청크 생성 안 함**: 빈 배열·빈 엔티티는 건너뛴다. 모든 엔티티가 비면 **0청크로 `EMBEDDED` 종결**
  (실패 아님 — §2.1 불변 원칙과 정합).
- **metadata**: `type`(project|experience|skill) · `source_index`(원본 배열 위치) · `label`(엔티티명/카테고리) ·
  **`chunk_version`**(청킹 스키마 버전 — 향후 전략 교체 시 백필 대상 식별용, §8).
- **교체 가능성**: 청킹은 순수 Worker 내부 로직이라 계약·임베딩 차원을 건드리지 않는다. 전략을 바꾸면
  **REINDEX 한 바퀴로 백필**(구버전 `chunk_version` 대상)하면 되고 DDL 마이그레이션이 없다 — 그래서 첫 버전을
  과튜닝하지 않는다. 검색 품질을 실측하며 파라미터(목표 크기·오버랩)나 계층형(parent-child)으로 승급.

### 2.6 청크 풍부화(enrichment) — 확정 (2026-07-21)

청크를 저장하기 전에 **LLM 이 청크들을 읽고 부가 정보를 뽑아 metadata 에 병합**한다. 질문 생성(agent)의
재료를 색인 시점에 미리 준비하고, 엔티티 청크의 "여러 주제 뭉개짐"(실측)을 보조 검색으로 보완하기 위함.

- **필드 3종**: `topics`(주제 명사구) · `relatedConcepts`(기술 개념) · `questionHints`(질문 소재).
  전부 청크에 실제 근거가 있는 것만(지어내기 금지).
- **호출 단위: 이력서당 1회** — 구조화 데이터 전체를 한 프롬프트에 넣어 **성과 분할(§2.5)과 풍부화를
  한 번에** 수행(호출 수 절약 + 표기 일관). 배열 길이·순서가 입력과 다르면 오류로 취급(재시도 대상).
- **필수 단계** — 위치는 청킹 직전(EMBEDDING 단계 내부, 새 상태 없음 — 분할 결과가 청크를 결정하므로).
  실패는 구조화와 동일하게 내부 재시도 → 소진 시 전파(재전달 경로). 부분 상태를 만들지 않는다.
- **임베딩 입력은 content 그대로** — 풍부화 결과를 임베딩에 섞는 것은 효과 불확실로 보류(§10).
- `metadata.chunk_version = 2` 로 상향(색인 스키마 버전) — v1 청크는 향후 REINDEX 백필 대상 식별 가능.

---

## 3. 복구·재개 (XAUTOCLAIM)

Worker가 XACK 전에 죽으면 메시지가 PEL(Pending Entries List)에 남는다. Worker는 ACK 없이 오래
방치된(idle) 메시지를 **XAUTOCLAIM으로 회수**해, **DB 상태 기준 체크포인트에서 재개**한다.

### 3.1 상태별 재개 지점

| 회수 시 DB 상태 | 재개 동작 |
|---|---|
| `EMBEDDED` | **스킵 후 ACK** (이미 완료 — at-least-once 중복) |
| `FAILED` | **스킵 후 ACK** (종결 상태) |
| `EMBEDDING` | 기존 청크(`resume_id` 기준) **정리 후 재임베딩** |
| `PARSED` | **임베딩부터** (structured_data 존재) |
| `UPLOADED`/`PARSING`/`TEXT_EXTRACTING`/`STRUCTURING` | **처음부터** (원문 미저장이므로 추출부터) |

- **스킵(EMBEDDED/FAILED) 시 상태 이벤트를 재발행하지 않는다** — 이미 종결 이벤트가 나갔고, SSE 유실 복구는 REST 담당.
- **임베딩 단계 진입 시 항상 `resume_id` 기준 기존 청크를 먼저 삭제**한 뒤 재생성한다(EMBEDDING 재개뿐 아니라
  REINDEX·부분 실패 재개 전반의 일반 규칙 — 중복 청크·삭제되지 않고 남은 청크 방지).
- `EMBEDDED`는 항상 완료를 의미한다 — REINDEX는 `EMBEDDING`으로 진입하므로(§2.3 확정) 이 행에 mode 분기가 필요 없다.

### 3.2 통일 진입 로직 (권장)
파이프라인 **진입 지점 결정**은 신규·회수 메시지를 하나의 로직으로 통일한다 — 항상 DB 상태를 읽고 결정.
단, **카운터 처리(§6)는 출처에 따라 다르다** — 신규(XREADGROUP `>`)는 새 런이라 `retry_count`를 0으로 리셋,
회수(XAUTOCLAIM)는 같은 런의 재개라 리셋하지 않는다. 진입 라우팅은 통일하되 이 구분은 유지.

### 3.3 동시·중복 처리 가드 (원자적 상태 전이)

at-least-once + XAUTOCLAIM 회수는 **실제로 죽지 않은 원본과 회수본이 경합**할 수 있다("resumeId 멱등"만으론
이중 처리를 못 막음). 방어:

- 각 단계 진입은 **원자적 상태 CAS**로 한다 — 예: `UPDATE ... SET parse_status='PARSING' WHERE resume_id=? AND parse_status='UPLOADED'`.
  영향 행이 0이면 다른 처리자가 이미 앞서간 것이므로 **재처리하지 않고 넘어간다**.
- 산출물 저장(§2.4 불변식 1)과 상태 전이를 **같은 트랜잭션**으로 묶어 부분 반영을 없앤다.

---

## 4. 포기 규칙 (delivery count 임계)

메시지 수신·회수 **직후, 처리 시작 전**에 delivery count를 확인한다. 임계(기본 **3**) 이상이면 재처리 없이:

1. DB에 **`FAILED` 기록** → `failed_at` 기록. `error_message` 는 `"재전달 임계 초과(delivery count=N)"` 에
   **기록된 마지막 실패 원인을 덧붙인다**(아래 원인 기록 참조) — 운영·디버깅용.
2. **XACK**

- **이 순서 고정.** 중간에 죽어도 재전달받은 쪽이 같은 경로를 다시 타면 수렴한다(①은 멱등 — 이미 FAILED면 그대로).
- **실패 원인 기록**: 내부 재시도 소진 시(§6)와 예상 밖 예외 시, 마지막 오류 요약을 `error_message` 에
  **best-effort 로 기록**해 둔다(상태는 유지). 포기 시점에 이 기록을 합류시켜 DB 에서 원인 추적이 가능하다.
  단 **상태 이벤트(SSE)의 message 는 간단 문구만** — 내부 예외 문구를 사용자 화면에 노출하지 않는다.
  DB 자체가 죽은 경우는 기록 불가(원리적 한계) — 워커 로그가 담당.
- `FAILED`는 Worker가 내부 재시도를 소진했거나 재전달 임계를 초과한 **끝 상태**다. 서버는 자동 재시도하지
  않으며, 복구는 항상 사용자의 §재분석 API가 유일 경로.

---

## 5. 별도 DLQ 스트림은 두지 않는다 (확정)

- 메시지의 모든 필드는 **DB에서 다시 조회할 수 있는 참조값(resumeId 등)**이라, 격리 보존할 고유 정보가 없다.
- 재처리는 스트림 재주입이 아니라 **재분석 API가 DB에서 새 메시지를 생성**하는 방식.
- 격리 건은 **`FAILED` 레코드가 겸한다** — 일반 실패와 동일 테이블에서 구분·조회 가능.

---

## 6. retry_count 운용

`resume_analysis_status.retry_count`는 **Worker 소유**, 서버는 읽기 전용.

> **두 카운터를 혼동하지 말 것** — §4 포기 규칙이 보는 것은 Redis PEL의 **delivery count**(메시지가 몇 번
> 전달됐나, Redis가 관리)이고, 여기 `retry_count`는 DB에 기록하는 **런 내부 재시도 횟수**(일시 오류 재시도,
> Worker가 관리)다. 서로 파생되지 않는 별개 값이다.

- 매 (내부) 시도 **즉시 DB 반영** (크래시 생존성·관측성).
- **새 런(신규 메시지, XREADGROUP `>`) 시작 시 0으로 리셋** — 회수(XAUTOCLAIM) 재개는 같은 런이므로 리셋하지 않음.
- 의미 = **"현재 분석 런의 시도 횟수"**.
- 서버는 재분석 시에도 **초기화하지 않는다** (리셋 시점·의미는 Worker 소관).

---

## 7. 계약 불일치 방어 — 계약 예시 대조 테스트

크로스 레포라 Worker CI·리뷰는 백엔드를 못 본다. 계약 사본이 알아채지 못한 채 어긋나는 것을 **테스트로 막는다**.

- 실제 스트림 필드맵 예시(JSON)를 워커 repo에 **계약 예시 파일**로 커밋한다.
- 테스트: **요청 decode**(문자열 필드맵 → pydantic 모델) + **상태 encode**(모델 → 문자열 필드맵) **양방향**을
  이 예시 파일과 정확히 대조한다. 계약이 바뀌면 예시 파일도 바뀌고, 그 변경이 PR에 그대로 드러난다.
- MVP: 예시 파일은 백엔드 record를 보고 손으로 작성해 계약 기준으로 삼는다.

---

## 8. AI 제공자 / 인프라 설정

- **구조화 LLM: Claude Haiku 4.5 (Bedrock) — 확정.** tool-use/JSON 스키마 강제로 §1.4 StructuredData 형태를 정확히 출력.
  구조화 품질(한국어 파싱)이 부족하면 Sonnet 4.6으로 교체(스키마·DB 영향이 0이라 위험 없는 교체). 프롬프트 캐싱으로
  반복되는 스키마/시스템 프롬프트 입력 비용 절감.
- **임베딩: Cohere Embed v4 (Bedrock 서울, `cohere.embed-v4:0`, 출력 차원 1024 지정) — 확정(v3 에서 변경).**
  이력서 RAG의 본질이 **한국어 검색 품질**이라 다국어 강한 Cohere 계열 유지. 당초 v3 로 확정했으나 서울 리전에
  v4 만 제공되어 상위 호환인 v4 로 변경(2026-07-21) — **출력 차원을 1024 로 지정**해 `vector(1024)` 스키마는
  그대로 유지한다. 색인은 `input_type=search_document`, 질의 시 `search_query`로 **비대칭 임베딩** 사용(동일 지원).
- 인증: 단일 IAM 원칙(백엔드 dev/prod S3 IAM Role 패턴과 일치).
- **로컬 주의**: Bedrock은 로컬 에뮬레이터가 없다(MinIO=S3 로컬과 다름). 실제 제공자를 로컬에서 쓰면 실제 Bedrock
  호출 → AWS 자격증명·리전 필요, 로컬에서도 과금.
- **리전: us-east-1 (조직 정책상 강제)**(2026-07-21 실 호출 탐침으로 확정). 당초 서울(ap-northeast-2)을
  원했으나, 계정이 소속된 조직(SW마에스트로 발급)의 **SCP 가 Bedrock 호출을 us-east-1 에서만 허용**한다 —
  서울의 모든 모델·`global.`/`apac.` cross-region 프로파일 전부 명시적 거부(실측). 호출 ID:
  Claude = `us.anthropic.claude-haiku-4-5-20251001-v1:0`, 임베딩 = `cohere.embed-v4:0`(직접 ID 허용).
  데이터 국내 상주는 조직 정책상 불가 — 인지하고 수용.
- **AI 제공자 추상화 (테스트·클라우드 독립)**: 구조화(LLM)·임베딩 호출을 각각 **인터페이스**로 정의하고 구현 2개를 둔다.
  - **가짜(fake) 제공자** — 로컬 개발·단위 테스트용. 클라우드/과금 없이 **결정적 값**을 반환한다(가짜 구조화기 = 정해진
    `StructuredData`, 가짜 임베딩기 = 1024차원 결정적 더미 벡터, 예: 텍스트 해시 기반).
  - **실제 Bedrock 제공자** — 실서버·통합 검증용. 클라우드 준비 후 연결.
  - 파이프라인 로직은 **인터페이스에만 의존** → Bedrock 없이 파이프라인 전체를 개발·테스트하고, 준비되면 설정으로 실제
    제공자만 연결(파이프라인 코드 무변경). 단위 테스트는 가짜로 우리 로직(청킹·상태 전이·재개·저장)을 검증하고,
    **모델 품질·실제 Bedrock 요청 형식**은 클라우드 준비 후 **통합 테스트(실 Bedrock 1회 호출)**로 별도 확인한다.
- **스키마 소유권**: `resume_chunks` 테이블(`content`, `metadata`, `embedding vector(1024)`) + `CREATE EXTENSION vector`는
  **Worker가 소유**(유일 writer이자 임베딩 차원의 주인). 차원 1024는 Cohere v3 확정에 종속 — 모델 교체 시 여기만 조정.
  백엔드는 `resumes`/`structured_data` 소유. 기동 시 멱등 DDL(`IF NOT EXISTS`).
- **시각 컬럼은 UTC-aware로 기록**: 백엔드 소유 `resume_analysis_status`의 `started_at`/`completed_at`/`failed_at`은
  **`timestamptz`(UTC)**다(백엔드 HBB1-232 확정). 워커는 타임존 포함 UTC(aware datetime, 예: `datetime.now(timezone.utc)`)로
  기록한다 — naive datetime 금지.

---

## 9. Worker 재량 파라미터

전부 Worker 설정값이며 백엔드 계약과 무관하다.

| 항목 | 기본(예시) | 비고 |
|---|---|---|
| delivery count 임계 | 3 | §4 포기 규칙 |
| idle time(회수 대상 판정) | 5~10분 | XAUTOCLAIM min-idle |
| 내부 재시도·백오프 | 지수 백오프 | 일시 오류(Bedrock throttle·네트워크)용 |
| heartbeat | **MVP 생략 가능** | idle time + delivery count로 충분 |
| Consumer Group / consumer | `kkori-worker` / 인스턴스 ID | 기동 시 `XGROUP CREATE ... MKSTREAM` 멱등 생성(스트림 선존 여부 무관) |

---

## 10. 열린 결정

- 청킹 목표 크기·오버랩의 **구체 수치** 튜닝(§2.5는 전략을 확정, 파라미터는 실측으로 조정).
- 프롬프트 캐싱·배치 적용 범위(비용 최적화) — 준실시간 스트림이라 배치는 부적합 가능.
- **OCR / 이미지-only PDF 지원(추후)** — 현재는 빈 추출을 `FAILED`로 종결(§2.1). 스캔 이력서 수요가 확인되면
  OCR 단계를 텍스트 추출에 추가.

## 결정 이력

> 근거·트레이드오프 상세는 `worker/docs/drafts/worker-design-decisions.md`(walkthrough 노트)에 기록. 여기엔 확정 사실만.

- 2026-07-15 REINDEX 런 진입 상태: Spring이 발행과 같은 트랜잭션에서 세팅(FULL→UPLOADED, REINDEX→EMBEDDING),
  `restartFor()` 구현. → §2.3.
- 2026-07-15 AI 모델 확정: 구조화 LLM = **Claude Haiku 4.5(Bedrock)**, 임베딩 = **Cohere Embed Multilingual v3
  (`vector(1024)`)**. 근거 §8. 구조화 품질 부족 시 Sonnet 4.6으로 교체(스키마·DB 영향 0으로 위험 없음). → §8.
- 2026-07-16 청킹 전략 확정: **엔티티 기반 + 오버플로 분할(C)**. 입력이 구조화 데이터라 엔티티 경계=청크 경계.
  `chunk_version` metadata로 향후 교체 시 REINDEX 백필. 근거·대안 비교 → §2.5 및 drafts 노트.
- 2026-07-16 이미지-only(스캔) PDF: OCR **추후 지원**. 현재는 **빈 텍스트 추출 → `FAILED`**로 명확히 실패 처리
  ("0청크 EMBEDDED"와의 혼동 방지). → §2.1, §10.
- 2026-07-16 백엔드 HBB1-232 확정 전달: (1) REINDEX 진입 상태 세팅 `ResumeAnalysisStatus.restartFor` 구현·PR#44 머지 예정,
  (2) AI 모델 = Haiku 4.5 + Cohere v3(둘 다 기반영), (3) 백엔드 `StructuredData` 경로 `dto/`→`domain/`(내용 동일, 워커 무영향),
  (4) `resume_analysis_status` 시각 컬럼 `timestamp`→**`timestamptz`(UTC)** → 워커는 UTC-aware로 기록. → §8.
- 2026-07-21 청킹 세분화: **성과 단위 청킹 확정** — 실제 이력서 A/B 실험(격차 +43%, 성과 문장 정밀 조준)을
  근거로 채택. 분할은 §2.6 LLM 호출에 통합(경계 판단은 내용 이해가 필요), 짧은 설명은 엔티티=청크 폴백,
  부모 문맥은 source_index→structured_data 조회. `chunk_version = 3`. → §2.5.
- 2026-07-21 청크 풍부화(enrichment) 도입: **topics·relatedConcepts·questionHints** 를 이력서당 1회 LLM 호출로
  추출해 metadata 병합(필수 단계, chunk_version 2). 포기 시 error_message 에 **마지막 실패 원인 합류**
  (DB 상세 / SSE 간단 분리). → §2.6, §4.
- 2026-07-21 Bedrock 리전·모델 확정: 임베딩은 v3 미제공으로 **Embed v4(`cohere.embed-v4:0`)로 변경**(출력 차원
  1024 지정, `vector(1024)` 스키마 유지). 리전은 서울로 정했다가 **같은 날 번복 — 조직 SCP 가 us-east-1 만
  허용**(서울·cross-region 프로파일 전부 명시적 거부, 실 호출 탐침으로 확인) → **us-east-1 확정**.
  Embed v4 실 호출 검증 완료(1024차원 + 의미 유사도 동작). → §8.
