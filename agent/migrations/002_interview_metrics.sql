-- interview_metrics — 파이프라인 메트릭 원본 저장(성능 개선 재료).
-- STT·LLM·TTS·VAD·EOU metrics_collected 이벤트를 이벤트당 1행 jsonb로 쌓는다.
-- 원본 우선 방침 — 자주 조회하는 필드의 컬럼 승격은 수집 안정화 후 후속 마이그레이션.
--
-- transcript와 달리 UNIQUE 없음: 복구 재디스패치로 같은 session_id에 잡 여러 개의
-- 행이 쌓이는 게 정상이고, 행 단위 멱등 키가 없다. 재시도 중복은 commit 후 응답
-- 유실 경계의 전량 중복뿐이라(단일 트랜잭션 배치) 원본 로그로서 허용한다.
CREATE TABLE IF NOT EXISTS interview_metrics (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id BIGINT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS interview_metrics_session_id_idx
    ON interview_metrics (session_id);
