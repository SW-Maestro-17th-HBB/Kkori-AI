-- interview_metrics — 파이프라인 메트릭 원본 저장(성능 개선 재료).
-- STT·LLM·TTS·VAD·EOU metrics_collected 이벤트를 이벤트당 1행 jsonb로 쌓는다.
-- 원본 우선 방침 — 자주 조회하는 필드의 컬럼 승격은 수집 안정화 후 후속 마이그레이션.
--
-- 세션 단위 UNIQUE 없음: 복구 재디스패치로 같은 session_id에 잡 여러 개의 행이
-- 쌓이는 게 정상. 재시도 멱등성은 배치 키가 담당한다 — batch_id(=잡 ID, 잡마다
-- 유일·재시도 간 불변) + ordinal(배치 내 순번)의 UNIQUE와 ON CONFLICT DO NOTHING이
-- commit 후 응답 유실 재시도의 전량 중복을 차단한다.
CREATE TABLE IF NOT EXISTS interview_metrics (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id BIGINT NOT NULL,
    batch_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (batch_id, ordinal)
);

CREATE INDEX IF NOT EXISTS interview_metrics_session_id_idx
    ON interview_metrics (session_id);
