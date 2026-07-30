-- interview_transcript — transcript 소유자인 agent가 DDL·마이그레이션을 소유한다
-- (docs/prd/interview-end.md §4 — 쓰기 권한 경계 = 소유권 경계).
--
-- session_id의 FK 대상(interview_session)은 Spring 소유 테이블이므로 FK 제약은
-- 걸지 않는다 (확정 — Spring이 user_id·resume_id도 무FK로 가는 방침과 일관).
-- 세션당 1행(UNIQUE)이 중복 flush 멱등성(ON CONFLICT DO NOTHING)의 근거다.
CREATE TABLE IF NOT EXISTS interview_transcript (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id BIGINT NOT NULL UNIQUE,
    content JSONB NOT NULL,
    deleted_at TIMESTAMPTZ
);
