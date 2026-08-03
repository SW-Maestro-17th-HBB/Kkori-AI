"""리포트 파이프라인 PostgreSQL 계층 — 로우 생성·상태 전이·산출물 저장.

소유권:
- `reports`·`report_scores`·`report_feedbacks` = **백엔드 소유**(스키마 정의 원천은
  Spring 엔티티) — 워커는 쓰기 주체일 뿐이다. 따라서 **테이블 구조 변경은 스트림
  메시지와 같은 "계약 변경"** 으로 취급한다: 백엔드 엔티티·이 파일·테스트 스키마
  (conftest)를 한 커밋 단위로 함께 갱신할 것.
- `report_generation_jobs` = **워커 소유** → 기동 시 멱등 DDL 생성 (resume_chunks 선례).
- `interview_session`·`interview_transcripts` = 면접 도메인 소유, 워커는 읽기 전용.
  `interview_session`(단수 명명)은 백엔드 develop 의 실물 엔티티에서 확인(2026-07-29).
  `interview_transcripts` 는 에이전트 구현(HBB1-287) 합의 전 잠정.

원칙 (이력서 repository.py 와 동일):
- 상태 전이는 원자적 CAS: `UPDATE ... WHERE status = 이전상태`. 영향 행 0 = 다른
  처리자가 앞서감 → 호출자는 재처리하지 않고 양보한다.
- 시각은 UTC-aware(timestamptz) 로 기록. naive datetime 금지.
- 리포트+Job 생성과 텍스트 산출물 저장은 함수 안에서 트랜잭션으로 묶는다 —
  "리포트가 있으면 Job 도 있다", "산출물은 전부 있거나 전무" 불변식.
- Spring 스키마(ddl-auto)에는 DB 기본값이 없으므로 created_at/updated_at 을 항상
  명시적으로 채운다.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from psycopg import AsyncConnection, errors
from psycopg.types.json import Jsonb
from pydantic import BaseModel

from src.contract import ImprovementTask, ReportStatus, Utterance, WeaknessTagCount

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------- 스키마 (워커 소유)

async def ensure_schema(conn: AsyncConnection) -> None:
    """워커 소유 `report_generation_jobs` 를 멱등 생성한다. 백엔드 소유 테이블은 건드리지 않는다.

    리포트와 1:1(report_id 유니크) — 생성은 리포트와 한 트랜잭션(create_report_with_job),
    재생성 시 Spring 이 requested_at 을 갱신한다. retry_count·error_message 는 워커의
    운영 기록(사용자 노출 실패 사유는 reports.failed_reason).
    """
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS report_generation_jobs (
            id            BIGSERIAL PRIMARY KEY,
            report_id     BIGINT NOT NULL UNIQUE,
            retry_count   INT NOT NULL DEFAULT 0,
            error_message TEXT,
            requested_at  TIMESTAMPTZ NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


# ---------------------------------------------------------------- 읽기 (멱등 판단·입력)

async def get_report_by_session(conn: AsyncConnection, session_id: int) -> dict | None:
    """세션의 리포트 조회 — 중복 전달의 멱등 판단용. 없으면 None.

    soft-delete(deleted_at) 를 무시하고 읽는다 — 세션 유니크 제약은 삭제 행에도 걸려
    있으므로, 삭제된 리포트가 있는 세션도 "이미 생성됨"(스킵 대상)으로 취급해야 한다.
    """
    cur = await conn.execute(
        """
        SELECT id, user_id, status, text_analyzed_at, audio_analyzed_at
        FROM reports WHERE interview_session_id = %s
        """,
        (session_id,),
    )
    return await cur.fetchone()


async def load_snapshot_source(conn: AsyncConnection, session_id: int) -> dict | None:
    """리포트 생성 재료 — 세션 소유자와 사용 이력서의 id·원본 파일명
    (user_id, resume_id, original_file_name).

    소유자를 여기서 읽는 이유: 생성 요청 메시지는 sessionId 만 담는 포인터 계약이라
    (2026-07-30 면접 도메인 합의) 소유자의 출처는 세션 행 하나다.
    세션이 없거나 이력서를 못 찾으면 None(유령 이벤트 — 스킵 대상). 이력서의
    soft-delete 는 무시하고 읽는다 — 스냅샷은 삭제 여부와 무관하게 파일명만 필요하다.
    """
    cur = await conn.execute(
        """
        SELECT s.user_id, s.resume_id, r.original_file_name
        FROM interview_session s
        JOIN resumes r ON r.id = s.resume_id
        WHERE s.id = %s
        """,
        (session_id,),
    )
    return await cur.fetchone()


async def load_transcript(conn: AsyncConnection, session_id: int) -> list[Utterance] | None:
    """대본(세션당 1행 jsonb) → 발화 목록. 대본이 없으면 None(스킵 대상)."""
    cur = await conn.execute(
        "SELECT utterances FROM interview_transcripts WHERE interview_session_id = %s",
        (session_id,),
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return [Utterance.model_validate(u) for u in row["utterances"]]


# ---------------------------------------------------------------- 로우 생성

async def create_report_with_job(
    conn: AsyncConnection,
    *,
    session_id: int,
    user_id: int,
    resume_id: int,
    resume_file_name: str,
) -> int | None:
    """리포트(PENDING) + Job 을 한 트랜잭션으로 생성하고 리포트 id 를 반환한다.

    실행 순서는 리포트 → Job(생성된 id 의존)이지만 커밋이 하나라 바깥에서는 중간
    상태가 보이지 않는다 — "리포트가 있으면 Job 도 있다" 불변식.
    세션 유니크 충돌 = 다른 처리자가 먼저 생성(중복 전달 경쟁) → None 반환,
    호출자는 get_report_by_session 으로 기존 로우 기준 진행한다.
    """
    now = _utcnow()
    try:
        async with conn.transaction():
            cur = await conn.execute(
                """
                INSERT INTO reports
                    (user_id, interview_session_id, resume_id, status,
                     resume_file_name_snapshot, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (user_id, session_id, resume_id, ReportStatus.PENDING.value,
                 resume_file_name, now, now),
            )
            report_id = (await cur.fetchone())["id"]
            await conn.execute(
                "INSERT INTO report_generation_jobs (report_id, requested_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s)",
                (report_id, now, now, now),
            )
            return report_id
    except errors.UniqueViolation:
        return None


# ---------------------------------------------------------------- 상태 (원자적 CAS)

async def try_transition(
    conn: AsyncConnection,
    report_id: int,
    from_status: ReportStatus,
    to_status: ReportStatus,
) -> bool:
    """원자적 상태 전이(CAS). True = 내가 전이시킴, False = 다른 처리자가 앞서감(양보)."""
    cur = await conn.execute(
        "UPDATE reports SET status = %(to)s, updated_at = %(now)s "
        "WHERE id = %(rid)s AND status = %(from)s",
        {
            "to": to_status.value,
            "from": from_status.value,
            "rid": report_id,
            "now": _utcnow(),
        },
    )
    return cur.rowcount == 1


async def mark_failed(conn: AsyncConnection, report_id: int, reason: str) -> bool:
    """FAILED 종결 — 텍스트 경로 실패에 한정한다(음성 실패는 포기 ACK 후 유예 완성 경로).

    종결 상태(COMPLETED/FAILED)는 덮어쓰지 않는다 — 멱등.
    """
    cur = await conn.execute(
        """
        UPDATE reports SET status = %(failed)s, failed_reason = %(msg)s, updated_at = %(now)s
        WHERE id = %(rid)s AND status NOT IN (%(failed)s, %(completed)s)
        """,
        {
            "failed": ReportStatus.FAILED.value,
            "completed": ReportStatus.COMPLETED.value,
            "msg": reason,
            "rid": report_id,
            "now": _utcnow(),
        },
    )
    return cur.rowcount == 1


async def try_complete(
    conn: AsyncConnection, report_id: int, *, require_audio: bool = True
) -> bool:
    """완성 판정 — 조건부 UPDATE 한 문장으로 COMPLETED 를 확정한다.

    require_audio=True(음성 분석 경로 운영 시): 텍스트·음성 두 단계가 모두 끝난
    리포트만 완성한다 — 어느 쪽이 나중에 끝나든 "나중에 끝난 쪽"의 호출만 성공.
    require_audio=False(음성 경로 도입 전): 텍스트만 끝나면 완성한다 — 전달력은
    빈 값으로 남고 overall 은 텍스트 3축 평균이 된다.
    이미 완결됐거나 필요한 단계가 미완이면 0행(False — 그대로 둔다).
    overall_score = 평가된 축의 평균 반올림: 텍스트 3축 + (delivery 가 있으면) 전달력.
    """
    audio_condition = "AND r.audio_analyzed_at IS NOT NULL" if require_audio else ""
    cur = await conn.execute(
        """
        UPDATE reports r
        SET status = %(completed)s, overall_score = sub.overall,
            completed_at = %(now)s, updated_at = %(now)s
        FROM (
            SELECT s.report_id,
                   round((s.logic_score + s.specificity_score + s.technical_accuracy_score
                          + coalesce(r2.delivery_score, 0))::numeric
                         / (3 + (r2.delivery_score IS NOT NULL)::int)) AS overall
            FROM report_scores s
            JOIN reports r2 ON r2.id = s.report_id
            WHERE s.report_id = %(rid)s
        ) sub
        WHERE r.id = %(rid)s AND r.status = %(processing)s
          AND r.text_analyzed_at IS NOT NULL {audio_condition}
        """.format(audio_condition=audio_condition),
        {
            "completed": ReportStatus.COMPLETED.value,
            "processing": ReportStatus.PROCESSING.value,
            "rid": report_id,
            "now": _utcnow(),
        },
    )
    return cur.rowcount == 1


# ---------------------------------------------------------------- 산출물 (텍스트 1단계)

class SessionScores(BaseModel):
    """report_scores 1행 — 세션 3축 점수(답변별 점수의 평균 반올림, 집계는 파이프라인 소관)."""

    logic_score: int
    specificity_score: int
    technical_accuracy_score: int


class FeedbackRecord(BaseModel):
    """report_feedbacks 1행 — 답변 하나의 평가 결과 저장 형태."""

    question_number: int
    logic_score: int
    specificity_score: int
    technical_accuracy_score: int
    feedback: str
    weakness_tags: list[str]
    improvement_tasks: list[ImprovementTask]
    resume_context: dict | None = None


async def save_text_results(
    conn: AsyncConnection,
    report_id: int,
    *,
    scores: SessionScores,
    feedbacks: list[FeedbackRecord],
    summary: str,
    tag_summary: list[WeaknessTagCount],
) -> None:
    """텍스트 분석(1단계) 산출물 일괄 저장 + text_analyzed_at — 한 트랜잭션(전부 or 전무).

    선삭제 후 삽입이라 재수행·재생성에서 멱등하다 (replace_chunks 선례).
    상태는 바꾸지 않는다 — 완성 판정은 try_complete 가 담당한다(음성이 미완일 수 있음).
    """
    if not feedbacks:
        raise ValueError("피드백 0건 — 평가 없이 산출물을 저장할 수 없다")
    now = _utcnow()
    async with conn.transaction():
        await conn.execute("DELETE FROM report_scores WHERE report_id = %s", (report_id,))
        await conn.execute(
            "INSERT INTO report_scores (report_id, logic_score, specificity_score, "
            "technical_accuracy_score, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (report_id, scores.logic_score, scores.specificity_score,
             scores.technical_accuracy_score, now, now),
        )
        await conn.execute("DELETE FROM report_feedbacks WHERE report_id = %s", (report_id,))
        async with conn.cursor() as cur:
            await cur.executemany(
                "INSERT INTO report_feedbacks (report_id, question_number, logic_score, "
                "specificity_score, technical_accuracy_score, feedback, weakness_tags, "
                "improvement_tasks, resume_context, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    (report_id, f.question_number, f.logic_score, f.specificity_score,
                     f.technical_accuracy_score, f.feedback, Jsonb(f.weakness_tags),
                     Jsonb([t.model_dump() for t in f.improvement_tasks]),
                     Jsonb(f.resume_context) if f.resume_context is not None else None,
                     now, now)
                    for f in feedbacks
                ],
            )
        await conn.execute(
            "UPDATE reports SET summary = %s, weakness_tag_summary = %s, "
            "text_analyzed_at = %s, updated_at = %s WHERE id = %s",
            (summary, Jsonb([t.model_dump() for t in tag_summary]), now, now, report_id),
        )


# ---------------------------------------------------------------- Job 운영 기록

async def increment_job_retry(conn: AsyncConnection, report_id: int) -> None:
    """내부 재시도마다 즉시 반영 (크래시 생존성·관측성 — 이력서 §6 선례)."""
    await conn.execute(
        "UPDATE report_generation_jobs SET retry_count = retry_count + 1, updated_at = %s "
        "WHERE report_id = %s",
        (_utcnow(), report_id),
    )


async def reset_job_retry(conn: AsyncConnection, report_id: int) -> None:
    """새 런(신규 메시지) 시작 시 0 으로 리셋 — 회수 재개에서는 호출하지 않는다."""
    await conn.execute(
        "UPDATE report_generation_jobs SET retry_count = 0, updated_at = %s "
        "WHERE report_id = %s",
        (_utcnow(), report_id),
    )


async def get_job_error(conn: AsyncConnection, report_id: int) -> str | None:
    """기록된 마지막 오류 조회 — 포기 규칙이 실패 사유에 합류시키는 용도."""
    cur = await conn.execute(
        "SELECT error_message FROM report_generation_jobs WHERE report_id = %s",
        (report_id,),
    )
    row = await cur.fetchone()
    return row["error_message"] if row else None


async def record_job_error(conn: AsyncConnection, report_id: int, summary: str) -> None:
    """진행 중 마지막 실패 원인 기록 (상태는 바꾸지 않음) — best-effort.

    기록 실패가 원래 예외 전파(재시도 경로)를 가리면 안 되므로 삼키고 로그만 남긴다.
    """
    try:
        await conn.execute(
            "UPDATE report_generation_jobs SET error_message = %s, updated_at = %s "
            "WHERE report_id = %s",
            (summary[:500], _utcnow(), report_id),
        )
    except Exception:
        logger.warning("Job 오류 기록 실패 (report_id=%s)", report_id, exc_info=True)
