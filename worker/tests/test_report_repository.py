"""리포트 저장 계층 테스트 — 실제 PostgreSQL 대상. 픽스처는 conftest.py 참조.

검증 대상 불변식:
- 리포트가 있으면 Job 도 있다 (한 트랜잭션 생성, 유니크 충돌 시 부분 로우 없음)
- 상태 전이는 CAS — 경쟁 시 정확히 한쪽만 성공
- 텍스트 산출물은 전부 있거나 전무, 재수행 시 갈아끼움(멱등)
- COMPLETED 는 텍스트·음성 둘 다 끝났을 때만, overall 은 평가된 축의 평균
"""

import pytest

from src.contract import ImprovementTask, ReportStatus, WeaknessTagCount
from src.report import repository as repo
from tests.conftest import requires_postgres, seed_session, seed_transcript

pytestmark = requires_postgres

UTTERANCES = [
    {"questionNumber": 1, "parentQuestionNumber": 1, "speaker": "INTERVIEWER",
     "questionType": "MAIN", "content": "JPA 영속성 컨텍스트를 설명해주세요.",
     "spokenAt": "2026-07-28T10:00:00Z"},
    {"questionNumber": 1, "parentQuestionNumber": 1, "speaker": "USER",
     "questionType": "MAIN", "content": "엔티티를 관리하는 1차 캐시입니다.",
     "spokenAt": "2026-07-28T10:00:10Z"},
]


def feedback(n: int, score: int = 80) -> repo.FeedbackRecord:
    return repo.FeedbackRecord(
        question_number=n,
        logic_score=score,
        specificity_score=score,
        technical_accuracy_score=score,
        feedback="두괄식으로 정리해 전달하면 더 좋겠습니다.",
        weakness_tags=["두괄식 부족"],
        improvement_tasks=[ImprovementTask(title="두괄식 연습", description="결론부터 말하는 연습을 해보세요.")],
        resume_context={"chunk": "Spring 프로젝트 경험"},
    )


SCORES = repo.SessionScores(logic_score=80, specificity_score=75, technical_accuracy_score=85)
TAGS = [WeaknessTagCount(tag="두괄식 부족", count=3)]


async def create(conn, session_id: int, resume_id: int, user_id: int = 1) -> int:
    rid = await repo.create_report_with_job(
        conn, session_id=session_id, user_id=user_id,
        resume_id=resume_id, resume_file_name="이력서.pdf",
    )
    assert rid is not None
    return rid


@pytest.mark.asyncio
async def test_스키마_멱등_생성(conn):
    await repo.ensure_schema(conn)  # 두 번 불러도 에러 없음
    cur = await conn.execute("SELECT count(*) AS n FROM report_generation_jobs")
    assert (await cur.fetchone())["n"] == 0


@pytest.mark.asyncio
async def test_생성시_리포트와_Job이_함께_생긴다(conn):
    session_id, resume_id = await seed_session(conn)
    report_id = await create(conn, session_id, resume_id)

    report = await repo.get_report_by_session(conn, session_id)
    assert report["id"] == report_id
    assert report["status"] == "PENDING"
    cur = await conn.execute(
        "SELECT retry_count, requested_at FROM report_generation_jobs WHERE report_id = %s",
        (report_id,),
    )
    job = await cur.fetchone()
    assert job["retry_count"] == 0
    assert job["requested_at"] is not None


@pytest.mark.asyncio
async def test_같은_세션_중복_생성은_None_부분로우_없음(conn):
    session_id, resume_id = await seed_session(conn)
    await create(conn, session_id, resume_id)

    dup = await repo.create_report_with_job(
        conn, session_id=session_id, user_id=1, resume_id=resume_id, resume_file_name="이력서.pdf"
    )
    assert dup is None
    for table in ("reports", "report_generation_jobs"):
        cur = await conn.execute(f"SELECT count(*) AS n FROM {table}")
        assert (await cur.fetchone())["n"] == 1


@pytest.mark.asyncio
async def test_스냅샷_재료_조회(conn):
    session_id, resume_id = await seed_session(conn, file_name="포트폴리오.pdf")
    source = await repo.load_snapshot_source(conn, session_id)
    assert source == {"resume_id": resume_id, "original_file_name": "포트폴리오.pdf"}
    assert await repo.load_snapshot_source(conn, session_id + 999) is None  # 유령 세션


@pytest.mark.asyncio
async def test_대본_조회와_파싱(conn):
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)

    utterances = await repo.load_transcript(conn, session_id)
    assert len(utterances) == 2
    assert utterances[0].questionNumber == 1
    assert await repo.load_transcript(conn, session_id + 999) is None  # 대본 없음


@pytest.mark.asyncio
async def test_상태_CAS_성공과_양보(conn):
    session_id, resume_id = await seed_session(conn)
    report_id = await create(conn, session_id, resume_id)

    assert await repo.try_transition(conn, report_id, ReportStatus.PENDING, ReportStatus.PROCESSING)
    # 같은 전이를 다시 시도(중복 처리자) → 이전 상태가 아니므로 양보
    assert not await repo.try_transition(conn, report_id, ReportStatus.PENDING, ReportStatus.PROCESSING)


@pytest.mark.asyncio
async def test_텍스트_산출물_일괄_저장(conn):
    session_id, resume_id = await seed_session(conn)
    report_id = await create(conn, session_id, resume_id)

    await repo.save_text_results(
        conn, report_id, scores=SCORES, feedbacks=[feedback(1), feedback(2)],
        summary="전반적으로 논리 전개가 좋습니다.", tag_summary=TAGS,
    )

    report = await repo.get_report_by_session(conn, session_id)
    assert report["text_analyzed_at"] is not None
    cur = await conn.execute(
        "SELECT question_number, weakness_tags, improvement_tasks FROM report_feedbacks "
        "WHERE report_id = %s ORDER BY question_number", (report_id,),
    )
    rows = await cur.fetchall()
    assert [r["question_number"] for r in rows] == [1, 2]
    assert rows[0]["weakness_tags"] == ["두괄식 부족"]
    assert rows[0]["improvement_tasks"][0]["title"] == "두괄식 연습"


@pytest.mark.asyncio
async def test_산출물_재저장은_갈아끼운다(conn):
    session_id, resume_id = await seed_session(conn)
    report_id = await create(conn, session_id, resume_id)

    await repo.save_text_results(
        conn, report_id, scores=SCORES, feedbacks=[feedback(1), feedback(2), feedback(3)],
        summary="첫 평가", tag_summary=TAGS,
    )
    await repo.save_text_results(
        conn, report_id, scores=SCORES, feedbacks=[feedback(1, score=90)],
        summary="재평가", tag_summary=TAGS,
    )

    cur = await conn.execute(
        "SELECT count(*) AS n FROM report_feedbacks WHERE report_id = %s", (report_id,)
    )
    assert (await cur.fetchone())["n"] == 1  # 잔여 피드백 없음
    cur = await conn.execute("SELECT summary FROM reports WHERE id = %s", (report_id,))
    assert (await cur.fetchone())["summary"] == "재평가"


@pytest.mark.asyncio
async def test_피드백_0건_저장은_거부(conn):
    with pytest.raises(ValueError):
        await repo.save_text_results(
            conn, 1, scores=SCORES, feedbacks=[], summary="", tag_summary=[]
        )


@pytest.mark.asyncio
async def test_완성판정_한쪽만_끝나면_거절(conn):
    session_id, resume_id = await seed_session(conn)
    report_id = await create(conn, session_id, resume_id)
    await repo.try_transition(conn, report_id, ReportStatus.PENDING, ReportStatus.PROCESSING)
    await repo.save_text_results(  # 텍스트만 완료, 음성 미완
        conn, report_id, scores=SCORES, feedbacks=[feedback(1)], summary="총평", tag_summary=TAGS,
    )

    assert not await repo.try_complete(conn, report_id)
    assert (await repo.get_report_by_session(conn, session_id))["status"] == "PROCESSING"


@pytest.mark.parametrize(
    ("delivery_score", "expected_overall"),
    [
        (None, 80),  # 텍스트 3축만: (80+75+85)/3 = 80
        (60, 75),    # 전달력 포함 4축: (80+75+85+60)/4 = 75
    ],
)
@pytest.mark.asyncio
async def test_완성판정_overall은_평가된_축의_평균(conn, delivery_score, expected_overall):
    session_id, resume_id = await seed_session(conn)
    report_id = await create(conn, session_id, resume_id)
    await repo.try_transition(conn, report_id, ReportStatus.PENDING, ReportStatus.PROCESSING)
    await repo.save_text_results(
        conn, report_id, scores=SCORES, feedbacks=[feedback(1)], summary="총평", tag_summary=TAGS,
    )
    # 음성 분석 완료를 흉내낸다 (음성 파이프라인은 이 PR 범위 밖)
    await conn.execute(
        "UPDATE reports SET audio_analyzed_at = now(), delivery_score = %s WHERE id = %s",
        (delivery_score, report_id),
    )

    assert await repo.try_complete(conn, report_id)
    cur = await conn.execute(
        "SELECT status, overall_score, completed_at FROM reports WHERE id = %s", (report_id,)
    )
    row = await cur.fetchone()
    assert row["status"] == "COMPLETED"
    assert row["overall_score"] == expected_overall
    assert row["completed_at"] is not None
    # 이미 완결된 리포트에 다시 판정 → 양보
    assert not await repo.try_complete(conn, report_id)


@pytest.mark.asyncio
async def test_실패기록은_종결상태를_덮어쓰지_않는다(conn):
    session_id, resume_id = await seed_session(conn)
    report_id = await create(conn, session_id, resume_id)

    assert await repo.mark_failed(conn, report_id, "LLM 응답 형식 오류")
    cur = await conn.execute("SELECT status, failed_reason FROM reports WHERE id = %s", (report_id,))
    row = await cur.fetchone()
    assert row["status"] == "FAILED"
    assert row["failed_reason"] == "LLM 응답 형식 오류"
    # 이미 FAILED(종결) → 멱등 거절
    assert not await repo.mark_failed(conn, report_id, "다른 사유")


@pytest.mark.asyncio
async def test_Job_retry_증가와_리셋과_오류기록(conn):
    session_id, resume_id = await seed_session(conn)
    report_id = await create(conn, session_id, resume_id)

    await repo.increment_job_retry(conn, report_id)
    await repo.increment_job_retry(conn, report_id)
    await repo.record_job_error(conn, report_id, "Bedrock 타임아웃")
    cur = await conn.execute(
        "SELECT retry_count, error_message FROM report_generation_jobs WHERE report_id = %s",
        (report_id,),
    )
    job = await cur.fetchone()
    assert job["retry_count"] == 2
    assert job["error_message"] == "Bedrock 타임아웃"

    await repo.reset_job_retry(conn, report_id)
    cur = await conn.execute(
        "SELECT retry_count FROM report_generation_jobs WHERE report_id = %s", (report_id,)
    )
    assert (await cur.fetchone())["retry_count"] == 0
