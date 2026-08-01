"""리포트 파이프라인 테스트 — 실제 PostgreSQL + FakeEvaluator (LLM 없이 흐름 검증).

검증 대상: 정상 완주, ACK/재전달 규칙(스킵·양보·재개·포기), 재전달 멱등,
평가 실패의 예외 전파와 재시도 흔적, 완성 판정 경로.
"""

import pytest

from src.config import Settings
from src.contract import ReportGenerationRequested, ReportStatus
from src.report.evaluator import FakeEvaluator
from src.report.pipeline import process_generation_request
from tests.conftest import requires_postgres, seed_session, seed_transcript

pytestmark = requires_postgres

UTTERANCES = [
    {"questionNumber": 1, "parentQuestionNumber": 1, "speaker": "INTERVIEWER",
     "questionType": "MAIN", "content": "JPA 영속성 컨텍스트를 설명해주세요.",
     "spokenAt": "2026-07-29T10:00:00Z"},
    {"questionNumber": 1, "parentQuestionNumber": 1, "speaker": "USER",
     "questionType": "MAIN", "content": "엔티티를 관리하는 1차 캐시입니다.",
     "spokenAt": "2026-07-29T10:00:10Z"},
    {"questionNumber": 2, "parentQuestionNumber": 1, "speaker": "INTERVIEWER",
     "questionType": "TAIL", "content": "그럼 flush와 commit의 차이는요?",
     "spokenAt": "2026-07-29T10:00:30Z"},
    {"questionNumber": 2, "parentQuestionNumber": 1, "speaker": "USER",
     "questionType": "TAIL", "content": "flush는 SQL 전송, commit은 확정입니다.",
     "spokenAt": "2026-07-29T10:00:40Z"},
]


class PublishSpy:
    """발행된 상태 이벤트를 수집하는 가짜 발행자."""

    def __init__(self):
        self.events = []

    async def __call__(self, report_id, user_id, status, message):
        self.events.append((report_id, user_id, status, message))

    def statuses(self):
        return [e[2] for e in self.events]


def fast_settings(dsn: str, audio_enabled: bool = False) -> Settings:
    """재시도 대기 없는 테스트용 설정. audio_enabled 기본값은 운영 기본(음성 경로 도입 전)과 동일."""
    return Settings(postgres_dsn=dsn, retry_base_delay_s=0.0,
                    audio_analysis_enabled=audio_enabled)


async def seed_ready_session(conn) -> int:
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, UTTERANCES)
    return session_id


async def run(conn, session_id, *, evaluator=None, publish=None, delivery_count=1,
              is_reclaimed=False, audio_enabled=False):
    await process_generation_request(
        ReportGenerationRequested(sessionId=session_id),
        conn=conn,
        evaluator=evaluator if evaluator is not None else FakeEvaluator(),
        publish=publish if publish is not None else PublishSpy(),
        settings=fast_settings(str(conn.info.dsn), audio_enabled),
        delivery_count=delivery_count,
        is_reclaimed=is_reclaimed,
    )


async def report_row(conn, session_id):
    cur = await conn.execute(
        "SELECT * FROM reports WHERE interview_session_id = %s", (session_id,)
    )
    return await cur.fetchone()


@pytest.mark.asyncio
async def test_정상_완주_음성_경로_도입_전에는_즉시_완성(conn):
    """기본 설정(audio_analysis_enabled=False) — 텍스트 평가만으로 COMPLETED 확정."""
    session_id = await seed_ready_session(conn)
    spy = PublishSpy()

    await run(conn, session_id, publish=spy)

    row = await report_row(conn, session_id)
    assert row["status"] == "COMPLETED"
    assert row["overall_score"] == 80  # 텍스트 3축 평균 (Fake 기본값)
    assert row["delivery_score"] is None  # 전달력은 빈 값 유지
    assert row["text_analyzed_at"] is not None
    assert row["summary"] is not None
    assert spy.statuses() == [ReportStatus.PROCESSING, ReportStatus.COMPLETED]
    cur = await conn.execute(
        "SELECT question_number FROM report_feedbacks WHERE report_id = %s "
        "ORDER BY question_number", (row["id"],)
    )
    assert [r["question_number"] for r in await cur.fetchall()] == [1, 2]
    cur = await conn.execute(
        "SELECT logic_score FROM report_scores WHERE report_id = %s", (row["id"],)
    )
    assert (await cur.fetchone())["logic_score"] == 80  # Fake 기본값의 평균


@pytest.mark.asyncio
async def test_음성_경로가_켜지면_완주_후_음성_대기(conn):
    """audio_analysis_enabled=True — 텍스트만 끝난 리포트는 PROCESSING 에 머문다."""
    session_id = await seed_ready_session(conn)
    spy = PublishSpy()

    await run(conn, session_id, publish=spy, audio_enabled=True)

    row = await report_row(conn, session_id)
    assert row["status"] == "PROCESSING"  # 음성 미완 — 완성은 음성 쪽 몫
    assert row["text_analyzed_at"] is not None
    assert spy.statuses() == [ReportStatus.PROCESSING]  # COMPLETED 는 아직


@pytest.mark.asyncio
async def test_음성이_이미_끝났으면_완주_시_COMPLETED(conn):
    session_id = await seed_ready_session(conn)
    spy = PublishSpy()
    await run(conn, session_id, publish=spy, audio_enabled=True)  # 텍스트 완료 (PROCESSING 잔류)

    row = await report_row(conn, session_id)
    await conn.execute(  # 음성 분석 완료를 흉내
        "UPDATE reports SET audio_analyzed_at = now(), delivery_score = 60 WHERE id = %s",
        (row["id"],),
    )
    await run(conn, session_id, publish=spy, audio_enabled=True)  # 재전달 → 판정만 재시도

    row = await report_row(conn, session_id)
    assert row["status"] == "COMPLETED"
    assert row["overall_score"] == 75  # (80+80+80+60)/4
    assert spy.statuses() == [ReportStatus.PROCESSING, ReportStatus.COMPLETED]


@pytest.mark.asyncio
async def test_종결_상태_재전달은_아무것도_안_한다(conn):
    session_id = await seed_ready_session(conn)
    await run(conn, session_id)
    row = await report_row(conn, session_id)
    await conn.execute("UPDATE reports SET status = 'COMPLETED' WHERE id = %s", (row["id"],))

    spy = PublishSpy()
    await run(conn, session_id, publish=spy)

    assert spy.events == []
    cur = await conn.execute(
        "SELECT count(*) AS n FROM report_feedbacks WHERE report_id = %s", (row["id"],)
    )
    assert (await cur.fetchone())["n"] == 2  # 갈아끼움도 없음


@pytest.mark.asyncio
async def test_재전달_멱등_판정_불가면_발행_없음(conn):
    session_id = await seed_ready_session(conn)
    spy = PublishSpy()
    await run(conn, session_id, publish=spy, audio_enabled=True)  # 완주 (음성 대기)

    await run(conn, session_id, publish=spy, audio_enabled=True)  # 같은 메시지 재전달

    assert spy.statuses() == [ReportStatus.PROCESSING]  # 재평가·재발행 없음
    row = await report_row(conn, session_id)
    assert row["status"] == "PROCESSING"


@pytest.mark.asyncio
async def test_대본_없으면_로우_없이_스킵(conn):
    session_id, _ = await seed_session(conn)  # 대본 미저장
    spy = PublishSpy()

    await run(conn, session_id, publish=spy)

    assert await report_row(conn, session_id) is None
    assert spy.events == []


@pytest.mark.asyncio
async def test_세션_없으면_로우_없이_스킵(conn):
    ghost_session = 99_999
    await seed_transcript(conn, ghost_session, UTTERANCES)  # 대본만 있고 세션 없음

    await run(conn, ghost_session)

    assert await report_row(conn, ghost_session) is None


@pytest.mark.asyncio
async def test_평가_실패는_예외_전파와_재시도_흔적을_남긴다(conn):
    session_id = await seed_ready_session(conn)
    spy = PublishSpy()
    failing = FakeEvaluator(fail_on=frozenset({1}))

    with pytest.raises(RuntimeError):  # ACK 없음(재전달 대상)의 시뮬레이션
        await run(conn, session_id, evaluator=failing, publish=spy)

    row = await report_row(conn, session_id)
    assert row["status"] == "PROCESSING"  # 로우는 남고 미완 — 재전달이 재개
    assert row["text_analyzed_at"] is None
    cur = await conn.execute(
        "SELECT retry_count, error_message FROM report_generation_jobs WHERE report_id = %s",
        (row["id"],),
    )
    job = await cur.fetchone()
    assert job["retry_count"] == 3  # 내부 재시도 3회 소진
    assert job["error_message"] == "RuntimeError"


@pytest.mark.asyncio
async def test_전달_임계_도달이면_FAILED_확정(conn):
    session_id = await seed_ready_session(conn)
    failing = FakeEvaluator(fail_on=frozenset({1}))
    with pytest.raises(RuntimeError):
        await run(conn, session_id, evaluator=failing)  # 1차: 실패 런 (오류 기록됨)

    spy = PublishSpy()
    await run(conn, session_id, publish=spy, delivery_count=3)  # 임계 도달

    row = await report_row(conn, session_id)
    assert row["status"] == "FAILED"
    assert "RuntimeError" in row["failed_reason"]  # 마지막 오류 합류
    assert spy.statuses() == [ReportStatus.FAILED]


@pytest.mark.asyncio
async def test_확정만_남은_리포트는_임계_도달_배달에서도_완성된다(conn):
    """텍스트 저장 후 완성 확정 전에 죽었던 리포트 — 포기(FAILED)가 아니라 완성으로 종결 (리뷰 반영)."""
    session_id = await seed_ready_session(conn)
    await run(conn, session_id, audio_enabled=True)  # 텍스트 저장됨 + PROCESSING (확정 전 사망 상태 재현)

    spy = PublishSpy()
    await run(conn, session_id, publish=spy, delivery_count=3)  # 임계 도달 배달, 텍스트 전용 모드

    row = await report_row(conn, session_id)
    assert row["status"] == "COMPLETED"  # FAILED 가 아니다
    assert spy.statuses() == [ReportStatus.COMPLETED]


@pytest.mark.asyncio
async def test_음성_대기중_리포트는_임계_도달_배달에서_FAILED가_되지_않는다(conn):
    """음성 경로가 켜진 모드 — 텍스트가 끝난 리포트는 임계 도달에도 음성을 계속 기다린다."""
    session_id = await seed_ready_session(conn)
    await run(conn, session_id, audio_enabled=True)  # 텍스트 완료, 음성 대기

    spy = PublishSpy()
    await run(conn, session_id, publish=spy, delivery_count=3, audio_enabled=True)

    row = await report_row(conn, session_id)
    assert row["status"] == "PROCESSING"  # 포기 대상이 아니다 — 완성은 음성 쪽 몫
    assert spy.events == []


@pytest.mark.asyncio
async def test_신규_경로는_진행중_로우에_양보하고_회수_경로는_재개한다(conn):
    session_id = await seed_ready_session(conn)
    failing = FakeEvaluator(fail_on=frozenset({1}))
    with pytest.raises(RuntimeError):
        await run(conn, session_id, evaluator=failing)  # PROCESSING 미완 로우를 만든다

    spy = PublishSpy()
    await run(conn, session_id, publish=spy)  # 신규 경로 → 양보
    assert spy.events == []
    assert (await report_row(conn, session_id))["text_analyzed_at"] is None

    await run(conn, session_id, publish=spy, is_reclaimed=True, audio_enabled=True)  # 회수 경로 → 재개
    row = await report_row(conn, session_id)
    assert row["text_analyzed_at"] is not None  # 완주
    assert spy.statuses() == []  # 재개는 PROCESSING 재발행 없음 (음성 대기라 COMPLETED 도 없음)


@pytest.mark.asyncio
async def test_점수_집계는_평균_반올림(conn):
    from src.report.evaluator import AnswerEvaluation

    session_id = await seed_ready_session(conn)
    evaluator = FakeEvaluator(results={
        1: AnswerEvaluation(questionNumber=1, logicScore=70, specificityScore=75,
                            technicalAccuracyScore=90, feedback="좋아요.",
                            weaknessTags=["두괄식 부족"]),
        2: AnswerEvaluation(questionNumber=2, logicScore=75, specificityScore=75,
                            technicalAccuracyScore=91, feedback="좋아요.",
                            weaknessTags=["두괄식 부족", "근거 부족"]),
    })

    await run(conn, session_id, evaluator=evaluator)

    row = await report_row(conn, session_id)
    cur = await conn.execute(
        "SELECT * FROM report_scores WHERE report_id = %s", (row["id"],)
    )
    scores = await cur.fetchone()
    assert scores["logic_score"] == 73  # (70+75)/2 = 72.5 → 73 (0.5 올림)
    assert scores["specificity_score"] == 75
    assert scores["technical_accuracy_score"] == 91  # 90.5 → 91
    assert row["weakness_tag_summary"] == [
        {"tag": "두괄식 부족", "count": 2},
        {"tag": "근거 부족", "count": 1},
    ]
