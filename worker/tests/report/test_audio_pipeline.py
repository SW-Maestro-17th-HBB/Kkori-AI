"""음성 분석 파이프라인 테스트 — 실제 PostgreSQL + 픽스처 녹음 + 가짜 S3.

검증 대상: 텍스트·음성 완료 순서에 따른 완성 판정, 종결 리포트 무시, 중복 전달,
포기 규칙(FAILED 없음), 로우 없음 규칙, 다운로드 실패 전파, 태그 병합, 유예 완성.
"""

import io

import pytest

from src.contract import AudioAnalysisRequested, ReportStatus
from src.report import repository as repo
from src.report.audio import TAG_FAST, TAG_SLOW
from src.report.audio_pipeline import ReportNotReady, process_audio_request
from tests.conftest import requires_postgres, seed_session, seed_transcript
from tests.report.test_audio import FIXTURE, FIXTURE_TEXT
from tests.report.test_pipeline import (
    PublishSpy,
    fast_settings,
    report_row,
    run as run_generation,
)

pytestmark = requires_postgres

RECORDING = FIXTURE.read_bytes()

# 픽스처 문장 4개를 지원자 답변으로 나눠 실은 대본 — 음절 수는 FIXTURE_TEXT 와 같다
_SENTENCES = [s + "." for s in FIXTURE_TEXT.split(". ") if s]
_SENTENCES[-1] = _SENTENCES[-1].rstrip(".")
UTTERANCES = []
for i, sentence in enumerate(_SENTENCES, start=1):
    UTTERANCES.append({
        "questionNumber": i, "parentQuestionNumber": 1, "speaker": "INTERVIEWER",
        "questionType": "MAIN" if i == 1 else "TAIL", "content": f"질문 {i}",
        "spokenAt": f"2026-07-29T10:0{i}:00Z",
    })
    UTTERANCES.append({
        "questionNumber": i, "parentQuestionNumber": 1, "speaker": "USER",
        "questionType": "MAIN" if i == 1 else "TAIL", "content": sentence,
        "spokenAt": f"2026-07-29T10:0{i}:10Z",
    })


class FakeS3:
    def __init__(self, data: bytes = RECORDING):
        self.data = data
        self.calls: list[tuple[str, str]] = []

    def get_object(self, Bucket: str, Key: str):
        self.calls.append((Bucket, Key))
        return {"Body": io.BytesIO(self.data)}


class BrokenS3:
    def get_object(self, Bucket: str, Key: str):
        raise ConnectionError("S3 불통")


async def seed(conn, utterances=UTTERANCES) -> int:
    session_id, _ = await seed_session(conn)
    await seed_transcript(conn, session_id, utterances)
    return session_id


async def run_audio(conn, session_id, *, s3=None, publish=None, delivery_count=1,
                    audio_enabled=True):
    await process_audio_request(
        AudioAnalysisRequested(sessionId=session_id, bucket="kkori-recordings",
                               objectKey=f"recordings/room-{session_id}.ogg"),
        conn=conn,
        s3=s3 if s3 is not None else FakeS3(),
        publish=publish if publish is not None else PublishSpy(),
        settings=fast_settings(str(conn.info.dsn), audio_enabled),
        delivery_count=delivery_count,
    )


async def text_done_awaiting_audio(conn) -> int:
    """텍스트 분석은 끝나고 음성을 기다리는 PROCESSING 리포트."""
    session_id = await seed(conn)
    await run_generation(conn, session_id, audio_enabled=True)
    row = await report_row(conn, session_id)
    assert row["status"] == ReportStatus.PROCESSING.value and row["text_analyzed_at"] is not None
    return session_id


# --- 정상 경로 ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_텍스트_완료_후_음성이_오면_4축으로_완성한다(conn):
    session_id = await text_done_awaiting_audio(conn)
    spy = PublishSpy()
    s3 = FakeS3()

    await run_audio(conn, session_id, s3=s3, publish=spy)

    row = await report_row(conn, session_id)
    assert s3.calls == [("kkori-recordings", f"recordings/room-{session_id}.ogg")]
    assert row["status"] == ReportStatus.COMPLETED.value
    assert row["audio_analyzed_at"] is not None
    assert 0 <= row["delivery_score"] <= 100
    scores = await conn.execute(
        "SELECT logic_score, specificity_score, technical_accuracy_score "
        "FROM report_scores WHERE report_id = %s", (row["id"],)
    )
    s = await scores.fetchone()
    four_axis = round((s["logic_score"] + s["specificity_score"]
                       + s["technical_accuracy_score"] + row["delivery_score"]) / 4)
    assert row["overall_score"] == four_axis
    assert spy.statuses() == [ReportStatus.COMPLETED]


@pytest.mark.asyncio
async def test_음성이_먼저_오면_저장만_하고_텍스트가_나중에_완성한다(conn):
    session_id = await seed(conn)
    source = await repo.load_snapshot_source(conn, session_id)
    report_id = await repo.create_report_with_job(
        conn, session_id=session_id, user_id=source["user_id"],
        resume_id=source["resume_id"], resume_file_name=source["original_file_name"],
    )
    spy = PublishSpy()

    await run_audio(conn, session_id, publish=spy)  # PENDING 상태에 도착

    row = await report_row(conn, session_id)
    assert row["id"] == report_id
    assert row["status"] == ReportStatus.PENDING.value  # 상태는 건드리지 않는다
    assert row["audio_analyzed_at"] is not None and row["delivery_score"] is not None
    assert spy.statuses() == []

    await run_generation(conn, session_id, publish=spy, audio_enabled=True)

    row = await report_row(conn, session_id)
    assert row["status"] == ReportStatus.COMPLETED.value
    assert spy.statuses() == [ReportStatus.PROCESSING, ReportStatus.COMPLETED]


@pytest.mark.asyncio
async def test_측정_불가_녹음은_전달력_없이_완성한다(conn):
    session_id = await text_done_awaiting_audio(conn)
    # 픽스처 전체 대신 침묵만 8초 — 발성 부족
    import numpy as np
    import soundfile as sf
    buf = io.BytesIO()
    sf.write(buf, np.zeros(48_000 * 8, dtype=np.float32), 48_000, format="OGG", subtype="OPUS")
    spy = PublishSpy()

    await run_audio(conn, session_id, s3=FakeS3(buf.getvalue()), publish=spy)

    row = await report_row(conn, session_id)
    assert row["status"] == ReportStatus.COMPLETED.value
    assert row["delivery_score"] is None
    assert row["audio_analyzed_at"] is not None
    cur = await conn.execute(
        "SELECT round((logic_score + specificity_score + technical_accuracy_score)::numeric / 3) AS o "
        "FROM report_scores WHERE report_id = %s", (row["id"],)
    )
    assert row["overall_score"] == (await cur.fetchone())["o"]  # 3축 평균
    assert spy.statuses() == [ReportStatus.COMPLETED]


# --- 태그 병합 ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_음성_태그는_텍스트_태그_뒤에_붙고_서로_지우지_않는다(conn):
    session_id = await text_done_awaiting_audio(conn)
    row = await report_row(conn, session_id)
    text_tags = [{"tag": "두괄식 부족", "count": 3}, {"tag": "근거 부족", "count": 1}]
    await conn.execute(  # FakeEvaluator 는 태그를 만들지 않으므로 텍스트 단계 결과를 직접 심는다
        "UPDATE reports SET weakness_tag_summary = %s WHERE id = %s",
        (__import__("psycopg").types.json.Jsonb(text_tags), row["id"]),
    )

    # 대본을 3배로 부풀려 "말 속도 빠름" 이 나오게 한다 (음절 수만 늘어남)
    await conn.execute(
        "UPDATE interview_transcript SET content = %s WHERE session_id = %s",
        (__import__("psycopg").types.json.Jsonb(UTTERANCES * 3), session_id),
    )
    await run_audio(conn, session_id)

    row = await report_row(conn, session_id)
    assert row["weakness_tag_summary"][: len(text_tags)] == text_tags
    assert row["weakness_tag_summary"][len(text_tags):] == [{"tag": TAG_FAST, "count": 1}]


@pytest.mark.asyncio
async def test_텍스트_재저장은_기존_음성_태그를_보존한다(conn):
    session_id = await seed(conn)
    source = await repo.load_snapshot_source(conn, session_id)
    report_id = await repo.create_report_with_job(
        conn, session_id=session_id, user_id=source["user_id"],
        resume_id=source["resume_id"], resume_file_name=source["original_file_name"],
    )
    voice = [repo.WeaknessTagCount(tag=TAG_SLOW, count=1)]
    assert await repo.save_audio_result(conn, report_id, delivery_score=70, voice_tags=voice)

    await run_generation(conn, session_id, audio_enabled=True)

    row = await report_row(conn, session_id)
    tags = row["weakness_tag_summary"]
    assert tags[-1] == {"tag": TAG_SLOW, "count": 1}
    assert all(t["tag"] != TAG_SLOW for t in tags[:-1])
    assert row["delivery_score"] == 70


# --- 무시·중복·포기 -----------------------------------------------------------

@pytest.mark.asyncio
async def test_COMPLETED_리포트에_늦게_온_음성은_무시한다(conn):
    session_id = await seed(conn)
    await run_generation(conn, session_id, audio_enabled=False)  # 텍스트만으로 완성
    before = await report_row(conn, session_id)
    assert before["status"] == ReportStatus.COMPLETED.value
    s3, spy = FakeS3(), PublishSpy()

    await run_audio(conn, session_id, s3=s3, publish=spy)

    after = await report_row(conn, session_id)
    assert s3.calls == []
    assert after["delivery_score"] is None and after["audio_analyzed_at"] is None
    assert after["overall_score"] == before["overall_score"]
    assert spy.statuses() == []


@pytest.mark.asyncio
async def test_FAILED_리포트에는_음성_분석을_하지_않는다(conn):
    session_id = await text_done_awaiting_audio(conn)
    row = await report_row(conn, session_id)
    assert await repo.mark_failed(conn, row["id"], "테스트 실패")
    s3 = FakeS3()

    await run_audio(conn, session_id, s3=s3)

    after = await report_row(conn, session_id)
    assert s3.calls == []
    assert after["status"] == ReportStatus.FAILED.value
    assert after["audio_analyzed_at"] is None


@pytest.mark.asyncio
async def test_중복_전달은_분석_없이_판정만_한다(conn):
    session_id = await text_done_awaiting_audio(conn)
    row = await report_row(conn, session_id)
    await conn.execute(
        "UPDATE reports SET audio_analyzed_at = now(), delivery_score = 55 WHERE id = %s",
        (row["id"],),
    )
    s3, spy = FakeS3(), PublishSpy()

    await run_audio(conn, session_id, s3=s3, publish=spy)

    after = await report_row(conn, session_id)
    assert s3.calls == []
    assert after["status"] == ReportStatus.COMPLETED.value
    assert after["delivery_score"] == 55  # 다시 계산하지 않는다
    assert spy.statuses() == [ReportStatus.COMPLETED]


@pytest.mark.asyncio
async def test_전달_임계_도달이면_FAILED_없이_포기한다(conn):
    session_id = await text_done_awaiting_audio(conn)
    s3, spy = FakeS3(), PublishSpy()

    await run_audio(conn, session_id, s3=s3, publish=spy, delivery_count=3)

    row = await report_row(conn, session_id)
    assert s3.calls == []
    assert row["status"] == ReportStatus.PROCESSING.value  # 유예 완성이 마무리한다
    assert row["failed_reason"] is None
    assert spy.statuses() == []


# --- 로우 없음 ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_로우_없고_지원자_발화도_없으면_스킵(conn):
    only_interviewer = [u for u in UTTERANCES if u["speaker"] == "INTERVIEWER"]
    session_id = await seed(conn, only_interviewer)
    s3 = FakeS3()

    await run_audio(conn, session_id, s3=s3)  # 정상 반환 = ACK

    assert s3.calls == []
    assert await report_row(conn, session_id) is None


@pytest.mark.asyncio
async def test_대본_자체가_없으면_스킵(conn):
    session_id, _ = await seed_session(conn)
    await run_audio(conn, session_id)
    assert await report_row(conn, session_id) is None


@pytest.mark.asyncio
async def test_로우_없는데_발화가_있으면_재전달을_기다린다(conn):
    session_id = await seed(conn)
    with pytest.raises(ReportNotReady):
        await run_audio(conn, session_id)
    assert await report_row(conn, session_id) is None  # 로우를 만들지 않는다


# --- 실패 전파 ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_다운로드_실패는_예외_전파와_재시도_흔적을_남긴다(conn):
    session_id = await text_done_awaiting_audio(conn)
    row = await report_row(conn, session_id)

    with pytest.raises(ConnectionError):
        await run_audio(conn, session_id, s3=BrokenS3())

    after = await report_row(conn, session_id)
    assert after["status"] == ReportStatus.PROCESSING.value
    assert after["audio_analyzed_at"] is None
    cur = await conn.execute(
        "SELECT retry_count, error_message FROM report_generation_jobs WHERE report_id = %s",
        (row["id"],),
    )
    job = await cur.fetchone()
    assert job["retry_count"] == 3 and job["error_message"] == "ConnectionError"


# --- 유예 완성 ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_유예_초과_리포트는_전달력_없이_완성된다(conn):
    overdue = await text_done_awaiting_audio(conn)
    fresh = await text_done_awaiting_audio(conn)
    await conn.execute(
        "UPDATE reports SET text_analyzed_at = now() - interval '11 minutes' "
        "WHERE interview_session_id = %s", (overdue,),
    )

    rows = await repo.complete_overdue_audio(conn, grace_seconds=600)

    overdue_row = await report_row(conn, overdue)
    assert [r["id"] for r in rows] == [overdue_row["id"]]
    assert rows[0]["user_id"] == overdue_row["user_id"]
    assert overdue_row["status"] == ReportStatus.COMPLETED.value
    assert overdue_row["delivery_score"] is None and overdue_row["completed_at"] is not None
    cur = await conn.execute(
        "SELECT round((logic_score + specificity_score + technical_accuracy_score)::numeric / 3) AS o "
        "FROM report_scores WHERE report_id = %s", (overdue_row["id"],)
    )
    assert overdue_row["overall_score"] == (await cur.fetchone())["o"]
    assert (await report_row(conn, fresh))["status"] == ReportStatus.PROCESSING.value


@pytest.mark.asyncio
async def test_유예_완성_후_도착한_음성은_무시된다(conn):
    session_id = await text_done_awaiting_audio(conn)
    await conn.execute(
        "UPDATE reports SET text_analyzed_at = now() - interval '1 hour' "
        "WHERE interview_session_id = %s", (session_id,),
    )
    assert len(await repo.complete_overdue_audio(conn, grace_seconds=600)) == 1

    await run_audio(conn, session_id)

    row = await report_row(conn, session_id)
    assert row["status"] == ReportStatus.COMPLETED.value
    assert row["delivery_score"] is None  # 보여준 점수는 바뀌지 않는다


@pytest.mark.asyncio
async def test_유예_완성은_음성이_이미_끝난_리포트를_건드리지_않는다(conn):
    session_id = await text_done_awaiting_audio(conn)
    row = await report_row(conn, session_id)
    await conn.execute(
        "UPDATE reports SET text_analyzed_at = now() - interval '1 hour', "
        "audio_analyzed_at = now(), delivery_score = 80 WHERE id = %s", (row["id"],),
    )
    assert await repo.complete_overdue_audio(conn, grace_seconds=600) == []
