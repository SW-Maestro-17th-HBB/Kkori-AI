"""음성 분석(2단계) 파이프라인 — 음성 분석 요청(report.audio.analysis.requested) 소비 흐름.

정상 반환 = ACK(무시·스킵·포기·완료), 예외 전파 = ACK 없음 → PEL 잔류 → 회수 재전달.
의존(커넥션·S3·상태 발행)은 인자로 받아 테스트에서 갈아끼운다 (생성 파이프라인 선례).

흐름 (백엔드 PRD 리포트 §1·§5):
1. 종결(COMPLETED·FAILED) — 무시하고 ACK. COMPLETED 는 보여준 점수를 사후에 바꾸지
   않기 위해(음성 없이 유예 완성된 경우 포함), FAILED 는 생성이 실패한 리포트에 음성
   분석을 얹지 않기 위해. FAILED 재생성은 전달력 없이(유예 완성) 끝난다
2. 이미 분석됨(audio_analyzed_at) — 중복 전달. 분석 없이 완성 판정만 재시도
3. 포기 규칙 — 전달 횟수 임계 도달 시 **FAILED 없이** ACK. 리포트는 유예 완성 경로가
   delivery NULL 로 완성한다 (FAILED 는 텍스트 경로 실패에 한정)
4. 리포트 로우 없음 — 대본의 지원자 발화가 0건이면 "리포트 없음"이 정상이라 ACK,
   발화가 있으면 생성 요청의 소비 지연이므로 예외로 재전달을 기다린다
5. 녹음 다운로드(S3, 내부 재시도) → 결정적 분석(스레드) → 저장 + 완성 판정 한 트랜잭션
"""

from __future__ import annotations

import asyncio
import logging

from psycopg import AsyncConnection

from src.config import Settings
from src.contract import AudioAnalysisRequested, ReportStatus, Speaker
from src.report.audio import DeliveryResult, analyze
from src.report.pipeline import _TERMINAL, PublishStatus, with_retry
from src.report.repository import (
    get_report_by_session,
    load_transcript,
    save_audio_result,
    try_complete,
)

logger = logging.getLogger(__name__)


class ReportNotReady(Exception):
    """지원자 발화가 있는 세션인데 리포트 로우가 아직 없다 — 생성 요청 소비 지연. 재전달 대기."""


def download_object(s3_client, bucket: str, object_key: str) -> bytes:
    """S3/MinIO 객체 다운로드 — blocking. 이력서 PDF 다운로드와 같은 경로."""
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    return response["Body"].read()


async def process_audio_request(
    request: AudioAnalysisRequested,
    *,
    conn: AsyncConnection,
    s3,
    publish: PublishStatus,
    settings: Settings,
    delivery_count: int = 1,
) -> None:
    """음성 분석 요청 1건 처리. 회수 경로도 같은 흐름이다 — 분석은 결정적이라 재개 개념이 없다."""
    sid = request.sessionId
    report = await get_report_by_session(conn, sid)

    if report is not None:
        # 1. 종결 상태 — 무시. COMPLETED 는 보여준 점수를 바꾸지 않기 위해, FAILED 는 생성이
        #    실패한 리포트에 음성 분석을 얹지 않기 위해(2026-09-05 결정). FAILED 를 재생성하면
        #    음성 요청은 이미 버려졌으므로 유예 완성 경로로 전달력 없이 완성된다.
        if report["status"] in _TERMINAL:
            return
        # 2. 중복 전달 — 분석 없이 판정만
        if report["audio_analyzed_at"] is not None:
            await _try_publish_completion(conn, report, publish, settings)
            return

    # 3. 포기 — FAILED 로 만들지 않는다. 흔적은 로그, 완성은 유예 경로 몫.
    if delivery_count >= settings.delivery_count_threshold:
        logger.warning(
            "음성 분석 포기 (session_id=%s, delivery count=%d) — 유예 완성 경로로 넘긴다",
            sid, delivery_count,
        )
        return

    utterances = await load_transcript(conn, sid)
    candidate_text = " ".join(
        u.content for u in (utterances or []) if u.speaker is Speaker.USER
    )

    # 4. 로우 없음 — 무응답 세션이면 정상, 발화가 있으면 소비 지연
    if report is None:
        if not candidate_text:
            logger.info("지원자 발화 없는 세션의 음성 요청 스킵 (session_id=%s)", sid)
            return
        raise ReportNotReady(f"리포트 로우 없음 — 생성 요청 소비 대기 (session_id={sid})")

    # 5. 다운로드(일시 오류 재시도) → 분석 → 저장 + 판정
    recording = await with_retry(
        lambda: asyncio.to_thread(download_object, s3, request.bucket, request.objectKey),
        conn=conn, report_id=report["id"], settings=settings,
    )
    result: DeliveryResult = await asyncio.to_thread(analyze, recording, candidate_text)
    _log_result(sid, result)

    async with conn.transaction():
        saved = await save_audio_result(
            conn, report["id"], delivery_score=result.score, voice_tags=result.tags
        )
        completed = saved and await try_complete(
            conn, report["id"], require_audio=settings.audio_analysis_enabled
        )
    if completed:
        await publish(report["id"], report["user_id"], ReportStatus.COMPLETED, "")


async def _try_publish_completion(
    conn: AsyncConnection, report: dict, publish: PublishStatus, settings: Settings
) -> None:
    if await try_complete(conn, report["id"], require_audio=settings.audio_analysis_enabled):
        await publish(report["id"], report["user_id"], ReportStatus.COMPLETED, "")


def _log_result(session_id: int, result: DeliveryResult) -> None:
    m = result.metrics
    if m is None:
        return
    rate = f"{m.articulation_rate:.2f}" if m.articulation_rate is not None else "-"
    logger.info(
        "전달력 산출 (session_id=%s) score=%s tags=%s | 길이 %.0fs 발성 %.0fs 음절 %d "
        "속도 %s음절/s 침묵비율 %.2f 긴침묵 %d회(%.1f/min)",
        session_id, result.score, [t.tag for t in result.tags],
        m.duration_s, m.phonation_s, m.syllables, rate, m.pause_ratio,
        m.long_pause_count, m.long_pauses_per_minute,
    )
