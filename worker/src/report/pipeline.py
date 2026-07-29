"""리포트 생성 파이프라인 — 생성 요청(report.generation.requested) 소비 흐름의 조립.

정상 반환 = ACK(종결·스킵·양보·포기), 예외 전파 = ACK 없음 → PEL 잔류 → 회수 재전달.
모든 의존(커넥션·평가기·상태 발행)은 인자로 받아 테스트에서 갈아끼운다 (이력서 선례).

흐름 (백엔드 PRD 리포트 §2·§5):
1. 포기 규칙 — 전달 횟수 임계 도달 시 FAILED 확정 + ACK
2. 멱등 판단 — 종결 스킵 / 텍스트 완료면 완성 판정만 재시도
3. 유령 방어 — 대본·세션 없으면 로우 생성 전에 스킵 (쓰레기 PENDING 방지)
4. 로우 확보 — 리포트+Job 한 트랜잭션 생성, 유니크 충돌은 양보
5. PROCESSING 진입 — CAS. 신규 경로에서 이미 PROCESSING 이면 양보,
   회수 경로면 죽은 처리자의 몫을 이어받아 재개(전이 없이 진행)
6. 평가 — 주제(본질문+꼬리)별 LLM 호출, 내부 재시도(지수 백오프)
7. 총평 + 집계(평균·태그 상위 3 — 결정적 코드)
8. 저장 — 산출물 일괄 + text_analyzed_at (한 트랜잭션)
9. 완성 판정 — 음성까지 끝났으면 COMPLETED + 발행 (아니면 음성 쪽이 나중에 판정)
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections import Counter
from typing import Awaitable, Callable, TypeVar

from psycopg import AsyncConnection

from src.config import Settings
from src.contract import (
    ReportGenerationRequested,
    ReportStatus,
    WeaknessTagCount,
    group_utterances,
)
from src.report.evaluator import (
    TEXT_WEAKNESS_TAGS,
    EvaluatedAnswer,
    Evaluator,
    group_topics,
)
from src.report.repository import (
    FeedbackRecord,
    SessionScores,
    create_report_with_job,
    get_job_error,
    get_report_by_session,
    increment_job_retry,
    load_snapshot_source,
    load_transcript,
    mark_failed,
    record_job_error,
    reset_job_retry,
    save_text_results,
    try_complete,
    try_transition,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# 상태 발행 콜백: (report_id, user_id, status, message)
PublishStatus = Callable[[int, int, ReportStatus, str], Awaitable[None]]

_TERMINAL = {ReportStatus.COMPLETED.value, ReportStatus.FAILED.value}


async def process_generation_request(
    request: ReportGenerationRequested,
    *,
    conn: AsyncConnection,
    evaluator: Evaluator,
    publish: PublishStatus,
    settings: Settings,
    delivery_count: int = 1,
    is_reclaimed: bool = False,
) -> None:
    """생성 요청 1건 처리.

    is_reclaimed: 회수(XAUTOCLAIM) 경로 여부. 신규 메시지는 새 런이라 retry_count 를
    리셋하고 진행 중(PROCESSING) 로우를 만나면 양보하지만, 회수 경로는 이전 처리자가
    죽었다는 판정이므로 리셋 없이 이어서 재개한다.
    """
    sid, uid = request.sessionId, request.userId
    report = await get_report_by_session(conn, sid)

    # 1. 포기 규칙: 임계 도달 시 재처리 없이 FAILED 확정 → 정상 반환(=ACK).
    #    이 순서 고정 — 도중에 죽어도 재전달본이 같은 경로로 수렴한다(mark_failed 멱등).
    if delivery_count >= settings.delivery_count_threshold:
        await _give_up(conn, report, uid, publish, delivery_count)
        return

    if report is not None:
        # 2a. 종결 상태 = at-least-once 중복 → 스킵, 이벤트 재발행 없음
        if report["status"] in _TERMINAL:
            return
        # 2b. 텍스트는 끝났고 완성 판정 직전에 죽은 재전달 → 평가 없이 판정만 재시도
        if report["text_analyzed_at"] is not None:
            if await try_complete(conn, report["id"]):
                await publish(report["id"], uid, ReportStatus.COMPLETED, "")
            return

    # 3. 유령 방어 — 로우를 만들기 전에 걸러 쓰레기 PENDING 을 남기지 않는다.
    #    (계약상 대본 저장 → 요청 발행 순서이므로, 대본 없음은 유령이지 타이밍이 아니다)
    utterances = await load_transcript(conn, sid)
    if utterances is None:
        logger.warning("대본 없는 생성 요청 스킵 (session_id=%s)", sid)
        return
    pairs = group_utterances(utterances)
    if not pairs:
        logger.warning("문답 없는 대본 스킵 (session_id=%s)", sid)
        return

    # 4. 로우 확보 — 없으면 생성(리포트+Job 한 트랜잭션), 유니크 충돌 = 경쟁 패배 → 양보
    if report is None:
        source = await load_snapshot_source(conn, sid)
        if source is None:
            logger.warning("세션 또는 이력서 없는 생성 요청 스킵 (session_id=%s)", sid)
            return
        report_id = await create_report_with_job(
            conn,
            session_id=sid,
            user_id=uid,
            resume_id=source["resume_id"],
            resume_file_name=source["original_file_name"],
        )
        if report_id is None:
            return  # 다른 처리자가 방금 생성 — 그쪽이 진행한다
        row_status = ReportStatus.PENDING.value
    else:
        report_id = report["id"]
        row_status = report["status"]

    # 5. PROCESSING 진입
    if row_status == ReportStatus.PENDING.value:
        if not await try_transition(
            conn, report_id, ReportStatus.PENDING, ReportStatus.PROCESSING
        ):
            return  # 다른 처리자가 앞서감 — 양보
        await publish(report_id, uid, ReportStatus.PROCESSING, "")
    elif not is_reclaimed:
        return  # 신규 경로에서 PROCESSING 미완 = 다른 처리자가 진행 중 — 양보
    # (회수 경로의 PROCESSING 미완 = 이전 처리자 사망 판정 — 전이·재발행 없이 재개)

    if not is_reclaimed:
        await reset_job_retry(conn, report_id)

    # 6~7. 주제별 평가 → 총평
    evaluated = await _evaluate_all(
        pairs, conn=conn, report_id=report_id, evaluator=evaluator, settings=settings
    )
    summary = await _with_retry(
        lambda: asyncio.to_thread(evaluator.summarize, evaluated),
        conn=conn, report_id=report_id, settings=settings,
    )

    # 8. 집계(결정적 코드) + 일괄 저장
    await save_text_results(
        conn,
        report_id,
        scores=aggregate_scores(evaluated),
        feedbacks=[_to_feedback(a) for a in evaluated],
        summary=summary,
        tag_summary=aggregate_tag_summary(evaluated),
    )

    # 9. 완성 판정 — 음성도 끝나 있으면 나중에 끝난 이쪽이 COMPLETED 를 확정한다
    if await try_complete(conn, report_id):
        await publish(report_id, uid, ReportStatus.COMPLETED, "")


# ---------------------------------------------------------------- 실패 경로

async def _give_up(
    conn: AsyncConnection,
    report: dict | None,
    user_id: int,
    publish: PublishStatus,
    delivery_count: int,
) -> None:
    """포기 규칙 — FAILED 확정(멱등) 후 이벤트 발행. 로우가 없으면 기록할 곳이 없다."""
    simple = f"재전달 임계 초과(delivery count={delivery_count})"
    if report is None:
        # 로우 생성 전에만 반복 실패한 경우 — 흔적 없이 버린다(조정 배치가 로우 부재로 감지)
        logger.warning("리포트 로우 없이 포기 — %s", simple)
        return
    last = await get_job_error(conn, report["id"])
    detail = f"{simple} — 마지막 오류: {last}" if last else simple
    if await mark_failed(conn, report["id"], detail):
        await publish(report["id"], user_id, ReportStatus.FAILED, simple)


async def _with_retry(
    thunk: Callable[[], Awaitable[T]],
    *,
    conn: AsyncConnection,
    report_id: int,
    settings: Settings,
) -> T:
    """외부 호출(LLM)의 일시 오류 내부 재시도 — 이력서 §9 와 동일 정책.

    실패할 때마다 Job.retry_count 를 즉시 +1 하고(크래시 생존성·관측성),
    지수 백오프(1s→2s→4s) 후 재시도한다. 소진하면 마지막 예외를 전파한다
    → 핸들러에서 ACK 없이 끝나 PEL 재전달(회수)로 이어진다.
    """
    for attempt in range(settings.retry_max_attempts):
        try:
            return await thunk()
        except Exception as e:
            await increment_job_retry(conn, report_id)
            if attempt + 1 >= settings.retry_max_attempts:
                # 포기 시 실패 사유에 합류시킬 마지막 오류 (타입명만 — 원문은 로그가 담당)
                await record_job_error(conn, report_id, type(e).__name__)
                raise
            await asyncio.sleep(settings.retry_base_delay_s * (2**attempt))
    raise AssertionError("unreachable")  # for 문은 반드시 return 또는 raise 로 끝난다


# ---------------------------------------------------------------- 평가·집계

async def _evaluate_all(
    pairs,
    *,
    conn: AsyncConnection,
    report_id: int,
    evaluator: Evaluator,
    settings: Settings,
) -> list[EvaluatedAnswer]:
    """주제별 순차 평가 — LLM 호출은 blocking 이라 스레드로 넘긴다 (동시화는 실측 후)."""
    evaluated: list[EvaluatedAnswer] = []
    for topic in group_topics(pairs):
        results = await _with_retry(
            lambda t=topic: asyncio.to_thread(evaluator.evaluate_topic, t),
            conn=conn, report_id=report_id, settings=settings,
        )
        evaluated.extend(
            EvaluatedAnswer(qa=qa, evaluation=e) for qa, e in zip(topic, results)
        )
    return evaluated


def _round_mean(values: list[int]) -> int:
    """평균 반올림(0.5 는 올림) — try_complete 의 SQL round 와 같은 방식."""
    return math.floor(sum(values) / len(values) + 0.5)


def aggregate_scores(evaluated: list[EvaluatedAnswer]) -> SessionScores:
    """영역 점수 = 답변별 해당 축 점수의 평균 반올림 (백엔드 PRD §1 점수 체계)."""
    return SessionScores(
        logic_score=_round_mean([a.evaluation.logicScore for a in evaluated]),
        specificity_score=_round_mean([a.evaluation.specificityScore for a in evaluated]),
        technical_accuracy_score=_round_mean(
            [a.evaluation.technicalAccuracyScore for a in evaluated]
        ),
    )


def aggregate_tag_summary(evaluated: list[EvaluatedAnswer]) -> list[WeaknessTagCount]:
    """태그 빈도 상위 3개. 동률은 어휘집 순서로 — 실행마다 같은 결과가 나오게."""
    counts = Counter(tag for a in evaluated for tag in a.evaluation.weaknessTags)
    vocabulary_order = {tag: i for i, tag in enumerate(TEXT_WEAKNESS_TAGS)}
    ranked = sorted(
        counts.items(), key=lambda kv: (-kv[1], vocabulary_order.get(kv[0], len(vocabulary_order)))
    )
    return [WeaknessTagCount(tag=tag, count=count) for tag, count in ranked[:3]]


def _to_feedback(answer: EvaluatedAnswer) -> FeedbackRecord:
    """평가 결과 → 저장 형태. question_number 는 반향 검증을 통과한 번호를 쓴다."""
    e = answer.evaluation
    return FeedbackRecord(
        question_number=e.questionNumber,
        logic_score=e.logicScore,
        specificity_score=e.specificityScore,
        technical_accuracy_score=e.technicalAccuracyScore,
        feedback=e.feedback,
        weakness_tags=e.weaknessTags,
        improvement_tasks=e.improvementTasks,
        resume_context=None,  # 이력서 근거 인용은 이번 범위 밖 — 컬럼은 비워 둔다
    )
