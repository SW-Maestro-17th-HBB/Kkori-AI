"""파이프라인 메트릭 원본 수집·flush — 세션별 이벤트당 1행 jsonb.

STT·TTS·VAD·EOU는 AgentSession의 metrics_collected 이벤트로, 세션 밖 LLM
(orchestrator·interview·초기 선택)은 각 인스턴스의 metrics_collected 이벤트로
같은 수집기에 모은다. 메모리에만 축적하고 잡 종료 시 일괄 flush한다 —
best-effort: 수집·저장의 어떤 실패도 면접을 중단시키지 않으며, transcript와
달리 소모성 데이터라 flush 소진 시 사본 없이 유실을 허용한다.
메트릭은 수치·식별자뿐이라 대화 내용을 포함하지 않는다(로그 프라이버시 방침 무충돌).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

DATABASE_URL_ENV = "KKORI_AGENT_DATABASE_URL"  # transcript_store와 동일 DB

_CONNECT_TIMEOUT_SECONDS = 2
_FLUSH_ATTEMPTS = 2  # 실패 시 1회 재시도 (transcript_store와 동일 계약)
# 다행 배치 INSERT라 transcript(1행)보다 시도 상한을 넉넉히 둔다 — 종료 시퀀스의
# 단계 타임아웃 경로가 아닌 job shutdown 콜백에서 실행되므로 여유가 있다
_ATTEMPT_TIMEOUT_SECONDS = 6.0
_STATEMENT_TIMEOUT_MS = 4000
# VAD처럼 고빈도 발행원이 있어도 장시간 면접이 메모리를 잠식하지 않게 하는 상한
_MAX_ROWS = 10_000

_INSERT_SQL = (
    "INSERT INTO interview_metrics (session_id, ts, kind, payload) "
    "VALUES (%s, %s, %s, %s)"
)

# 축적 행: (이벤트 시각, 종별 판별자, 원본 payload)
MetricsRow = tuple[datetime, str, dict]


class MetricsLog:
    """metrics_collected 수집기 — 동기 핸들러, pydantic 원본을 그대로 축적한다."""

    def __init__(self) -> None:
        self._rows: list[MetricsRow] = []
        self._dropped = 0

    @property
    def rows(self) -> list[MetricsRow]:
        return list(self._rows)

    def handler(self, source: str | None = None) -> Callable[[Any], None]:
        """`.on("metrics_collected", ...)`용 핸들러 — source는 같은 클래스라
        label로 구분되지 않는 LLM 인스턴스(orchestrator/interview 등)를 가른다."""

        def _on_metrics(ev: Any) -> None:
            self.record(ev, source=source)

        return _on_metrics

    def record(self, ev: Any, *, source: str | None = None) -> None:
        try:
            # 세션 이벤트(MetricsCollectedEvent)는 .metrics로 래핑, 인스턴스는 원본
            metrics = getattr(ev, "metrics", ev)
            payload = metrics.model_dump(mode="json")
            kind = payload.get("type") or type(metrics).__name__
            raw_ts = payload.get("timestamp")
            ts = (
                datetime.fromtimestamp(raw_ts, tz=timezone.utc)
                if isinstance(raw_ts, (int, float))
                else datetime.now(timezone.utc)
            )
            if source is not None:
                payload["source"] = source
        except Exception:
            logger.warning("메트릭 직렬화 실패 — 해당 이벤트 폐기", exc_info=True)
            return
        if len(self._rows) >= _MAX_ROWS:
            self._dropped += 1
            if self._dropped == 1:
                logger.warning("메트릭 %d행 상한 도달 — 이후 이벤트 폐기", _MAX_ROWS)
            return
        self._rows.append((ts, kind, payload))


async def flush_metrics(session_id: str, rows: Sequence[MetricsRow]) -> bool:
    """수집된 메트릭을 일괄 INSERT한다. True = flush 완료(빈 rows no-op 포함)."""
    if not rows:
        return True
    url = os.getenv(DATABASE_URL_ENV)
    if not url:
        logger.warning("%s 미설정 — metrics flush 생략", DATABASE_URL_ENV)
        return False
    try:
        numeric_session_id = int(session_id)
    except ValueError:
        # 세션 계약상 sessionId는 숫자(interview_session.id) — 아니면 저장 대상이 아니다
        logger.error("sessionId가 숫자가 아님 — metrics flush 불가")
        return False

    params = [
        (numeric_session_id, ts, kind, Jsonb(payload)) for ts, kind, payload in rows
    ]
    for attempt in range(1, _FLUSH_ATTEMPTS + 1):
        try:
            # 시도 전체(연결+실행+commit)를 상한으로 감싼다 — transcript_store와 동일 근거
            await asyncio.wait_for(
                _flush_once(url, params), _ATTEMPT_TIMEOUT_SECONDS
            )
            logger.info("metrics flush 완료 — %d행", len(params))
            return True
        except Exception as exc:
            logger.warning(
                "metrics flush 실패(%s) — 시도 %d/%d",
                type(exc).__name__,
                attempt,
                _FLUSH_ATTEMPTS,
            )
    logger.error("metrics flush 소진 — %d행 유실(소모성 데이터, 사본 없음)", len(params))
    return False


async def _flush_once(url: str, params: list[tuple]) -> None:
    # 단일 트랜잭션 배치 — 실패 시 전체 롤백이라 재시도가 부분 중복을 만들지 않는다
    async with await psycopg.AsyncConnection.connect(
        url,
        connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        options=f"-c statement_timeout={_STATEMENT_TIMEOUT_MS}",
    ) as conn:
        async with conn.cursor() as cur:
            await cur.executemany(_INSERT_SQL, params)
