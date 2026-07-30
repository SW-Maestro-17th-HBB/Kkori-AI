"""디스패치 metadata 파싱 — docs/prd/interview.md §1 (세션 컨텍스트 주입)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Spring 면접 유형 enum — THIRTY_MIN(30분 이력서 기반, 현재 범위), FIVE_MIN(5분 CS, 별도 스토리)
INTERVIEW_TYPE_THIRTY_MIN = "THIRTY_MIN"
INTERVIEW_TYPE_FIVE_MIN = "FIVE_MIN"


@dataclass(frozen=True)
class SessionContext:
    session_id: str | None = None
    interview_type: str = INTERVIEW_TYPE_THIRTY_MIN
    position: str | None = None
    resume_context: str | None = None


def parse_job_metadata(raw: str) -> SessionContext:
    """metadata JSON을 세션 컨텍스트로 파싱한다.

    부재·형식 오류 시 폴백 컨텍스트를 반환한다
    (콘솔·metadata 없는 로컬 dispatch 테스트 포함).
    resume_context 원문은 개인정보이므로 로그에 남기지 않는다.
    """
    if not raw:
        logger.warning("job metadata 없음 — 세션 컨텍스트 없이 진행")
        return SessionContext()

    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError(f"JSON 객체가 아님: {type(data).__name__}")
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("job metadata 파싱 실패 — 세션 컨텍스트 없이 진행: %s", exc)
        return SessionContext()

    session_id = data.get("sessionId")
    interview_type = data.get("interviewType")
    interview_type = (
        interview_type
        if isinstance(interview_type, str) and interview_type
        else INTERVIEW_TYPE_THIRTY_MIN
    )
    if interview_type == INTERVIEW_TYPE_FIVE_MIN:
        # 5분 CS 파이프라인은 별도 스토리 — 수신 시 처리(동일 진행/거부)는 그때 확정 (PRD §1 제약사항)
        logger.warning("FIVE_MIN(5분 CS) 파이프라인 미구현 — THIRTY_MIN과 동일하게 진행")
    elif interview_type != INTERVIEW_TYPE_THIRTY_MIN:
        # 미지원 값 원문은 로그에 남기지 않는다
        logger.warning("미지원 interviewType(길이 %d) — THIRTY_MIN과 동일하게 진행", len(interview_type))

    position = data.get("position")
    resume_context = data.get("resumeContext")
    return SessionContext(
        session_id=str(session_id) if session_id is not None else None,
        interview_type=interview_type,
        position=position if isinstance(position, str) and position else None,
        resume_context=resume_context if isinstance(resume_context, str) and resume_context else None,
    )
