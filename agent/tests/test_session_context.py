"""디스패치 metadata 파싱 테스트 — docs/prd/interview.md §1 검증 기준."""

import json

from src.session_context import INTERVIEW_TYPE_THIRTY_MIN, SessionContext, parse_job_metadata


def test_full_metadata():
    ctx = parse_job_metadata(
        json.dumps(
            {
                "sessionId": 123,
                "interviewType": "THIRTY_MIN",
                "position": "BACKEND",
                "resumeContext": "기술: Java, Spring",
            }
        )
    )
    assert ctx == SessionContext(
        session_id="123",
        interview_type="THIRTY_MIN",
        position="BACKEND",
        resume_context="기술: Java, Spring",
    )


def test_empty_metadata_falls_back():
    assert parse_job_metadata("") == SessionContext()


def test_invalid_json_falls_back():
    assert parse_job_metadata("not json") == SessionContext()


def test_non_object_json_falls_back():
    assert parse_job_metadata("[1, 2]") == SessionContext()


def test_partial_metadata_defaults():
    ctx = parse_job_metadata("{}")
    assert ctx.session_id is None
    assert ctx.interview_type == INTERVIEW_TYPE_THIRTY_MIN
    assert ctx.position is None
    assert ctx.resume_context is None


def test_empty_or_non_string_optional_fields_become_none():
    ctx = parse_job_metadata(json.dumps({"position": "", "resumeContext": 3}))
    assert ctx.position is None
    assert ctx.resume_context is None


def test_five_min_interview_type_is_kept():
    # 5분 CS 파이프라인은 별도 스토리 — 현재는 값만 보존하고 30분과 동일 진행
    assert parse_job_metadata(json.dumps({"interviewType": "FIVE_MIN"})).interview_type == "FIVE_MIN"


def test_non_string_interview_type_falls_back():
    assert (
        parse_job_metadata(json.dumps({"interviewType": 3})).interview_type
        == INTERVIEW_TYPE_THIRTY_MIN
    )
    assert (
        parse_job_metadata(json.dumps({"interviewType": ""})).interview_type
        == INTERVIEW_TYPE_THIRTY_MIN
    )
