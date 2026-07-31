"""디스패치 metadata 파싱 테스트 — docs/prd/interview.md §1 검증 기준."""

import json

from src.session_context import INTERVIEW_TYPE_THIRTY_MIN, SessionContext, parse_job_metadata


# 세션 생성 계약 픽스처 (canonical sample) — 원본: Kkori-Backend
# docs/requirements/session/agent-dispatch.md. 백엔드(DispatchMetadataAssemblerTest)는
# 조립 산출물이 이 문자열과 자구까지 일치함을, 여기서는 같은 문자열의 파싱을 검증한다.
# 자구(compact·필드 순서·비ASCII 원문) 하나라도 바꾸면 계약 변경 — 양 레포 합의·동시 반영 필수.
CONTRACT_METADATA = (
    r'{"sessionId":"123","interviewType":"THIRTY_MIN","position":"BACKEND",'
    r'"resumeContext":"[기술 스택]\n- 언어: Java, Python\n- 프레임워크: Spring Boot'
    r'\n\n[프로젝트]\n- Kkori (백엔드): AI 면접 준비 서비스의 세션 생성 API와 LiveKit '
    r'실시간 음성 연동을 설계·구현. user 행 잠금 기반 동시성 제어로 유저당 단일 세션 불변식을 보장'
    r' (기술: Spring Boot, PostgreSQL)'
    r'\n\n[경험]\n- ABC 커머스 인턴: 결제 정산 배치의 지연 문제를 인덱스 재설계로 개선하고 '
    r'처리 시간을 40% 단축"}'
)

CONTRACT_METADATA_NO_RESUME = '{"sessionId":"124","interviewType":"FIVE_MIN","position":"FRONTEND"}'

# 파싱 기대값 — JSON 이스케이프(\n)가 실제 개행으로 풀린 형태
CONTRACT_RESUME_CONTEXT = (
    "[기술 스택]\n- 언어: Java, Python\n- 프레임워크: Spring Boot"
    "\n\n[프로젝트]\n- Kkori (백엔드): AI 면접 준비 서비스의 세션 생성 API와 LiveKit "
    "실시간 음성 연동을 설계·구현. user 행 잠금 기반 동시성 제어로 유저당 단일 세션 불변식을 보장"
    " (기술: Spring Boot, PostgreSQL)"
    "\n\n[경험]\n- ABC 커머스 인턴: 결제 정산 배치의 지연 문제를 인덱스 재설계로 개선하고 "
    "처리 시간을 40% 단축"
)


def test_contract_fixture_parses_verbatim():
    assert parse_job_metadata(CONTRACT_METADATA) == SessionContext(
        session_id="123",
        interview_type="THIRTY_MIN",
        position="BACKEND",
        resume_context=CONTRACT_RESUME_CONTEXT,
    )


def test_contract_fixture_without_resume():
    # resumeContext는 이력서 데이터가 없으면 필드 자체를 생략한다 (계약).
    # FIVE_MIN은 경고 후 THIRTY_MIN과 동일 진행 — interviewType 값은 보존된다
    assert parse_job_metadata(CONTRACT_METADATA_NO_RESUME) == SessionContext(
        session_id="124",
        interview_type="FIVE_MIN",
        position="FRONTEND",
        resume_context=None,
    )


def test_contract_fixture_is_canonical_serialization():
    # 픽스처가 계약 직렬화 자구(compact·비ASCII 원문·필드 순서)와 일치하는지 자가 검증 —
    # 재생성 시 ensure_ascii=False 누락(한글 \uXXXX 이스케이프) 같은 표류를 잡는다
    for fixture in (CONTRACT_METADATA, CONTRACT_METADATA_NO_RESUME):
        assert json.dumps(json.loads(fixture), ensure_ascii=False, separators=(",", ":")) == fixture


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
