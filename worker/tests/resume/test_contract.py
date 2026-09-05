"""계약 예시 대조 테스트 (PRD §7).

실제 필드맵 예시(examples/*.json)를 고정해두고, 워커의 계약 모델이
그 예시를 정확히 읽고(decode) / 만드는지(encode) 양방향으로 대조한다.
요청은 스트림 필드로, 상태는 같은 필드맵을 JSON 으로 Pub/Sub 발행한다.
계약이 바뀌면 예시 파일도 바뀌어야 하고, 그 변경이 PR diff에 그대로 드러난다.
"""

import json
from pathlib import Path

import pytest

from src.contract import AnalysisMode, AnalysisStatus, ParseRequest, StatusChanged

EXAMPLES = Path(__file__).parent.parent / "examples"


def _load(name: str) -> dict:
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))


def test_요청_읽기_계약예시와_일치():
    req = ParseRequest.decode(_load("parse_requested.json"))
    assert req.resumeId == 5
    assert req.userId == 3
    assert req.bucket == "kkori-resumes"
    assert req.objectKey == "resumes/3/9f8a2c.pdf"
    assert req.mode is AnalysisMode.FULL


def test_요청_왕복시_문자열맵_보존():
    fields = _load("parse_requested.json")
    assert ParseRequest.decode(fields).encode() == fields


def test_상태_쓰기_계약예시와_일치():
    # message 미지정 → "" 로 직렬화 되어야 예시와 일치
    msg = StatusChanged(resumeId=5, userId=3, status=AnalysisStatus.EMBEDDING)
    assert msg.encode() == _load("status_changed.json")


def test_상태_JSON_페이로드는_encode와_키값이_같다():
    """Pub/Sub 으로 나가는 JSON 을 되읽으면 encode() 결과와 똑같아야 한다 (값 전부 문자열)."""
    encoded = StatusChanged(resumeId=5, userId=3, status=AnalysisStatus.EMBEDDING).encode()
    assert json.loads(json.dumps(encoded, ensure_ascii=False)) == _load("status_changed.json")
    assert all(isinstance(v, str) for v in encoded.values())


def test_상태_message는_항상_문자열():
    msg = StatusChanged(resumeId=1, userId=1, status=AnalysisStatus.FAILED, message="")
    assert msg.encode()["message"] == ""


def test_알수없는_mode는_거부():
    fields = {
        "resumeId": "1",
        "userId": "1",
        "bucket": "b",
        "objectKey": "k",
        "mode": "WRONG",
    }
    with pytest.raises(Exception):
        ParseRequest.decode(fields)


def test_숫자가_아닌_resumeId는_거부():
    fields = {
        "resumeId": "notanumber",
        "userId": "1",
        "bucket": "b",
        "objectKey": "k",
        "mode": "FULL",
    }
    with pytest.raises(Exception):
        ParseRequest.decode(fields)
