"""리포트 계약 예시 대조 테스트 — 이력서 계약(test_contract.py)과 같은 방식.

실제 스트림 필드맵 예시(examples/*.json)를 고정해두고, 계약 모델이 그 예시를
정확히 읽고(decode) / 만드는지(encode) 양방향으로 대조한다. 계약이 바뀌면
예시 파일도 함께 바뀌어야 하고, 그 변경이 PR diff에 그대로 드러난다.
"""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.contract import (
    AudioAnalysisRequested,
    RegenerateRequested,
    ReportStatus,
    ReportStatusChanged,
    ReportGenerationRequested,
)

EXAMPLES = Path(__file__).parent.parent / "examples"


def _load(name: str) -> dict:
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("example", "model"),
    [
        ("generation_requested.json", ReportGenerationRequested),
        ("audio_analysis_requested.json", AudioAnalysisRequested),
        ("regenerate_requested.json", RegenerateRequested),
        ("report_status_changed.json", ReportStatusChanged),
    ],
)
def test_예시_왕복_decode_후_encode가_원본과_같다(example, model):
    fields = _load(example)
    assert model.decode(fields).encode() == fields


def test_생성요청_필드값이_예시와_일치한다():
    msg = ReportGenerationRequested.decode(_load("generation_requested.json"))
    assert msg.sessionId == 17


def test_생성요청_모르는_필드는_무시한다():
    """발행 측이 덧붙이는 부가 필드(requestedAt 등)는 계약 위반이 아니다 (2026-07-30 합의)."""
    msg = ReportGenerationRequested.decode(
        {"sessionId": "17", "requestedAt": "2026-07-30T10:00:00Z"}
    )
    assert msg.sessionId == 17
    assert msg.encode() == {"sessionId": "17"}  # 무시된 필드는 재발행에도 없다


def test_상태메시지_status는_계약의_4상태만_허용한다():
    fields = _load("report_status_changed.json")
    assert ReportStatusChanged.decode(fields).status == ReportStatus.PROCESSING

    with pytest.raises(ValidationError):
        ReportStatusChanged.decode({**fields, "status": "EVALUATING"})


def test_상태메시지_message_없으면_빈문자열로_encode된다():
    fields = _load("report_status_changed.json")
    without_message = {k: v for k, v in fields.items() if k != "message"}
    assert ReportStatusChanged.decode(without_message).encode()["message"] == ""


def test_필수필드_누락은_검증오류다():
    with pytest.raises(ValidationError):
        ReportGenerationRequested.decode({"requestedAt": "2026-07-30T10:00:00Z"})  # sessionId 누락
    with pytest.raises(ValidationError):
        RegenerateRequested.decode({"reportId": "7", "userId": "3"})  # sessionId 누락


def test_숫자필드에_문자가_오면_검증오류다():
    with pytest.raises(ValidationError):
        ReportGenerationRequested.decode({"sessionId": "abc"})
