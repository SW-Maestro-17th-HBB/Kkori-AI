"""livekit 내부 로그 마스킹 테스트 — 답변 원문 운영 로그 금지 (PRD 기타 요구사항).

테스트가 추가한 필터만 되돌린다 — 세션에 이미 설치돼 있던 필터(예: src.main 임포트로
설치된 것)를 지우면 다른 테스트의 전역 상태를 훼손하므로, 스냅샷 복원 방식을 쓴다.
"""

import logging

import pytest

from src.log_privacy import RedactSpeechExtra, install_privacy_filter


@pytest.fixture
def restore_filters():
    """설치 전 필터 목록을 저장했다가 테스트 후 그대로 복원한다."""
    target = logging.getLogger("livekit.agents")
    root = logging.getLogger()
    before_logger = list(target.filters)
    before_handlers = [(handler, list(handler.filters)) for handler in root.handlers]
    yield
    target.filters[:] = before_logger
    for handler, filters in before_handlers:
        handler.filters[:] = filters


def test_sensitive_extra_fields_are_redacted_before_handlers(restore_filters, caplog):
    install_privacy_filter()
    with caplog.at_level(logging.WARNING, logger="livekit.agents"):
        logging.getLogger("livekit.agents").warning(
            "skipping reply to user input, current speech generation cannot be interrupted",
            extra={"user_input": "주민번호 990101-1234567입니다", "speech_id": "sp_1"},
        )
    record = caplog.records[0]
    assert record.user_input == "[redacted]"  # STT 원문 마스킹
    assert record.speech_id == "sp_1"  # 비민감 extra는 유지
    assert "990101-1234567" not in caplog.text


def test_install_is_idempotent(restore_filters):
    install_privacy_filter()
    install_privacy_filter()
    target = logging.getLogger("livekit.agents")
    count = sum(isinstance(f, RedactSpeechExtra) for f in target.filters)
    assert count == 1


def test_child_logger_records_are_redacted_via_root_handlers(restore_filters):
    """getLogger(__name__) 하위 로거 경로 — 상위 로거 필터는 안 타므로 핸들러 필터가 잡는다."""
    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Capture()
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        install_privacy_filter()  # 이미 존재하는 루트 핸들러에 필터 부착
        child = logging.getLogger("livekit.agents.voice.avatar._queue_io")
        child.warning("audio dropped", extra={"user_input": "주민번호 990101-1234567입니다"})
        captured = [r for r in records if hasattr(r, "user_input")]
        assert captured and captured[0].user_input == "[redacted]"
    finally:
        root.removeHandler(handler)
