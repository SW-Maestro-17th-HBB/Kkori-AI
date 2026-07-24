"""livekit 내부 로그 마스킹 테스트 — 답변 원문 운영 로그 금지 (PRD 기타 요구사항)."""

import logging

from src.log_privacy import RedactSpeechExtra, install_privacy_filter


def _cleanup():
    target = logging.getLogger("livekit.agents")
    for existing in list(target.filters):
        if isinstance(existing, RedactSpeechExtra):
            target.removeFilter(existing)


def test_sensitive_extra_fields_are_redacted_before_handlers(caplog):
    install_privacy_filter()
    try:
        with caplog.at_level(logging.WARNING, logger="livekit.agents"):
            logging.getLogger("livekit.agents").warning(
                "skipping reply to user input, current speech generation cannot be interrupted",
                extra={"user_input": "주민번호 990101-1234567입니다", "speech_id": "sp_1"},
            )
        record = caplog.records[0]
        assert record.user_input == "[redacted]"  # STT 원문 마스킹
        assert record.speech_id == "sp_1"  # 비민감 extra는 유지
        assert "990101-1234567" not in caplog.text
    finally:
        _cleanup()


def test_install_is_idempotent():
    install_privacy_filter()
    install_privacy_filter()
    try:
        target = logging.getLogger("livekit.agents")
        count = sum(isinstance(f, RedactSpeechExtra) for f in target.filters)
        assert count == 1
    finally:
        _cleanup()
