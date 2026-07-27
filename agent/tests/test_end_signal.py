"""사용자 종료 신호 수신 검증 단위 테스트 — 신뢰 경계 3조건. docs/prd/interview-end.md §3."""

import json

from src.interview.end_signal import is_end_signal

TOPIC = "interview:end"


def _payload(session_id) -> bytes:
    return json.dumps({"sessionId": session_id}).encode("utf-8")


def _check(**overrides) -> bool:
    kwargs = dict(
        participant=None,
        topic=TOPIC,
        data=_payload("123"),
        expected_topic=TOPIC,
        session_id="123",
    )
    kwargs.update(overrides)
    return is_end_signal(**kwargs)


def test_server_sent_matching_signal_is_accepted():
    assert _check() is True


def test_numeric_session_id_payload_matches_string_session():
    assert _check(data=_payload(123)) is True


def test_participant_sent_signal_is_rejected():
    # 참가자 발신 동일 topic — Spring 관문 우회 차단
    assert _check(participant=object()) is False


def test_other_topic_is_ignored():
    assert _check(topic="chat:message") is False


def test_session_id_mismatch_is_rejected():
    assert _check(data=_payload("999")) is False


def test_malformed_payload_is_rejected():
    assert _check(data=b"not-json") is False
    assert _check(data=b"{}") is False
