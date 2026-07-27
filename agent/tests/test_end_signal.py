"""사용자 종료 신호 수신 검증 단위 테스트 — 신뢰 경계 3조건·초기화 구간 보관.
docs/prd/interview-end.md §3."""

import json
from types import SimpleNamespace

from src.interview.end_signal import EndSignalReceiver, is_end_signal

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


# --- 초기화 구간 보관 (EndSignalReceiver) ---

def _packet(**overrides) -> SimpleNamespace:
    fields = dict(participant=None, topic=TOPIC, data=_payload("123"))
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _receiver() -> EndSignalReceiver:
    return EndSignalReceiver(expected_topic=TOPIC, session_id="123")


def test_signal_before_bind_is_buffered_and_delivered_on_bind():
    receiver = _receiver()
    receiver.on_data(_packet())  # 파이프라인 준비 전 도착 — 유실되지 않는다
    ends: list[bool] = []
    receiver.bind(lambda: ends.append(True))
    assert ends == [True]


def test_signal_after_bind_is_delivered_immediately():
    receiver = _receiver()
    ends: list[bool] = []
    receiver.bind(lambda: ends.append(True))
    assert ends == []
    receiver.on_data(_packet())
    assert ends == [True]


def test_invalid_signal_before_bind_is_not_buffered():
    receiver = _receiver()
    receiver.on_data(_packet(participant=object()))  # 참가자 발신 — 무시
    receiver.on_data(_packet(topic="chat:message"))
    ends: list[bool] = []
    receiver.bind(lambda: ends.append(True))
    assert ends == []
