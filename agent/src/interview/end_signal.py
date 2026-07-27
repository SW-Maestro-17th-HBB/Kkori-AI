"""사용자 종료 신호(SendData) 수신 검증 — 신뢰 경계. docs/prd/interview-end.md §3.

프론트 → Spring `POST /end` → Spring이 서버 API SendData로 룸에 종료 신호를 보낸다.
다음 3조건을 모두 만족할 때만 종료 신호로 처리한다:
(1) 발신자가 서버 API — 서버 SDK 발신 data 패킷은 participant가 없다,
(2) 종료 topic 일치, (3) payload의 sessionId가 현재 세션과 일치.
참가자(candidate 포함)가 보낸 동일 topic 메시지는 무시한다 — Spring 관문 우회 차단.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)


def is_end_signal(
    *,
    participant: object | None,
    topic: str | None,
    data: bytes,
    expected_topic: str,
    session_id: str,
) -> bool:
    if participant is not None:
        # 참가자 발신 — 서버 API가 아니다 (프론트·candidate의 관문 우회 차단)
        logger.warning("참가자 발신 종료 topic 메시지 — 무시")
        return False
    if topic != expected_topic:
        return False
    try:
        payload = json.loads(data.decode("utf-8"))
        payload_session_id = payload["sessionId"]
    except Exception:
        # payload 원문은 기록하지 않는다 — 형식만 판정
        logger.warning("종료 신호 payload 파싱 실패 — 무시")
        return False
    if str(payload_session_id) != session_id:
        logger.warning("종료 신호 sessionId 불일치 — 무시")
        return False
    return True


class EndSignalReceiver:
    """수신과 처리의 분리 — 초기화 구간의 종료 신호 유실 방지.

    LiveKit data 메시지는 재전달되지 않으므로 리스너는 룸 이벤트가 흐르기 전에
    등록돼야 한다. 파이프라인이 준비되기 전에 도착한 유효 신호는 보관되고,
    bind 시점에 즉시 전달된다. 룸 이벤트와 bind는 같은 이벤트 루프에서 동기
    실행되므로 별도 잠금이 필요 없다.
    """

    def __init__(self, *, expected_topic: str, session_id: str) -> None:
        self._expected_topic = expected_topic
        self._session_id = session_id
        self._requested = False
        self._on_end: Callable[[], None] | None = None

    def on_data(self, packet) -> None:
        """`data_received` 리스너 — 룸 연결 전에 등록해도 무방하다."""
        if not is_end_signal(
            participant=packet.participant,
            topic=packet.topic,
            data=packet.data,
            expected_topic=self._expected_topic,
            session_id=self._session_id,
        ):
            return
        logger.info("사용자 종료 신호 수신")
        self._requested = True
        if self._on_end is not None:
            self._on_end()

    def bind(self, on_end: Callable[[], None]) -> None:
        """파이프라인 준비 완료 시 호출 — 보류된 신호가 있으면 즉시 전달한다."""
        self._on_end = on_end
        if self._requested:
            on_end()
