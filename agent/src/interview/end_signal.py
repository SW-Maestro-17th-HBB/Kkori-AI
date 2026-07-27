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
