"""livekit-agents 내부 로그의 발화 원문 마스킹 — PRD "답변 원문 운영 로그 금지".

프레임워크는 단일 로거("livekit.agents")로 로그를 내며, 일부 경고의 extra 필드에
STT 원문을 그대로 담는다 — 예: non-interruptible 재생 중 턴 skip 경고의
`user_input`(agent_activity), 우리 설계에서 정상적으로 밟히는 경로다. 운영용 JSON
formatter는 extra를 그대로 출력하므로, 로거 필터로 해당 필드를 마스킹한다.
(로거 레벨 필터는 핸들러 전달 전에 실행되므로 전파된 핸들러에도 마스킹이 적용된다.)
"""

from __future__ import annotations

import logging

# livekit-agents 1.6.6 전수 조사 기준 발화·컨텍스트 원문이 담기는 extra 필드.
# 버전 업그레이드 시 extra={...} 필드 목록을 재확인해 동기화할 것.
_SENSITIVE_FIELDS = ("user_input", "user_transcript", "transcript", "text", "agent_context")
_REDACTED = "[redacted]"


class RedactSpeechExtra(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for field in _SENSITIVE_FIELDS:
            if hasattr(record, field):
                setattr(record, field, _REDACTED)
        return True


def install_privacy_filter() -> None:
    """마스킹 필터를 설치한다 — 멱등, 로거·루트 핸들러 양쪽.

    livekit-agents 대부분은 공유 로거("livekit.agents")로 로그를 내지만 일부 모듈은
    `getLogger(__name__)` 하위 로거를 쓴다 — 하위 로거가 낸 레코드는 상위 로거의
    핸들러로 전파되지만 상위 로거의 **필터는 타지 않으므로**, 루트 핸들러에도 같은
    필터를 건다. 핸들러는 런타임(cli)이 늦게 구성할 수 있어 entrypoint에서
    재호출한다(멱등).
    """
    target = logging.getLogger("livekit.agents")
    if not any(isinstance(existing, RedactSpeechExtra) for existing in target.filters):
        target.addFilter(RedactSpeechExtra())
    for handler in logging.getLogger().handlers:
        if not any(isinstance(existing, RedactSpeechExtra) for existing in handler.filters):
            handler.addFilter(RedactSpeechExtra())
