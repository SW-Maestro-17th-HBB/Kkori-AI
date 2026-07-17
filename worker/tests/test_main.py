"""FastStream 워커 뼈대 테스트.

핵심은 **상태 발행이 Spring 이 읽는 네이티브 필드 형식**인지 확인하는 것.
(소비 측 네이티브 필드 → ParseRequest 검증은 실 Redis 로 수동 확인했고, 추후 통합 테스트로 고정한다.)
"""

import pytest

from src.contract import AnalysisStatus, StatusChanged
from src.main import app, broker, handle_parse_requested, publish_status


class _FakeRedis:
    """xadd 호출을 기록하는 가짜 redis."""

    def __init__(self) -> None:
        self.entries: list[tuple[str, dict]] = []

    async def xadd(self, stream: str, fields: dict) -> None:
        self.entries.append((stream, fields))


@pytest.mark.asyncio
async def test_상태발행은_네이티브_필드로():
    fake = _FakeRedis()
    await publish_status(fake, resume_id=5, user_id=3, status=AnalysisStatus.EMBEDDING)

    assert len(fake.entries) == 1
    stream, fields = fake.entries[0]
    assert stream == StatusChanged.STREAM_KEY
    # 각 키가 개별(네이티브) 필드 + 전부 문자열 (Spring 이 그대로 읽는 형식)
    assert fields == {
        "resumeId": "5",
        "userId": "3",
        "status": "EMBEDDING",
        "message": "",
    }


@pytest.mark.asyncio
async def test_상태발행_message_기본은_빈문자열():
    fake = _FakeRedis()
    await publish_status(fake, 1, 1, AnalysisStatus.FAILED)
    assert fake.entries[0][1]["message"] == ""


def test_앱_구성():
    assert broker is not None
    assert app is not None
    assert callable(handle_parse_requested)
