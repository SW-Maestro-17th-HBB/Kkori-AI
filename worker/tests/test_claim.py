"""XREADGROUP CLAIM 공통 모듈 테스트 — 서버 버전 판정."""

import pytest

from src.messaging.claim import ensure_claim_supported, supports_claim
from tests.conftest import requires_redis_with_claim


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("7.4.2", False),
        ("8.2.1", False),
        ("8.4.0", True),
        ("8.10.2", True),  # 마이너를 문자열로 비교하면 8.10 < 8.4 가 된다
        ("9.0.0", True),
    ],
)
def test_CLAIM_지원_버전_판정(version, expected):
    assert supports_claim(version) is expected


@requires_redis_with_claim
@pytest.mark.asyncio
async def test_지원하는_Redis_면_기동_확인을_통과한다():
    await ensure_claim_supported("redis://localhost:6379")


@pytest.mark.asyncio
async def test_지원하지_않는_Redis_면_기동을_막는다(monkeypatch):
    class _OldRedis:
        async def info(self, section):
            return {"redis_version": "7.4.2"}

        async def aclose(self):
            pass

    monkeypatch.setattr("src.messaging.claim.Redis.from_url", lambda url: _OldRedis())

    with pytest.raises(RuntimeError, match="7.4.2"):
        await ensure_claim_supported("redis://unused")
