"""FastStream 워커 뼈대 테스트.

핵심은 **상태 발행이 Spring 이 읽는 네이티브 필드 형식**인지 확인하는 것.
(소비 측 네이티브 필드 → ParseRequest 검증은 실 Redis 로 수동 확인했고, 추후 통합 테스트로 고정한다.)
"""

import json

import pytest

from faststream.asgi import AsgiFastStream

from src.contract import AnalysisStatus, StatusChanged
from src.main import SYNC_ANALYZE_PATH, app, broker, handle_parse_requested, publish_status


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


# ------------------------------------------------- 동기 엔드포인트 배선 (PRD §11)


async def _call_asgi(asgi_app, method: str, path: str, body: bytes = b""):
    """ASGI 앱을 직접 호출하는 최소 클라이언트 — 의존성(httpx 등) 추가 없이 라우팅을 검증한다."""
    sent: list[dict] = []
    consumed = False

    async def receive():
        nonlocal consumed
        if consumed:
            return {"type": "http.disconnect"}
        consumed = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        sent.append(message)

    await asgi_app({"type": "http", "method": method, "path": path, "headers": []}, receive, send)
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    payload = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return status, payload


def test_앱은_AsgiFastStream_이고_동기_경로가_등록됨():
    assert isinstance(app, AsgiFastStream)
    assert SYNC_ANALYZE_PATH == "/internal/analyses/resume"  # Spring 계약 경로 고정
    routes = dict(app.routes)
    assert SYNC_ANALYZE_PATH in routes
    assert "POST" in routes[SYNC_ANALYZE_PATH].methods


@pytest.mark.asyncio
async def test_동기엔드포인트_잘못된_JSON_은_400():
    status, payload = await _call_asgi(app, "POST", SYNC_ANALYZE_PATH, b"{")
    assert status == 400
    assert "잘못된 JSON" in json.loads(payload)["error"]


@pytest.mark.asyncio
async def test_동기엔드포인트_계약위반_바디는_422():
    body = json.dumps({"resumeId": "abc"}).encode()
    status, payload = await _call_asgi(app, "POST", SYNC_ANALYZE_PATH, body)
    assert status == 422
    assert "계약 위반" in json.loads(payload)["error"]


@pytest.mark.asyncio
async def test_동기엔드포인트_GET_은_405():
    status, _ = await _call_asgi(app, "GET", SYNC_ANALYZE_PATH)
    assert status == 405


@pytest.mark.asyncio
async def test_미등록_경로는_404():
    status, _ = await _call_asgi(app, "POST", "/없는/경로")
    assert status == 404
