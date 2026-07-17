"""파이프라인 라우팅·임베딩 단계 테스트 (§2, §3.1) — 실제 DB + 가짜 임베딩."""

import pytest

from src.ai import FakeEmbedder
from src.config import Settings
from src.contract import AnalysisMode, AnalysisStatus, ParseRequest
from src.db import count_chunks, get_parse_status
from src.pipeline import process_request
from tests.conftest import DIM, requires_postgres, seed_resume

pytestmark = requires_postgres

SD = {
    "profile": {"name": "홍길동", "email": "a@b.com"},
    "skills": [{"category": "백엔드", "items": ["Java", "Spring"]}],
    "projects": [
        {"name": "주문 시스템", "role": "백엔드", "description": "Redis Stream 도입.", "techStacks": ["Redis"]}
    ],
    "experiences": [{"title": "인턴", "description": "API 개발."}],
}


class Recorder:
    """발행된 상태 이벤트를 기록하는 가짜 publish."""

    def __init__(self) -> None:
        self.events: list[tuple[int, int, str, str]] = []

    async def __call__(self, rid: int, uid: int, status: AnalysisStatus, message: str) -> None:
        self.events.append((rid, uid, status.value, message))

    def statuses(self) -> list[str]:
        return [e[2] for e in self.events]


def _request(rid: int, mode: AnalysisMode = AnalysisMode.REINDEX) -> ParseRequest:
    return ParseRequest(resumeId=rid, userId=1, bucket="b", objectKey="k", mode=mode)


async def _run(conn, rid, mode=AnalysisMode.REINDEX) -> Recorder:
    rec = Recorder()
    await process_request(
        _request(rid, mode),
        conn=conn,
        embedder=FakeEmbedder(dim=DIM),
        publish=rec,
        settings=Settings(embedding_dim=DIM),
    )
    return rec


@pytest.mark.asyncio
async def test_REINDEX_행복경로_EMBEDDING에서_EMBEDDED까지(conn):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)  # Spring restartFor 상태
    rec = await _run(conn, rid)
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert await count_chunks(conn, rid) == 3  # 프로젝트1+경력1+스킬1
    assert rec.statuses() == ["EMBEDDING", "EMBEDDED"]


@pytest.mark.asyncio
async def test_PARSED에서_재개시_임베딩부터(conn):
    rid = await seed_resume(conn, AnalysisStatus.PARSED, SD)
    rec = await _run(conn, rid, mode=AnalysisMode.FULL)
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert rec.statuses() == ["EMBEDDING", "EMBEDDED"]


@pytest.mark.asyncio
async def test_종결상태는_스킵_이벤트없음(conn):
    for status in (AnalysisStatus.EMBEDDED, AnalysisStatus.FAILED):
        rid = await seed_resume(conn, status, SD)
        rec = await _run(conn, rid)
        assert rec.events == []  # 재발행 없음 (§3.1)
        assert await get_parse_status(conn, rid) == status.value  # 상태 불변


@pytest.mark.asyncio
async def test_레코드없는_유령메시지_스킵(conn):
    rec = Recorder()
    await process_request(
        _request(999999),
        conn=conn, embedder=FakeEmbedder(dim=DIM), publish=rec,
        settings=Settings(embedding_dim=DIM),
    )
    assert rec.events == []


@pytest.mark.asyncio
async def test_EMBEDDING인데_structured_data없으면_FAILED(conn):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, structured_data=None)
    rec = await _run(conn, rid)
    assert await get_parse_status(conn, rid) == "FAILED"
    assert rec.statuses() == ["EMBEDDING", "FAILED"]
    assert "계약 위반" in rec.events[-1][3]


@pytest.mark.asyncio
async def test_REINDEX인데_이른상태면_계약위반_FAILED(conn):
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, SD)
    rec = await _run(conn, rid, mode=AnalysisMode.REINDEX)
    assert await get_parse_status(conn, rid) == "FAILED"
    assert rec.statuses() == ["FAILED"]


@pytest.mark.asyncio
async def test_FULL_이른상태는_아직_미구현(conn):
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    with pytest.raises(NotImplementedError):
        await _run(conn, rid, mode=AnalysisMode.FULL)


@pytest.mark.asyncio
async def test_빈_이력서는_0청크로_EMBEDDED_종결(conn):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, {})  # 전부 빈 구조
    rec = await _run(conn, rid)
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert await count_chunks(conn, rid) == 0
    assert rec.statuses() == ["EMBEDDING", "EMBEDDED"]


@pytest.mark.asyncio
async def test_재실행해도_멱등_청크중복없음(conn):
    """at-least-once 중복 전달 시나리오 — 완료 후 같은 메시지가 또 와도 안전."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    await _run(conn, rid)
    rec2 = await _run(conn, rid)  # 중복 전달
    assert await count_chunks(conn, rid) == 3  # 그대로
    assert rec2.events == []  # 스킵, 재발행 없음
