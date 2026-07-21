"""파이프라인 라우팅·단계 테스트 (§2, §3.1) — 실제 DB + 가짜 AI/추출."""

import pytest

from src.ai import FakeEmbedder, FakeEnricher, FakeStructurer
from src.config import Settings
from src.contract import AnalysisMode, AnalysisStatus, ParseRequest
from src.storage.repository import count_chunks, get_parse_status, load_structured_data
from src.analysis.pipeline import process_request
from src.contract.structured_data import StructuredData
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


def _fetch_text(text: str = "이력서 원문 텍스트"):
    async def fetch(bucket: str, key: str) -> str:
        return text

    return fetch


async def _run(conn, rid, mode=AnalysisMode.REINDEX, *, text="이력서 원문 텍스트") -> Recorder:
    rec = Recorder()
    await process_request(
        _request(rid, mode),
        conn=conn,
        embedder=FakeEmbedder(dim=DIM),
        structurer=FakeStructurer(StructuredData.model_validate(SD)),
        enricher=FakeEnricher(),
        fetch_text=_fetch_text(text),
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
        conn=conn, embedder=FakeEmbedder(dim=DIM),
        structurer=FakeStructurer(), enricher=FakeEnricher(), fetch_text=_fetch_text(),
        publish=rec, settings=Settings(embedding_dim=DIM),
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
async def test_FULL_행복경로_UPLOADED에서_EMBEDDED까지(conn):
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    rec = await _run(conn, rid, mode=AnalysisMode.FULL)
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    # 구조화 결과가 저장됐고 (§2.4), 청크도 생성됨
    data = await load_structured_data(conn, rid)
    assert data is not None and data.profile.name == "홍길동"
    assert await count_chunks(conn, rid) == 3
    # 단계 이벤트 전부 순서대로 (§1.3)
    assert rec.statuses() == [
        "PARSING", "TEXT_EXTRACTING", "STRUCTURING", "PARSED", "EMBEDDING", "EMBEDDED",
    ]


@pytest.mark.asyncio
async def test_FULL_빈추출이면_FAILED(conn):
    """이미지-only(스캔) PDF — 빈 텍스트 → 구조화로 가지 않고 FAILED (§2.1)."""
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    rec = await _run(conn, rid, mode=AnalysisMode.FULL, text="   \n  ")
    assert await get_parse_status(conn, rid) == "FAILED"
    assert rec.statuses() == ["PARSING", "TEXT_EXTRACTING", "FAILED"]
    assert "OCR 미지원" in rec.events[-1][3]
    assert await load_structured_data(conn, rid) is None  # 구조화 안 함


@pytest.mark.asyncio
async def test_FULL_중간상태_크래시재개는_처음부터(conn):
    """원문 미저장이라 PARSING~STRUCTURING 재개도 처음부터 (§3.1)."""
    rid = await seed_resume(conn, AnalysisStatus.TEXT_EXTRACTING, None)
    rec = await _run(conn, rid, mode=AnalysisMode.FULL)
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert rec.statuses()[0] == "PARSING"  # 처음부터 다시


@pytest.mark.asyncio
async def test_FULL_추출예외는_전파되어_PEL잔류(conn):
    """다운로드 실패·손상 PDF → 예외 전파 = ACK 안 됨 → 재시도 대상."""
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)

    async def broken_fetch(bucket: str, key: str) -> str:
        raise ConnectionError("S3 다운로드 실패")

    rec = Recorder()
    with pytest.raises(ConnectionError):
        await process_request(
            _request(rid, AnalysisMode.FULL),
            conn=conn, embedder=FakeEmbedder(dim=DIM),
            structurer=FakeStructurer(), enricher=FakeEnricher(), fetch_text=broken_fetch,
            publish=rec, settings=Settings(embedding_dim=DIM),
        )
    # 상태는 TEXT_EXTRACTING 에 남음 → 재전달 시 §3.1 표대로 처음부터
    assert await get_parse_status(conn, rid) == "TEXT_EXTRACTING"


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


async def _run_with_delivery(conn, rid, delivery_count, mode=AnalysisMode.FULL) -> Recorder:
    rec = Recorder()
    await process_request(
        _request(rid, mode),
        conn=conn, embedder=FakeEmbedder(dim=DIM),
        structurer=FakeStructurer(StructuredData.model_validate(SD)),
        enricher=FakeEnricher(),
        fetch_text=_fetch_text(), publish=rec,
        settings=Settings(embedding_dim=DIM),  # delivery_count_threshold 기본 3
        delivery_count=delivery_count,
    )
    return rec


@pytest.mark.asyncio
async def test_포기규칙_임계이상이면_재처리없이_FAILED(conn):
    """§4: delivery count 임계(3) 이상 → 처리 없이 FAILED 기록 후 반환(=ACK)."""
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    rec = await _run_with_delivery(conn, rid, delivery_count=3)
    assert await get_parse_status(conn, rid) == "FAILED"
    assert rec.statuses() == ["FAILED"]  # 파이프라인 단계 이벤트 없음 = 재처리 안 함
    assert "재전달 임계 초과" in rec.events[-1][3]
    assert await count_chunks(conn, rid) == 0


@pytest.mark.asyncio
async def test_포기규칙_임계미만은_정상처리(conn):
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    rec = await _run_with_delivery(conn, rid, delivery_count=2)
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert rec.statuses()[0] == "PARSING"  # 정상 파이프라인 진행


@pytest.mark.asyncio
async def test_포기규칙_이미_EMBEDDED면_덮지않음(conn):
    """완료 직후 ACK 만 못 한 메시지가 임계로 회수된 경우 — 완료를 실패로 오염시키지 않는다."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDED, SD)
    rec = await _run_with_delivery(conn, rid, delivery_count=5)
    assert await get_parse_status(conn, rid) == "EMBEDDED"  # 보호됨
    assert rec.events == []  # 이벤트 재발행 없음


@pytest.mark.asyncio
async def test_풍부화_결과가_metadata에_병합됨(conn):
    """§2.6: enricher 가 뽑은 topics·questionHints 가 청크 metadata 에 저장된다."""
    from src.ai import ChunkEnrichment

    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    enrichments = [  # SD 는 청크 3개(프로젝트1·경력1·스킬1)를 만든다
        ChunkEnrichment(topics=["비동기 처리"], relatedConcepts=["Redis Stream"],
                        questionHints=["도입 이유"]),
        ChunkEnrichment(topics=["실무 경험"]),
        ChunkEnrichment(),
    ]
    rec = Recorder()
    await process_request(
        _request(rid), conn=conn, embedder=FakeEmbedder(dim=DIM),
        structurer=FakeStructurer(), enricher=FakeEnricher(enrichments),
        fetch_text=_fetch_text(), publish=rec, settings=Settings(embedding_dim=DIM),
    )
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    cur = await conn.execute(
        "SELECT metadata FROM resume_chunks WHERE resume_id = %s ORDER BY id", (rid,))
    metas = [row["metadata"] for row in await cur.fetchall()]
    assert metas[0]["topics"] == ["비동기 처리"]
    assert metas[0]["relatedConcepts"] == ["Redis Stream"]
    assert metas[0]["questionHints"] == ["도입 이유"]
    assert metas[1]["topics"] == ["실무 경험"]
    assert metas[2]["topics"] == []  # 빈 풍부화도 키는 존재 (부분 상태 없음)
    assert all(m["chunk_version"] == 2 for m in metas)  # 색인 스키마 v2


@pytest.mark.asyncio
async def test_포기시_DB엔_마지막오류_합류_SSE는_간단문구(conn):
    """§4 개선: DB error_message 에는 원인 상세, SSE 이벤트에는 간단 문구."""
    from src.storage.repository import get_error_message, record_last_error

    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    await record_last_error(conn, rid, "ConnectionError: Bedrock 연결 실패")  # 이전 실패의 기록
    rec = await _run_with_delivery(conn, rid, delivery_count=3)

    assert await get_parse_status(conn, rid) == "FAILED"
    db_msg = await get_error_message(conn, rid)
    assert "재전달 임계 초과" in db_msg and "마지막 오류: ConnectionError" in db_msg  # DB 상세
    assert rec.events[-1][3] == "재전달 임계 초과(delivery count=3)"  # SSE 간단 (원인 미포함)
