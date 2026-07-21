"""DB 계층 테스트 — 실제 PostgreSQL(pgvector) 대상. 픽스처는 conftest.py 참조."""

import pytest

from src.analysis.chunking import Chunk, ChunkType
from src.contract import AnalysisStatus
from src.storage.repository import (
    count_chunks,
    ensure_schema,
    get_parse_status,
    increment_retry_count,
    load_structured_data,
    mark_failed,
    replace_chunks,
    reset_retry_count,
    save_structured_data,
    try_transition,
)
from src.contract.structured_data import StructuredData
from tests.conftest import DIM, requires_postgres, seed_resume

pytestmark = requires_postgres


@pytest.mark.asyncio
async def test_스키마_멱등_생성(conn):
    await ensure_schema(conn, dim=DIM)  # 두 번 불러도 에러 없음
    cur = await conn.execute("SELECT count(*) AS n FROM resume_chunks")
    assert (await cur.fetchone())["n"] == 0


@pytest.mark.asyncio
async def test_상태_CAS_성공과_양보(conn):
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED)
    # 첫 전이는 성공
    assert await try_transition(conn, rid, AnalysisStatus.UPLOADED, AnalysisStatus.PARSING)
    assert await get_parse_status(conn, rid) == "PARSING"
    # 같은 전이를 다시 시도(중복 처리자) → 이전 상태가 아니므로 양보
    assert not await try_transition(conn, rid, AnalysisStatus.UPLOADED, AnalysisStatus.PARSING)


@pytest.mark.asyncio
async def test_PARSING진입시_started_at_기록(conn):
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED)
    await try_transition(conn, rid, AnalysisStatus.UPLOADED, AnalysisStatus.PARSING)
    cur = await conn.execute(
        "SELECT started_at FROM resume_analysis_status WHERE resume_id = %s", (rid,)
    )
    started = (await cur.fetchone())["started_at"]
    assert started is not None and started.tzinfo is not None  # UTC-aware(timestamptz)


@pytest.mark.asyncio
async def test_mark_failed_멱등_종결상태_보호(conn):
    rid = await seed_resume(conn, AnalysisStatus.PARSING)
    assert await mark_failed(conn, rid, "첫 실패 사유")
    assert await get_parse_status(conn, rid) == "FAILED"
    # 이미 FAILED → 덮어쓰지 않음
    assert not await mark_failed(conn, rid, "다른 사유")
    cur = await conn.execute(
        "SELECT error_message FROM resume_analysis_status WHERE resume_id = %s", (rid,)
    )
    assert (await cur.fetchone())["error_message"] == "첫 실패 사유"
    # EMBEDDED 도 보호
    rid2 = await seed_resume(conn, AnalysisStatus.EMBEDDED)
    assert not await mark_failed(conn, rid2, "x")
    assert await get_parse_status(conn, rid2) == "EMBEDDED"


@pytest.mark.asyncio
async def test_retry_count_리셋과_증가(conn):
    rid = await seed_resume(conn, AnalysisStatus.PARSING)
    await increment_retry_count(conn, rid)
    await increment_retry_count(conn, rid)
    cur = await conn.execute(
        "SELECT retry_count FROM resume_analysis_status WHERE resume_id = %s", (rid,)
    )
    assert (await cur.fetchone())["retry_count"] == 2
    await reset_retry_count(conn, rid)
    cur = await conn.execute(
        "SELECT retry_count FROM resume_analysis_status WHERE resume_id = %s", (rid,)
    )
    assert (await cur.fetchone())["retry_count"] == 0


@pytest.mark.asyncio
async def test_structured_data_저장_후_로드(conn):
    rid = await seed_resume(conn, AnalysisStatus.STRUCTURING)
    data = StructuredData.model_validate(
        {"profile": {"name": "홍길동"}, "skills": [{"category": "백엔드", "items": ["Java"]}]}
    )
    await save_structured_data(conn, rid, data)
    loaded = await load_structured_data(conn, rid)
    assert loaded is not None
    assert loaded.profile.name == "홍길동"
    assert loaded.skills[0].items == ["Java"]


@pytest.mark.asyncio
async def test_structured_data_없으면_None(conn):
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED)
    assert await load_structured_data(conn, rid) is None


def _chunk(label: str, idx: int = 0) -> Chunk:
    return Chunk(
        content=f"[프로젝트] {label}", type=ChunkType.PROJECT,
        source_index=idx, label=label, chunk_version=1,
    )


@pytest.mark.asyncio
async def test_청크_교체_선삭제_후_삽입(conn):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING)
    v = [0.1] * DIM
    async with conn.transaction():
        await replace_chunks(conn, rid, [_chunk("A", 0), _chunk("B", 1)], [v, v])
    assert await count_chunks(conn, rid) == 2
    # 재실행(재개 시나리오) → 기존 2개 삭제 후 1개만
    async with conn.transaction():
        await replace_chunks(conn, rid, [_chunk("C", 0)], [v])
    assert await count_chunks(conn, rid) == 1
    cur = await conn.execute(
        "SELECT content, metadata FROM resume_chunks WHERE resume_id = %s", (rid,)
    )
    row = await cur.fetchone()
    assert row["content"] == "[프로젝트] C"
    assert row["metadata"]["chunk_version"] == 1


@pytest.mark.asyncio
async def test_0청크는_삭제만(conn):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING)
    v = [0.1] * DIM
    async with conn.transaction():
        await replace_chunks(conn, rid, [_chunk("A")], [v])
    async with conn.transaction():
        await replace_chunks(conn, rid, [], [])  # 빈 이력서 재색인
    assert await count_chunks(conn, rid) == 0


@pytest.mark.asyncio
async def test_개수불일치_거부(conn):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING)
    with pytest.raises(ValueError):
        await replace_chunks(conn, rid, [_chunk("A")], [])


@pytest.mark.asyncio
async def test_벡터_유사도_조회_동작(conn):
    """pgvector 코사인 검색이 실제로 동작하는지 (agent top-k 의 기반)."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING)
    a = [1.0] + [0.0] * (DIM - 1)
    b = [0.0, 1.0] + [0.0] * (DIM - 2)
    async with conn.transaction():
        await replace_chunks(conn, rid, [_chunk("A", 0), _chunk("B", 1)], [a, b])
    from pgvector import Vector

    cur = await conn.execute(
        "SELECT label FROM resume_chunks, LATERAL (SELECT metadata->>'label' AS label) t "
        "WHERE resume_id = %s ORDER BY embedding <=> %s LIMIT 1",
        (rid, Vector(a)),
    )
    assert (await cur.fetchone())["label"] == "A"  # a 와 가장 가까운 건 A
