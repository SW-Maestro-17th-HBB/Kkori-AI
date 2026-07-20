"""내부 재시도(지수 백오프) 테스트 (§6, §9) — 실제 DB + 가짜 AI/추출."""

import pytest

from src.ai import FakeEmbedder, FakeStructurer
from src.config import Settings
from src.contract import AnalysisMode, AnalysisStatus, ParseRequest
from src.db import get_parse_status
from src.pipeline import process_request
from src.structured_data import StructuredData
from tests.conftest import DIM, requires_postgres, seed_resume
from tests.test_pipeline import SD, Recorder

pytestmark = requires_postgres

# 테스트는 백오프 대기 없이 (지수 백오프 로직은 그대로 타되 sleep 0초)
SETTINGS = Settings(embedding_dim=DIM, retry_base_delay_s=0)


async def _retry_count(conn, rid) -> int:
    cur = await conn.execute(
        "SELECT retry_count FROM resume_analysis_status WHERE resume_id = %s", (rid,)
    )
    return (await cur.fetchone())["retry_count"]


class FlakyFetch:
    """처음 n번 실패 후 성공하는 추출 — 일시 오류 시뮬레이션."""

    def __init__(self, fail_times: int, text: str = "이력서 원문") -> None:
        self.remaining = fail_times
        self.text = text
        self.calls = 0

    async def __call__(self, bucket: str, key: str) -> str:
        self.calls += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise ConnectionError("일시 오류")
        return self.text


async def _run_full(conn, rid, fetch, *, is_reclaimed=False) -> Recorder:
    rec = Recorder()
    await process_request(
        ParseRequest(resumeId=rid, userId=1, bucket="b", objectKey="k", mode=AnalysisMode.FULL),
        conn=conn,
        embedder=FakeEmbedder(dim=DIM),
        structurer=FakeStructurer(StructuredData.model_validate(SD)),
        fetch_text=fetch,
        publish=rec,
        settings=SETTINGS,
        is_reclaimed=is_reclaimed,
    )
    return rec


@pytest.mark.asyncio
async def test_일시오류_재시도후_성공(conn):
    """2번 실패 → 3번째 성공. 런은 완주하고 retry_count 에 실패 횟수가 남는다."""
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    fetch = FlakyFetch(fail_times=2)
    await _run_full(conn, rid, fetch)
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert fetch.calls == 3  # 실패 2 + 성공 1
    assert await _retry_count(conn, rid) == 2  # 실패한 시도마다 즉시 기록 (§6)


@pytest.mark.asyncio
async def test_재시도_소진하면_예외전파_PEL재전달경로(conn):
    """3번 전부 실패 → 예외 전파(ACK 안 됨 → 재전달). 상태는 단계에 남는다."""
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    fetch = FlakyFetch(fail_times=99)
    with pytest.raises(ConnectionError):
        await _run_full(conn, rid, fetch)
    assert fetch.calls == 3  # retry_max_attempts
    assert await _retry_count(conn, rid) == 3
    assert await get_parse_status(conn, rid) == "TEXT_EXTRACTING"  # 체크포인트 잔류


@pytest.mark.asyncio
async def test_신규런은_retry_count_리셋(conn):
    """이전 런의 retry_count 가 남아 있어도 새 메시지 런은 0부터 (§6)."""
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, None)
    await conn.execute(
        "UPDATE resume_analysis_status SET retry_count = 7 WHERE resume_id = %s", (rid,)
    )
    await _run_full(conn, rid, FlakyFetch(fail_times=0))  # 실패 없음
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert await _retry_count(conn, rid) == 0  # 리셋 후 실패 없었으니 0


@pytest.mark.asyncio
async def test_회수재개는_retry_count_유지(conn):
    """is_reclaimed=True 면 같은 런의 연장 — 리셋하지 않는다 (§3.2)."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    await conn.execute(
        "UPDATE resume_analysis_status SET retry_count = 2 WHERE resume_id = %s", (rid,)
    )
    await _run_full(conn, rid, FlakyFetch(fail_times=0), is_reclaimed=True)
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert await _retry_count(conn, rid) == 2  # 유지
