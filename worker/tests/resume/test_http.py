"""동기 분석 엔드포인트 통합 테스트 (PRD §11) — 실제 DB + 가짜 AI/Redis.

핵심 계약: **2xx = EMBEDDED 종결까지 완료**. 스트림 경로와 같은 배선(`_process`)을
타므로 상태 이벤트 발행까지 동일해야 한다(§11.2 공정 비교 조건).
HTTP 파싱·라우팅은 test_main 배선 테스트가, 처리·판정은 `analyze_sync` 직접 호출로
검증한다(`reclaim_one` 직접 호출 테스트와 같은 스타일).
"""

import asyncio
import json

import pytest

import src.main as main
from src.ai import FakeEmbedder, FakeEnricher, FakeStructurer
from src.config import Settings
from src.contract import AnalysisMode, AnalysisStatus, ParseRequest
from src.contract.structured_data import StructuredData
from src.storage.repository import count_chunks, create_pool, get_parse_status
from tests.conftest import DIM, TEST_DSN, requires_postgres, seed_resume
from tests.resume.test_main import _call_asgi, _FakeRedis
from tests.resume.test_pipeline import SD

pytestmark = requires_postgres


@pytest.fixture
def wired(conn, monkeypatch):
    """main 모듈의 자원·설정을 테스트용으로 배선 (test_reclaim 과 동일 패턴)."""
    monkeypatch.setattr(main._Resources, "db", conn)
    monkeypatch.setattr(main._Resources, "embedder", FakeEmbedder(dim=DIM))
    monkeypatch.setattr(
        main._Resources, "structurer", FakeStructurer(StructuredData.model_validate(SD))
    )
    monkeypatch.setattr(main._Resources, "enricher", FakeEnricher())
    monkeypatch.setattr(main.settings, "embedding_dim", DIM)


def _request(rid: int, mode: AnalysisMode = AnalysisMode.REINDEX) -> ParseRequest:
    return ParseRequest(resumeId=rid, userId=1, bucket="b", objectKey="k", mode=mode)


def _statuses(fake_redis: _FakeRedis) -> list[str]:
    return [fields["status"] for _, fields in fake_redis.entries]


def _body(resp) -> dict:
    return json.loads(resp.body)


@pytest.mark.asyncio
async def test_REINDEX_동기호출_200과_EMBEDDED(conn, wired):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)  # Spring restartFor 상태
    fake = _FakeRedis()

    resp = await main.analyze_sync(_request(rid), fake)

    assert resp.status_code == 200
    assert _body(resp)["status"] == "EMBEDDED"
    # 2xx 시점에 DB 가 이미 종단 상태 — 계약의 핵심
    assert await get_parse_status(conn, rid) == "EMBEDDED"
    assert await count_chunks(conn, rid) == 3
    # 스트림 경로와 동일한 상태 이벤트 발행 (§11.2)
    assert _statuses(fake) == ["EMBEDDING", "EMBEDDED"]


@pytest.mark.asyncio
async def test_FULL_동기호출_200과_EMBEDDED(conn, wired, monkeypatch):
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED)

    async def fake_fetch(bucket: str, key: str) -> str:
        return "이력서 원문 텍스트"

    monkeypatch.setattr(main, "fetch_text", fake_fetch)  # S3 격리

    resp = await main.analyze_sync(_request(rid, AnalysisMode.FULL), _FakeRedis())

    assert resp.status_code == 200
    assert await get_parse_status(conn, rid) == "EMBEDDED"


@pytest.mark.asyncio
async def test_유령_resumeId_는_404(conn, wired):
    fake = _FakeRedis()
    resp = await main.analyze_sync(_request(999_999), fake)

    assert resp.status_code == 404
    assert fake.entries == []  # 유령은 이벤트도 없다


@pytest.mark.asyncio
async def test_REINDEX_계약위반은_500과_FAILED(conn, wired):
    # REINDEX 인데 이른 상태(UPLOADED) — 파이프라인이 예외 없이 FAILED 로 종결하는 경로 (§2.3)
    rid = await seed_resume(conn, AnalysisStatus.UPLOADED, SD)

    resp = await main.analyze_sync(_request(rid), _FakeRedis())

    assert resp.status_code == 500
    assert await get_parse_status(conn, rid) == "FAILED"


@pytest.mark.asyncio
async def test_이미_EMBEDDED_면_스킵하고_200(conn, wired):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDED, SD)
    fake = _FakeRedis()

    resp = await main.analyze_sync(_request(rid), fake)

    assert resp.status_code == 200  # 멱등 — 중복 호출도 성공 (§2.4)
    assert fake.entries == []  # 스킵이라 재발행 없음


@pytest.mark.asyncio
async def test_처리중_예외는_500과_에러노출(conn, wired, monkeypatch):
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)

    async def boom(*args, **kwargs):
        raise RuntimeError("의도된 실패")

    monkeypatch.setattr(main, "process_request", boom)

    resp = await main.analyze_sync(_request(rid), _FakeRedis())

    assert resp.status_code == 500
    # 실패를 숨기지 않는다 — 원인(예외 타입·메시지)이 바디에 보여야 한다 (§11.1)
    assert "RuntimeError" in _body(resp)["error"]


# ------------------------------------------------- 커넥션 풀 (§11.4)


@pytest.fixture
def wired_providers(conn, monkeypatch):
    """AI 자원만 배선 — `_Resources.db` 는 비워서 analyze_sync 가 실제 풀 경로를 탄다."""
    monkeypatch.setattr(main._Resources, "embedder", FakeEmbedder(dim=DIM))
    monkeypatch.setattr(
        main._Resources, "structurer", FakeStructurer(StructuredData.model_validate(SD))
    )
    monkeypatch.setattr(main._Resources, "enricher", FakeEnricher())
    monkeypatch.setattr(main.settings, "embedding_dim", DIM)


async def _open_pool(max_size: int):
    pool = create_pool(Settings(postgres_dsn=TEST_DSN), max_size=max_size)
    await pool.open()
    return pool


@pytest.mark.asyncio
async def test_풀_경유_동기호출_200과_EMBEDDED(conn, wired_providers, monkeypatch):
    """풀에서 빌린 연결로 파이프라인·상태 재조회까지 실제 전 구간 검증."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    pool = await _open_pool(2)
    monkeypatch.setattr(main._Resources, "pool", pool)
    try:
        resp = await main.analyze_sync(_request(rid), _FakeRedis())
        assert resp.status_code == 200
        assert await get_parse_status(conn, rid) == "EMBEDDED"
        assert await count_chunks(conn, rid) == 3
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_스트림_경로도_풀에서_연결을_빌린다(conn, wired_providers, monkeypatch):
    """conn 인자·주입 없이 `_process` 를 부르면(스트림 경로와 동일) 풀 대여 브랜치를 탄다 (§11.4)."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    pool = await _open_pool(2)
    monkeypatch.setattr(main._Resources, "pool", pool)
    try:
        await main._process(_request(rid), delivery_count=1, redis=_FakeRedis())

        assert await get_parse_status(conn, rid) == "EMBEDDED"
        stats = pool.get_stats()
        assert stats["pool_size"] == stats["pool_available"]  # 빌린 연결이 반납됐다
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_동시_300요청에도_DB연결은_상한_이하(conn, wired_providers, monkeypatch):
    """수용 기준: 동시 300건에도 PG 연결 ≤ 풀 상한 — too many clients 구조적 불가."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDED, SD)  # 스킵 경로 — 처리 자체는 단순화
    pool = await _open_pool(5)
    monkeypatch.setattr(main._Resources, "pool", pool)

    async def slow_process(*args, **kwargs):  # 연결을 쥔 채 머무는 시간을 만든다
        await asyncio.sleep(0.05)

    monkeypatch.setattr(main, "process_request", slow_process)

    peak = 0
    stop = asyncio.Event()

    async def watch_connections():
        nonlocal peak
        while not stop.is_set():
            cur = await conn.execute(
                "SELECT count(*) AS n FROM pg_stat_activity WHERE datname = current_database()"
            )
            peak = max(peak, (await cur.fetchone())["n"])
            await asyncio.sleep(0.02)

    watcher = asyncio.create_task(watch_connections())
    try:
        responses = await asyncio.gather(
            *[main.analyze_sync(_request(rid), _FakeRedis()) for _ in range(300)]
        )
    finally:
        stop.set()
        await watcher
        await pool.close()

    assert [r.status_code for r in responses] == [200] * 300  # too many clients 였다면 500
    # 풀 5 + 관측용 conn 픽스처 1 (+여유 1) — 유입 300 과 무관하게 유한
    assert peak <= 5 + 2, f"피크 연결 수 {peak} — 상한 초과"


@pytest.mark.asyncio
async def test_풀_대기_초과는_503(conn, wired_providers, monkeypatch):
    """연결 1개를 다른 요청이 쥐고 있으면 타임아웃까지 대기 후 503."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDED, SD)
    pool = await _open_pool(1)
    monkeypatch.setattr(main._Resources, "pool", pool)
    monkeypatch.setattr(main.settings, "db_pool_wait_timeout_s", 0.05)

    async def slow_process(*args, **kwargs):
        await asyncio.sleep(0.5)

    monkeypatch.setattr(main, "process_request", slow_process)

    try:
        first = asyncio.create_task(main.analyze_sync(_request(rid), _FakeRedis()))
        await asyncio.sleep(0.1)  # 첫 요청이 유일한 연결을 잡을 시간
        second = await main.analyze_sync(_request(rid), _FakeRedis())

        assert second.status_code == 503
        assert "대기 초과" in _body(second)["error"]
        assert (await first).status_code == 200  # 먼저 빌린 쪽은 정상 완료
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_ASGI_종단_행복경로(conn, wired, monkeypatch):
    """핸들러(JSON 파싱·검증) → 배선(analyze_sync) 연결의 사각지대 방지 — 앱 전체 호출 1건."""
    rid = await seed_resume(conn, AnalysisStatus.EMBEDDING, SD)
    monkeypatch.setattr(main.broker, "_connection", _FakeRedis(), raising=False)

    body = json.dumps(
        {"resumeId": rid, "userId": 1, "bucket": "b", "objectKey": "k", "mode": "REINDEX"}
    ).encode()
    status, payload = await _call_asgi(main.app, "POST", main.SYNC_ANALYZE_PATH, body)

    assert status == 200
    assert json.loads(payload)["status"] == "EMBEDDED"
    assert await get_parse_status(conn, rid) == "EMBEDDED"
