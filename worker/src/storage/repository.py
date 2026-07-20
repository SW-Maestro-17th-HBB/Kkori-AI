"""PostgreSQL 계층 — 스키마·상태 전이·산출물 저장 (PRD §2.4, §3, §8).

소유권(§8):
- `resume_chunks` + `CREATE EXTENSION vector` = **워커 소유** → 기동 시 멱등 DDL 로 생성.
- `resumes`(structured_data)·`resume_analysis_status` = 백엔드 소유 → 워커는 읽고/갱신만 한다.

원칙:
- 상태 전이는 **원자적 CAS**(§3.3): `UPDATE ... WHERE parse_status = 이전상태`. 영향 행 0 = 다른
  처리자가 앞서감 → 호출자는 재처리하지 않고 넘어간다.
- 시각은 **UTC-aware** 로 기록(timestamptz, 백엔드 HBB1-232). naive datetime 금지.
- 다단계 원자성(산출물 저장 + 상태 전이, §2.4)은 호출자가 `conn.transaction()` 으로 묶는다.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pgvector import Vector
from pgvector.psycopg import register_vector_async
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.analysis.chunking import Chunk
from src.config import Settings
from src.contract import AnalysisStatus
from src.contract.structured_data import StructuredData


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def connect(settings: Settings) -> AsyncConnection:
    """pgvector 어댑터가 등록된 async 커넥션을 연다 (autocommit — 원자성은 transaction() 으로)."""
    conn = await AsyncConnection.connect(
        settings.postgres_dsn, autocommit=True, row_factory=dict_row
    )
    # vector 타입이 있어야 어댑터 등록이 가능하므로 확장을 먼저 보장한다(멱등, 워커 소유 §8)
    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    await register_vector_async(conn)
    return conn


# ---------------------------------------------------------------- 스키마 (워커 소유)

async def ensure_schema(conn: AsyncConnection, dim: int) -> None:
    """워커 소유 스키마를 멱등 생성한다. 백엔드 소유 테이블은 건드리지 않는다."""
    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS resume_chunks (
            id         BIGSERIAL PRIMARY KEY,
            resume_id  BIGINT NOT NULL,
            content    TEXT NOT NULL,
            metadata   JSONB NOT NULL,
            embedding  vector({dim}) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_resume_chunks_resume_id ON resume_chunks (resume_id)"
    )
    # 유사도 검색용 HNSW 인덱스 (코사인). agent 의 top-k 검색이 사용한다.
    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_resume_chunks_embedding
        ON resume_chunks USING hnsw (embedding vector_cosine_ops)
        """
    )


# ---------------------------------------------------------------- 상태 (원자적 CAS)

async def get_parse_status(conn: AsyncConnection, resume_id: int) -> str | None:
    """현재 파이프라인 상태. 레코드가 없으면 None (유령 이벤트 방어 — 스킵 대상)."""
    cur = await conn.execute(
        "SELECT parse_status FROM resume_analysis_status WHERE resume_id = %s",
        (resume_id,),
    )
    row = await cur.fetchone()
    return row["parse_status"] if row else None


async def try_transition(
    conn: AsyncConnection,
    resume_id: int,
    from_status: AnalysisStatus,
    to_status: AnalysisStatus,
) -> bool:
    """원자적 상태 전이(CAS). True = 내가 전이시킴, False = 다른 처리자가 앞서감(양보).

    부수 시각 기록: PARSING 진입 시 started_at, EMBEDDED 진입 시 completed_at (UTC-aware).
    """
    extra = ""
    if to_status is AnalysisStatus.PARSING:
        extra = ", started_at = %(now)s"
    elif to_status is AnalysisStatus.EMBEDDED:
        extra = ", completed_at = %(now)s"
    cur = await conn.execute(
        f"""
        UPDATE resume_analysis_status
        SET parse_status = %(to)s{extra}, updated_at = %(now)s
        WHERE resume_id = %(rid)s AND parse_status = %(from)s
        """,
        {
            "to": to_status.value,
            "from": from_status.value,
            "rid": resume_id,
            "now": _utcnow(),
        },
    )
    return cur.rowcount == 1


async def mark_failed(conn: AsyncConnection, resume_id: int, message: str) -> bool:
    """FAILED 종결 기록 (§4). 멱등 — 이미 FAILED/EMBEDDED(종결)면 덮어쓰지 않는다."""
    cur = await conn.execute(
        """
        UPDATE resume_analysis_status
        SET parse_status = %(failed)s, error_message = %(msg)s,
            failed_at = %(now)s, updated_at = %(now)s
        WHERE resume_id = %(rid)s AND parse_status NOT IN (%(failed)s, %(embedded)s)
        """,
        {
            "failed": AnalysisStatus.FAILED.value,
            "embedded": AnalysisStatus.EMBEDDED.value,
            "msg": message,
            "rid": resume_id,
            "now": _utcnow(),
        },
    )
    return cur.rowcount == 1


async def reset_retry_count(conn: AsyncConnection, resume_id: int) -> None:
    """새 런 시작 시 0 으로 리셋 (§6 — 신규 메시지에서만, 회수 재개에선 호출하지 않는다)."""
    await conn.execute(
        "UPDATE resume_analysis_status SET retry_count = 0, updated_at = %s WHERE resume_id = %s",
        (_utcnow(), resume_id),
    )


async def increment_retry_count(conn: AsyncConnection, resume_id: int) -> None:
    """내부 재시도마다 즉시 반영 (§6 — 크래시 생존성·관측성)."""
    await conn.execute(
        "UPDATE resume_analysis_status SET retry_count = retry_count + 1, updated_at = %s "
        "WHERE resume_id = %s",
        (_utcnow(), resume_id),
    )


# ---------------------------------------------------------------- 산출물

async def load_structured_data(
    conn: AsyncConnection, resume_id: int
) -> StructuredData | None:
    """REINDEX 입력 — 사용자 수정본이 반영된 structured_data (§2.2)."""
    cur = await conn.execute(
        "SELECT structured_data FROM resumes WHERE id = %s", (resume_id,)
    )
    row = await cur.fetchone()
    if row is None or row["structured_data"] is None:
        return None
    return StructuredData.model_validate(row["structured_data"])


async def save_structured_data(
    conn: AsyncConnection, resume_id: int, data: StructuredData
) -> None:
    """LLM 구조화 결과 저장. 상태 전이(PARSED)와 같은 트랜잭션으로 묶을 것 (§2.4 불변식 1)."""
    await conn.execute(
        "UPDATE resumes SET structured_data = %s, updated_at = %s WHERE id = %s",
        (Jsonb(data.model_dump()), _utcnow(), resume_id),
    )


async def replace_chunks(
    conn: AsyncConnection,
    resume_id: int,
    chunks: list[Chunk],
    embeddings: list[list[float]],
) -> None:
    """기존 청크 전부 삭제 후 재삽입 (§3.1 — 임베딩 진입 시 항상 선삭제, 중복·잔여 청크 방지).

    삭제·삽입의 원자성은 호출자의 `conn.transaction()` 이 보장한다.
    """
    if len(chunks) != len(embeddings):
        raise ValueError(f"청크 {len(chunks)}개 ≠ 임베딩 {len(embeddings)}개")
    await conn.execute("DELETE FROM resume_chunks WHERE resume_id = %s", (resume_id,))
    if not chunks:
        return  # 0청크 이력서 — 삭제만 하고 끝 (§2.5)
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO resume_chunks (resume_id, content, metadata, embedding) "
            "VALUES (%s, %s, %s, %s)",
            [
                (resume_id, c.content, Jsonb(c.metadata()), Vector(e))
                for c, e in zip(chunks, embeddings)
            ],
        )


async def count_chunks(conn: AsyncConnection, resume_id: int) -> int:
    cur = await conn.execute(
        "SELECT count(*) AS n FROM resume_chunks WHERE resume_id = %s", (resume_id,)
    )
    return (await cur.fetchone())["n"]
