"""세션 메타·owner·복원 상태의 Redis 저장소 — docs/prd/interview-recovery.md §2.

세션 메타(`interview:{sessionId}:meta` 해시)는 복원 재료(시작 시각·candidate
identity·재연결 deadline)를 모은다. `owner`는 메타 밖의 별도 키다 — 복원 재료가
아니라 종결 단계 가드라서 수명이 다르다(메타는 flush 성공 시 purge되지만 owner는
그 뒤의 룸 삭제까지 유효해야 한다).

모든 쓰기는 best-effort다 — 실패해도 면접은 계속되고, 복원은 재해 복구 층위다.
해시 쓰기는 TTL을 만들지도 갱신하지도 않으므로 쓰기+EXPIRE를 원자 실행한다
(transcript writer의 RPUSH+EXPIRE와 같은 패턴).
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone

from redis.asyncio import Redis

from src.config import REDIS_TRANSCRIPT_TTL_SECONDS
from src.interview.redis_sink import REDIS_URL_ENV

logger = logging.getLogger(__name__)

_OP_TIMEOUT_SECONDS = 2.0

_META_STARTED_AT = "startedAt"
_META_CANDIDATE_IDENTITY = "candidateIdentity"
_META_RECONNECT_DEADLINE = "reconnectDeadline"


def _meta_key(session_id: str) -> str:
    return f"interview:{session_id}:meta"


def _owner_key(session_id: str) -> str:
    return f"interview:{session_id}:owner"


def _transcript_key(session_id: str) -> str:
    return f"interview:{session_id}:transcript"


def _termination_key(session_id: str) -> str:
    return f"interview:{session_id}:termination"


def _client(url: str) -> Redis:
    return Redis.from_url(
        url,
        socket_timeout=_OP_TIMEOUT_SECONDS,
        socket_connect_timeout=_OP_TIMEOUT_SECONDS,
        decode_responses=True,
    )


def _to_iso(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_iso(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        moment = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        return None
    return moment.astimezone(timezone.utc)


async def _with_client(operation, *, failure_log: str, default):
    """단기 클라이언트로 1회 작업 실행 — 실패는 경고 로그 후 default 반환."""
    url = os.getenv(REDIS_URL_ENV)
    if not url:
        return default
    redis: Redis | None = None
    try:
        redis = _client(url)
        return await operation(redis)
    except Exception as exc:
        logger.warning("%s(%s)", failure_log, type(exc).__name__)
        return default
    finally:
        if redis is not None:
            with suppress(Exception):
                await redis.aclose()


async def init_session_meta(
    session_id: str, *, started_at: datetime, candidate_identity: str
) -> bool:
    """최초 메타 초기화 — startedAt·candidateIdentity·EXPIRE를 단일 트랜잭션으로.

    HSETNX라 재디스패치·중복 기록이 원래 값을 덮어쓰지 못한다. 두 필드를 함께
    기록해 한 필드만 남는 부분 상태를 구조적으로 제거한다(PRD §2).
    """

    async def op(redis: Redis) -> bool:
        async with redis.pipeline(transaction=True) as pipe:
            pipe.hsetnx(_meta_key(session_id), _META_STARTED_AT, _to_iso(started_at))
            pipe.hsetnx(
                _meta_key(session_id), _META_CANDIDATE_IDENTITY, candidate_identity
            )
            pipe.expire(_meta_key(session_id), REDIS_TRANSCRIPT_TTL_SECONDS)
            await pipe.execute()
        return True

    return await _with_client(
        op, failure_log="세션 메타 초기화 실패 — 면접은 계속(복원 재료 없이)", default=False
    )


async def record_reconnect_deadline(session_id: str, deadline: datetime) -> bool:
    """이탈 관측 시 재연결 deadline 기록 — 이탈마다 갱신(HSET), 창은 절대 시각이다."""

    async def op(redis: Redis) -> bool:
        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                _meta_key(session_id), _META_RECONNECT_DEADLINE, _to_iso(deadline)
            )
            pipe.expire(_meta_key(session_id), REDIS_TRANSCRIPT_TTL_SECONDS)
            await pipe.execute()
        return True

    return await _with_client(
        op, failure_log="재연결 deadline 기록 실패 — 창 타이머로 계속", default=False
    )


async def clear_reconnect_deadline(session_id: str) -> bool:
    """재입장 시 deadline 삭제 — 창이 닫혔다."""

    async def op(redis: Redis) -> bool:
        await redis.hdel(_meta_key(session_id), _META_RECONNECT_DEADLINE)
        return True

    return await _with_client(
        op, failure_log="재연결 deadline 삭제 실패 — 복원 시 stale 값 감수", default=False
    )


async def claim_owner(session_id: str, job_id: str) -> bool:
    """잡 시작 시 소유권 기록 — last-wins. 종결 단계 가드의 재료(관측·완화 계층)."""

    async def op(redis: Redis) -> bool:
        await redis.set(
            _owner_key(session_id), job_id, ex=REDIS_TRANSCRIPT_TTL_SECONDS
        )
        return True

    return await _with_client(
        op, failure_log="owner 기록 실패 — 가드 없이 계속", default=False
    )


async def owner_allows(session_id: str, job_id: str) -> bool:
    """종결 단계 직전 소유권 확인 — 다른 잡의 식별자가 관측될 때만 False.

    부재·조회 실패는 통과다(인수자는 반드시 자기 식별자를 기록하므로 부재 =
    인수 관측 없음). 원자성 없는 완화 계층 — 안전 보장은 Spring dispatch
    단일성 계약이다(PRD §2).
    """

    async def op(redis: Redis) -> bool:
        current = await redis.get(_owner_key(session_id))
        if current is not None and current != job_id:
            logger.warning("owner 불일치 관측 — 종결 단계 생략 (완화 계층)")
            return False
        return True

    return await _with_client(op, failure_log="owner 조회 실패 — 통과 처리", default=True)


async def release_owner(session_id: str, job_id: str) -> None:
    """잡 종료 직전 자기 소유면 DEL — best-effort, 실패 시 TTL 소멸."""

    async def op(redis: Redis) -> None:
        current = await redis.get(_owner_key(session_id))
        if current == job_id:
            await redis.delete(_owner_key(session_id))

    await _with_client(op, failure_log="owner 정리 실패 — TTL로 만료", default=None)


async def purge_session_state(session_id: str) -> bool:
    """flush 성공 후 정리 — transcript 사본 + 세션 메타 DEL (owner는 비대상).

    interview-end.md §4의 purge 집합을 recovery PRD가 확장한 형태다 —
    candidateIdentity 등 개인 식별 재료의 보존을 TTL 만료보다 앞당겨 끝낸다.
    owner는 purge 뒤에 오는 룸 삭제까지 가드로 유효해야 하므로 지우지 않는다.
    """

    async def op(redis: Redis) -> bool:
        await redis.delete(_transcript_key(session_id), _meta_key(session_id))
        return True

    return await _with_client(
        op, failure_log="Redis 세션 상태 정리 실패 — TTL로 만료", default=False
    )


@dataclass(frozen=True)
class RestoreState:
    """잡 시작 시 1회 조회한 복원 재료 스냅샷 — PRD §2 판별표의 입력."""

    terminated: bool = False
    started_at: datetime | None = None
    started_at_malformed: bool = False  # 값은 있으나 파싱 불가 — "유실"로 취급(폴백 층위)
    candidate_identity: str | None = None
    reconnect_deadline: datetime | None = None
    utterances: tuple[dict, ...] = field(default_factory=tuple)
    dropped: int = 0  # transcript 사본에서 JSON 파싱조차 안 된 항목 수

    @property
    def restorable(self) -> bool:
        """복원 대상 여부 — 시작 시각 또는 대화 사본이 남아 있으면 복원한다."""
        return (
            self.started_at is not None
            or self.started_at_malformed
            or bool(self.utterances)
        )


async def read_restore_state(session_id: str) -> RestoreState:
    """표식·메타·transcript를 한 연결로 조회한다 — 실패는 '상태 없음'으로 수렴.

    조회 실패 시 신규 세션과 구분할 수 없으므로 새 면접 폴백으로 이어진다
    (PRD §2 — Redis 장애 클래스의 수용 리스크).
    """

    async def op(redis: Redis) -> RestoreState:
        async with redis.pipeline(transaction=False) as pipe:
            pipe.exists(_termination_key(session_id))
            pipe.hgetall(_meta_key(session_id))
            pipe.lrange(_transcript_key(session_id), 0, -1)
            terminated, meta, raw_items = await pipe.execute()

        started_raw = meta.get(_META_STARTED_AT)
        started_at = _parse_iso(started_raw)
        utterances: list[dict] = []
        dropped = 0
        for raw in raw_items:
            try:
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError("객체가 아님")
                utterances.append(data)
            except (json.JSONDecodeError, ValueError):
                dropped += 1
        if dropped:
            logger.warning("transcript 사본 JSON 파싱 불가 %d건 드롭 — 재구성 계속", dropped)
        return RestoreState(
            terminated=bool(terminated),
            started_at=started_at,
            started_at_malformed=bool(started_raw) and started_at is None,
            candidate_identity=meta.get(_META_CANDIDATE_IDENTITY) or None,
            reconnect_deadline=_parse_iso(meta.get(_META_RECONNECT_DEADLINE)),
            utterances=tuple(utterances),
            dropped=dropped,
        )

    return await _with_client(
        op,
        failure_log="복원 상태 조회 실패 — 상태 없음으로 진행(새 면접 폴백)",
        default=RestoreState(),
    )
