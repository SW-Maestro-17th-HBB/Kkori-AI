"""재연결 모니터 단위 테스트 — 이탈·재입장·창 소진. docs/prd/interview-recovery.md §1."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.interview.end_state import EndCause
from src.interview.reconnect import PresenceMonitor

NOW = datetime(2026, 8, 6, 9, 0, 0, tzinfo=timezone.utc)
CANDIDATE = "user-42"


def _make(**kwargs):
    env = SimpleNamespace(
        closings=[],
        resumes=0,
        invalidations=0,
        deadlines=[],
        clears=0,
    )

    async def resume():
        env.resumes += 1

    async def record(deadline):
        env.deadlines.append(deadline)

    async def clear():
        env.clears += 1

    def invalidate():
        env.invalidations += 1

    env.monitor = PresenceMonitor(
        candidate_identity=CANDIDATE,
        window_seconds=kwargs.pop("window_seconds", 3600),
        begin_closing_fn=lambda cause: env.closings.append(cause) or True,
        resume_fn=resume,
        invalidate_fn=invalidate,
        record_deadline_fn=record,
        clear_deadline_fn=clear,
        wall_clock=kwargs.pop("wall_clock", lambda: NOW),
        **kwargs,
    )
    return env


async def _settle(env):
    await asyncio.sleep(0.01)
    await env.monitor.aclose()


def test_disconnect_starts_window_and_discards_inflight():
    env = _make()

    async def scenario():
        env.monitor.on_participant_disconnected(CANDIDATE)
        await _settle(env)

    asyncio.run(scenario())
    assert env.monitor.is_present is False
    assert env.invalidations == 1  # 진행 중 생성·발화 폐기 — 청자 없음
    assert env.deadlines == [NOW + timedelta(seconds=3600)]  # 절대 deadline 내구 기록
    assert env.monitor.last_disconnect_at == NOW.timestamp()  # 입력 경계 노출
    assert env.closings == []


def test_non_candidate_events_are_ignored():
    env = _make()

    async def scenario():
        env.monitor.on_participant_disconnected("observer-1")
        env.monitor.on_participant_connected("observer-1")
        await _settle(env)

    asyncio.run(scenario())
    assert env.monitor.is_present is True  # 위치·순서 기반 판정 없음 — identity 일치만
    assert env.invalidations == 0
    assert env.resumes == 0
    assert env.deadlines == []


def test_reentry_within_window_clears_deadline_and_resumes():
    env = _make()

    async def scenario():
        env.monitor.on_participant_disconnected(CANDIDATE)
        env.monitor.on_participant_connected(CANDIDATE)
        await _settle(env)

    asyncio.run(scenario())
    assert env.monitor.is_present is True
    assert env.clears == 1  # 창 닫힘 — deadline 삭제
    assert env.resumes == 1  # 재개 안내·앵커 트리거
    assert env.closings == []  # 타이머 취소 — 창 소진 없음


def test_window_expiry_converges_to_reconnect_timeout():
    env = _make(window_seconds=0)

    async def scenario():
        env.monitor.on_participant_disconnected(CANDIDATE)
        await _settle(env)

    asyncio.run(scenario())
    assert env.closings == [EndCause.RECONNECT_TIMEOUT]


def test_window_expiry_after_hard_deadline_converges_to_hard_timeout():
    env = _make(window_seconds=0, hard_exceeded_fn=lambda: True)

    async def scenario():
        env.monitor.on_participant_disconnected(CANDIDATE)
        await _settle(env)

    asyncio.run(scenario())
    # 시간 기준 이원화 — hard 선소진이면 정상 종료(flush) 원인으로 수렴
    assert env.closings == [EndCause.HARD_TIMEOUT]


def test_repeated_cycles_record_fresh_deadline_each_time():
    env = _make()

    async def scenario():
        env.monitor.on_participant_disconnected(CANDIDATE)
        env.monitor.on_participant_connected(CANDIDATE)
        env.monitor.on_participant_disconnected(CANDIDATE)
        env.monitor.on_participant_connected(CANDIDATE)
        await _settle(env)

    asyncio.run(scenario())
    assert len(env.deadlines) == 2  # 이탈마다 새 창 — deadline 갱신
    assert env.clears == 2
    assert env.resumes == 2
    assert env.closings == []


def test_deadline_ops_are_serialized_on_fast_reentry():
    """빠른 재입장 — 늦은 HSET이 HDEL을 되살리지 않도록 발생 순서대로 실행된다."""
    env = _make()
    order: list[str] = []

    async def slow_record(deadline):
        await asyncio.sleep(0.05)  # 기록이 느린 상황 — 직렬화 없으면 clear가 앞선다
        order.append("record")

    async def fast_clear():
        order.append("clear")

    env.monitor._record_deadline_fn = slow_record
    env.monitor._clear_deadline_fn = fast_clear

    async def scenario():
        env.monitor.on_participant_disconnected(CANDIDATE)
        env.monitor.on_participant_connected(CANDIDATE)
        await asyncio.sleep(0.2)
        await env.monitor.aclose()

    asyncio.run(scenario())
    assert order == ["record", "clear"]  # FIFO — 소진된 deadline이 되살아나지 않는다
