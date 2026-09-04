"""종료 시퀀스 단위 테스트 — 순서·실패 격리·유한 시간. docs/prd/interview-end.md §3."""

import asyncio

from src.interview.end_sequence import EndSequence
from src.interview.end_state import EndCause


class _Recorder:
    def __init__(self):
        self.events: list[str] = []
        self.shutdowns: list[str] = []

    def step(self, name, *, result=None, error=False, hang=False):
        async def fn():
            if hang:
                await asyncio.sleep(3600)
            self.events.append(name)
            if error:
                raise RuntimeError(name)
            return result

        return fn

    def writer(self, *, error=False):
        rec = self

        class _Writer:
            async def aclose(self):
                rec.events.append("writer")
                if error:
                    raise RuntimeError("writer boom")

        return _Writer()


def _sequence(rec: _Recorder, **kwargs) -> EndSequence:
    return EndSequence(shutdown_fn=lambda reason: rec.shutdowns.append(reason), **kwargs)


def test_full_sequence_runs_in_order_and_shuts_down():
    rec = _Recorder()
    seq = _sequence(
        rec,
        writer=rec.writer(),
        flush_fn=rec.step("flush", result=True),
        purge_fn=rec.step("purge"),
        publish_fn=rec.step("publish"),
        delete_room_fn=rec.step("delete"),
    )
    asyncio.run(seq.run(EndCause.FINAL_QUESTION))
    assert rec.events == ["writer", "flush", "purge", "publish", "delete"]
    assert rec.shutdowns == ["interview end: FINAL_QUESTION"]


def test_flush_failure_skips_purge_and_publish_but_still_exits():
    rec = _Recorder()
    seq = _sequence(
        rec,
        flush_fn=rec.step("flush", error=True),
        purge_fn=rec.step("purge"),
        publish_fn=rec.step("publish"),
        delete_room_fn=rec.step("delete"),
    )
    asyncio.run(seq.run(EndCause.USER_REQUEST))
    assert rec.events == ["flush", "delete"]  # flush 실패 → 정리·발행 생략, 퇴장은 계속
    assert rec.shutdowns == ["interview end: USER_REQUEST"]


def test_missing_flush_impl_skips_dependent_steps():
    rec = _Recorder()
    seq = _sequence(
        rec,
        purge_fn=rec.step("purge"),
        publish_fn=rec.step("publish"),
        delete_room_fn=rec.step("delete"),
    )
    asyncio.run(seq.run(EndCause.LLM_END))
    assert rec.events == ["delete"]  # flush 미구현(HBB1-287) — 생략 경고 후 계속
    assert rec.shutdowns == ["interview end: LLM_END"]


def test_room_delete_retries_bounded_then_exits():
    rec = _Recorder()
    seq = _sequence(
        rec,
        flush_fn=rec.step("flush", result=True),
        delete_room_fn=rec.step("delete", error=True),
        room_delete_max_attempts=2,
        room_delete_backoff_seconds=0.001,  # 재시도 간 backoff — 테스트는 짧게
    )
    asyncio.run(seq.run(EndCause.HARD_TIMEOUT))
    assert rec.events == ["flush", "delete", "delete"]  # bounded retry 후 소진
    assert rec.shutdowns == ["interview end: HARD_TIMEOUT"]  # 소진에도 퇴장 보장


def test_hanging_writer_close_is_bounded_by_timeout():
    rec = _Recorder()

    class _HangingWriter:
        async def aclose(self):
            await asyncio.sleep(3600)

    seq = _sequence(
        rec,
        writer=_HangingWriter(),
        flush_fn=rec.step("flush", result=True),
        delete_room_fn=rec.step("delete"),
        step_timeout_seconds=0.01,
    )
    # 외부 watchdog — 내부 타임아웃이 회귀하면 1시간 대기가 아니라 즉시 실패한다
    asyncio.run(asyncio.wait_for(seq.run(EndCause.USER_REQUEST), timeout=0.5))
    assert rec.events == ["flush", "delete"]  # writer hang이 시퀀스를 막지 않는다
    assert rec.shutdowns == ["interview end: USER_REQUEST"]


def test_hanging_step_is_bounded_by_timeout():
    rec = _Recorder()
    seq = _sequence(
        rec,
        flush_fn=rec.step("flush", hang=True),
        delete_room_fn=rec.step("delete"),
        step_timeout_seconds=0.01,
    )
    # 외부 watchdog — 내부 타임아웃이 회귀하면 1시간 대기가 아니라 즉시 실패한다
    asyncio.run(asyncio.wait_for(seq.run(EndCause.USER_REQUEST), timeout=0.5))
    assert rec.events == ["delete"]  # hang한 flush는 타임아웃으로 실패 처리
    assert rec.shutdowns == ["interview end: USER_REQUEST"]


def test_writer_close_failure_does_not_stop_sequence():
    rec = _Recorder()
    seq = _sequence(
        rec,
        writer=rec.writer(error=True),
        flush_fn=rec.step("flush", result=True),
        purge_fn=rec.step("purge"),
        publish_fn=rec.step("publish"),
        delete_room_fn=rec.step("delete"),
    )
    asyncio.run(seq.run(EndCause.FINAL_QUESTION))
    assert rec.events == ["writer", "flush", "purge", "publish", "delete"]


def test_local_run_without_room_delete_still_exits():
    rec = _Recorder()
    seq = _sequence(rec)
    asyncio.run(seq.run(EndCause.USER_REQUEST))
    assert rec.events == []  # 로컬·콘솔 — 정리 대상 없이 퇴장만
    assert rec.shutdowns == ["interview end: USER_REQUEST"]


# --- 재연결·복원 (docs/prd/interview-recovery.md) ---

def test_reconnect_timeout_skips_flush_and_purge_but_deletes_room():
    rec = _Recorder()
    seq = _sequence(
        rec,
        writer=rec.writer(),
        flush_fn=rec.step("flush", result=True),
        purge_fn=rec.step("purge"),
        publish_fn=rec.step("publish"),
        delete_room_fn=rec.step("delete"),
    )
    asyncio.run(seq.run(EndCause.RECONNECT_TIMEOUT))
    # 창 소진 = 면접 미완주 — flush 생략이 곧 ABORTED 신호(행 없음), 사본은 TTL 소멸
    assert rec.events == ["writer", "delete"]
    assert rec.shutdowns == ["interview end: RECONNECT_TIMEOUT"]


def test_recovered_closing_runs_full_flush_sequence():
    rec = _Recorder()
    seq = _sequence(
        rec,
        writer=rec.writer(),
        flush_fn=rec.step("flush", result=True),
        purge_fn=rec.step("purge"),
        publish_fn=rec.step("publish"),
        delete_room_fn=rec.step("delete"),
    )
    asyncio.run(seq.run(EndCause.RECOVERED_CLOSING))
    # 복원된 closing — 미완이던 종료 시퀀스를 flush부터 재개해 ENDED로 수렴한다
    assert rec.events == ["writer", "flush", "purge", "publish", "delete"]


def test_owner_mismatch_skips_terminal_steps_but_still_exits():
    rec = _Recorder()

    async def guard():
        return False  # 다른 잡의 식별자 관측 — 후발 잡이 세션의 주인

    seq = _sequence(
        rec,
        writer=rec.writer(),
        flush_fn=rec.step("flush", result=True),
        purge_fn=rec.step("purge"),
        publish_fn=rec.step("publish"),
        delete_room_fn=rec.step("delete"),
        guard_fn=guard,
    )
    asyncio.run(seq.run(EndCause.FINAL_QUESTION))
    assert rec.events == ["writer"]  # flush·정리·룸 삭제 생략 — 잡만 종료
    assert rec.shutdowns == ["interview end: FINAL_QUESTION"]


def test_owner_check_failure_passes_through():
    rec = _Recorder()

    async def guard():
        raise RuntimeError("redis down")  # 조회 실패 — 통과(부재=통과 원칙)

    seq = _sequence(
        rec,
        flush_fn=rec.step("flush", result=True),
        purge_fn=rec.step("purge"),
        publish_fn=rec.step("publish"),
        delete_room_fn=rec.step("delete"),
        guard_fn=guard,
    )
    asyncio.run(seq.run(EndCause.FINAL_QUESTION))
    assert rec.events == ["flush", "purge", "publish", "delete"]
