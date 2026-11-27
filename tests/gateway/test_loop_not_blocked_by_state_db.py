"""A slow ``state.db`` (a saturated disk, a cold cache) must never stall the gateway loop.

Seen on a live host: the loop's own thread waited for the routing lock, and for a boot-time sweep of
``session_admissions``, until the loop watchdog killed the process. Each test makes one such write or
read slow and proves that the loop keeps answering a liveness probe meanwhile.
"""
import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.session import SessionSource, SessionStore
from gateway import session_api_turn, session_authorities  # noqa: F401 -- imported cold here, not on the probed loop

SLOW_S = 1.0
MAX_GAP_S = 0.25  # the worst silence a 20 ms probe may show while the slow work runs


@pytest.fixture()
def store(tmp_path, monkeypatch):
    import hermes_state
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", tmp_path / "state.db")
    s = SessionStore(sessions_dir=tmp_path, config=GatewayConfig())
    s._ensure_loaded()
    s.get_or_create_session(SessionSource(platform=Platform.TELEGRAM, chat_id="1", chat_type="dm", user_id="u"))
    return s


async def _worst_gap(work):
    """Run ``work`` (a coroutine) beside a 20 ms probe; return the longest silence the probe saw."""
    gaps, stop = [], asyncio.Event()

    async def probe():
        last = time.monotonic()
        while not stop.is_set():
            await asyncio.sleep(0.02)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    task = asyncio.create_task(probe())
    await asyncio.sleep(0.05)
    try:
        await work
    finally:
        stop.set()
        await task
    return max(gaps)


def test_ensure_loaded_does_not_wait_for_the_routing_lock(store):
    store._lock.acquire()  # a writer holding the lock across its fsync
    try:
        done = threading.Event()
        threading.Thread(target=lambda: (store._ensure_loaded(), done.set()), daemon=True).start()
        assert done.wait(2), "_ensure_loaded of a loaded store waited for _lock"
    finally:
        store._lock.release()


def test_ensure_loaded_still_loads_an_unloaded_store(tmp_path, monkeypatch):
    import hermes_state
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", tmp_path / "state.db")
    s = SessionStore(sessions_dir=tmp_path, config=GatewayConfig())
    assert not s._loaded
    s._ensure_loaded()
    assert s._loaded


def test_update_entry_writes_after_releasing_the_routing_lock(store, monkeypatch):
    key = next(iter(store._entries))
    seen = {}
    real = store._persist_routing_data

    def slow_persist(data, generation):
        seen["lock_free"] = store._lock.acquire(blocking=False)  # a loop-side reader could take it
        if seen["lock_free"]:
            store._lock.release()
        time.sleep(0.1)  # the commit
        return real(data, generation)

    monkeypatch.setattr(store, "_persist_routing_data", slow_persist)
    assert store.suspend_session(key) is True
    assert seen["lock_free"], "the routing write ran while _lock was held"
    assert store._entries[key].suspended is True


def test_update_entry_is_durable_after_the_unlocked_write(store, tmp_path):
    key = next(iter(store._entries))
    assert store.suspend_session(key) is True
    reloaded = SessionStore(sessions_dir=tmp_path, config=GatewayConfig())
    reloaded._ensure_loaded()
    assert reloaded._entries[key].suspended is True


def _slow_store(monkeypatch, sweeps):
    """An authority whose state.db is slow: the first sweep statement blocks for SLOW_S."""
    import contextlib

    class Conn:
        def execute(self, *_a):
            return []

    def slow_collect(db):
        sweeps.append(threading.current_thread() is not threading.main_thread())
        time.sleep(SLOW_S)  # a cold state.db on a saturated disk

    monkeypatch.setattr("gateway.session_ingress_media.collect_unheld_api_images", slow_collect)
    monkeypatch.setattr("gateway.session_api_media.compact_settled_api_payloads", lambda db: None)
    monkeypatch.setattr("gateway.session_authorities.owner_scope", lambda a: contextlib.nullcontext())
    db = SimpleNamespace(_read_ctx=lambda: contextlib.nullcontext(Conn()))
    authority = SimpleNamespace(profile_id="p", db=db)
    return SimpleNamespace(gateway_runner=SimpleNamespace(session_authority=authority))


def test_boot_recovery_runs_the_slow_sweep_off_the_loop(monkeypatch):
    sweeps = []
    adapter = _slow_store(monkeypatch, sweeps)
    started = time.monotonic()
    gap = asyncio.run(_worst_gap(session_api_turn.recover_api_turns_off_loop(adapter)))
    assert time.monotonic() - started >= SLOW_S, "the sweep did not run"
    assert sweeps == [True], "the sweep ran on the loop's thread"
    assert gap < MAX_GAP_S, f"the loop went silent for {gap:.2f}s during the sweep"


def test_publishing_the_api_adapter_does_not_run_the_sweep_inline(monkeypatch):
    from gateway.run import GatewayRunner

    sweeps = []
    adapter = _slow_store(monkeypatch, sweeps)
    host = object.__new__(GatewayRunner)
    host.adapters = {}
    host._sync_voice_mode_state_to_adapter = host._bind_voice_input_callback = lambda a: None

    async def publish():
        began = time.monotonic()
        host._publish_primary_adapter(Platform.API_SERVER, adapter)
        assert time.monotonic() - began < 0.2, "publication waited for the store sweep"
        assert host.adapters[Platform.API_SERVER] is adapter
        await asyncio.gather(*host._background_tasks)  # the recovery still finishes

    gap = asyncio.run(_worst_gap(publish()))
    assert sweeps == [True]
    assert gap < MAX_GAP_S, f"the loop went silent for {gap:.2f}s while the adapter published"
