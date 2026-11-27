"""Compactions of one state.db run one at a time, so the short turn writes are not starved behind a queue
of large commits (each one an fsync-bound transaction on a saturated disk)."""
import threading
import time

from hermes_state import SessionDB


def _stub_db(path, running, peak, hold_s=0.05):
    """A SessionDB whose transcript write only measures how many compactions are inside it at once."""
    db = object.__new__(SessionDB)
    db.db_path = path

    def fake_write(fn, messages, **_kwargs):
        with running["lock"]:
            running["now"] += 1
            peak["max"] = max(peak["max"], running["now"])
        time.sleep(hold_s)
        with running["lock"]:
            running["now"] -= 1
        return len(messages)

    db._execute_transcript_write = fake_write
    return db


def _run_parallel(dbs, n_each):
    threads = [threading.Thread(target=db.archive_and_compact, args=("s", [{"role": "user", "content": "x"}]))
               for db in dbs for _ in range(n_each)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert not any(t.is_alive() for t in threads)


def test_compactions_of_one_state_db_never_overlap(tmp_path):
    running, peak = {"lock": threading.Lock(), "now": 0}, {"max": 0}
    path = tmp_path / "state.db"
    # Two handles on the same file share the gate: one profile, one compaction at a time.
    _run_parallel([_stub_db(path, running, peak), _stub_db(path, running, peak)], n_each=3)
    assert peak["max"] == 1


def test_compactions_of_different_state_dbs_still_run_in_parallel(tmp_path):
    running, peak = {"lock": threading.Lock(), "now": 0}, {"max": 0}
    dbs = [_stub_db(tmp_path / name / "state.db", running, peak, hold_s=0.3) for name in ("a", "b")]
    _run_parallel(dbs, n_each=1)
    assert peak["max"] == 2, "another profile's compaction waited for this one"


def test_a_failed_compaction_releases_the_gate(tmp_path):
    running, peak = {"lock": threading.Lock(), "now": 0}, {"max": 0}
    db = _stub_db(tmp_path / "state.db", running, peak)
    real = db._execute_transcript_write

    def boom(*_a, **_k):
        raise RuntimeError("lease lost")

    db._execute_transcript_write = boom
    try:
        db.archive_and_compact("s", [])
    except RuntimeError:
        pass
    db._execute_transcript_write = real
    assert db.archive_and_compact("s", [{"role": "user", "content": "x"}]) == 1
