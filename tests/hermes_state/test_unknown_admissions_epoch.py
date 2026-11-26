"""Read-only unknown-admission probes must not scan the admission history.

Plain ``unittest`` so it also runs under the system interpreter (older SQLite) the
read-only monitors use, not only under pytest.
"""

import os
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from hermes_state import SessionDB

INDEX = "session_admissions_unknown_epoch"
MONITOR = "SELECT admission_id FROM session_admissions WHERE status='unknown' AND owner_epoch=?"
STALE = """SELECT seq, admission_id, request_id, principal_id, target_session_id, payload_digest,
                  intent, status, outcome, owner_epoch, generation, length(payload_json) AS payload_bytes
           FROM session_admissions WHERE status='unknown' AND owner_epoch IS NOT NULL
             AND owner_epoch < ? ORDER BY seq"""
COUNT_CURRENT = "SELECT count(*) FROM session_admissions WHERE status='unknown' AND owner_epoch=?"
COUNT_NULL = "SELECT count(*) FROM session_admissions WHERE status='unknown' AND owner_epoch IS NULL"
EPOCH = 7
VM_BUDGET = 1_000
FINISHED = 100_000
QUERIES = [(MONITOR, (EPOCH,)), (STALE, (EPOCH,)), (COUNT_CURRENT, (EPOCH,)), (COUNT_NULL, ())]


def create_store(path: Path) -> None:
    db = SessionDB(db_path=path)
    try:
        db.create_session("fixture", source="test")
    finally:
        db.close()


def reopen(path: Path) -> None:
    """The normal writable open: SCHEMA_SQL + reconcile, as the gateway runs it."""
    SessionDB(db_path=path).close()


def seed(path: Path, *, drop_index: bool) -> None:
    with closing(sqlite3.connect(path)) as conn:
        if drop_index:  # simulate a store created by a release without the index
            conn.execute(f"DROP INDEX IF EXISTS {INDEX}")
        insert = """INSERT INTO session_admissions (
                admission_id, request_id, principal_id, target_session_id,
                lineage_json, payload_json, payload_digest, intent, status, outcome, owner_epoch
            ) VALUES (?, ?, 'fixture', 'fixture', '[]', '{}', 'fixture', 'queue', ?, ?, ?)"""
        conn.executemany(insert, ((f"finished-{i}", f"finished-{i}", "terminal", "finished", EPOCH)
                                  for i in range(FINISHED)))
        # seq order deliberately differs from epoch order so ORDER BY seq is really exercised.
        rows = [("current", "unknown", EPOCH), ("old", "unknown", EPOCH - 1),
                ("pending", "queued", EPOCH), ("earlier", "unknown", EPOCH - 2),
                ("missing-1", "unknown", None), ("missing-2", "unknown", None),
                ("future", "unknown", EPOCH + 1), ("missing-pending", "queued", None),
                ("stale-pending", "queued", EPOCH - 1)]
        conn.executemany(insert, ((key, key, status, None, epoch) for key, status, epoch in rows))
        conn.commit()


def probe(path: Path, sql: str, params: tuple, *, budget: int | None = None):
    """Run one query the way the monitors do: a fresh mode=ro connection."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params)]
        if budget is not None:
            conn.set_progress_handler(lambda: 1, budget - 1)  # interrupt past the budget
        return plan, conn.execute(sql, params).fetchall()


def index_count(path: Path) -> int:
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        return conn.execute("SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?",
                            (INDEX,)).fetchone()[0]


EXPECTED = [
    [("current",)],
    ["old", "earlier"],  # seq order, checked on admission_id; full rows compared to baseline
    [(1,)],
    [(2,)],
]


class TestUnknownAdmissionsEpoch(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self._old_home = os.environ.get("HERMES_HOME")
        os.environ["HERMES_HOME"] = str(self.root / "home")
        self.path = self.root / "state.db"

    def tearDown(self):
        if self._old_home is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = self._old_home
        self._tmp.cleanup()

    def assert_results(self, results):
        self.assertEqual(results[0], EXPECTED[0])
        self.assertEqual([row[1] for row in results[1]], EXPECTED[1])
        self.assertEqual(results[2:], EXPECTED[2:])

    def assert_indexed_within_budget(self, baseline):
        for (sql, params), expected in zip(QUERIES, baseline):
            with self.subTest(sql=sql.split()[1]):
                plan, rows = probe(self.path, sql, params, budget=VM_BUDGET)
                self.assertEqual(rows, expected)  # full projections, same order
                self.assertTrue(any("SEARCH" in p and INDEX in p for p in plan), plan)

    def test_fresh_store_is_born_with_the_index(self):
        create_store(self.path)
        self.assertEqual(index_count(self.path), 1)
        seed(self.path, drop_index=False)
        results = [probe(self.path, sql, params)[1] for sql, params in QUERIES]
        self.assert_results(results)
        self.assert_indexed_within_budget(results)

    def test_existing_store_gains_the_index_on_writable_open_with_identical_results(self):
        create_store(self.path)
        seed(self.path, drop_index=True)
        self.assertEqual(index_count(self.path), 0)

        baseline = []
        for sql, params in QUERIES:
            plan, rows = probe(self.path, sql, params)
            self.assertTrue(any("SCAN session_admissions" in p for p in plan), plan)
            with self.assertRaisesRegex(sqlite3.OperationalError, "interrupted"):
                probe(self.path, sql, params, budget=VM_BUDGET)
            baseline.append(rows)
        self.assert_results(baseline)

        for _ in range(2):  # upgrade, then idempotent reopen
            reopen(self.path)
            self.assertEqual(index_count(self.path), 1)
            self.assert_indexed_within_budget(baseline)


if __name__ == "__main__":
    unittest.main()
