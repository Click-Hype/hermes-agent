"""The read-only unknown-admission probe must not scan admission history."""

import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from persistence.sqlite._holder import StoreHolder
from persistence.sqlite.conversation._store.sessions import create_session

INDEX = "session_admissions_unknown_epoch"
QUERY = "SELECT admission_id FROM session_admissions WHERE status='unknown' AND owner_epoch=?"
STALE = """SELECT seq, admission_id, request_id, principal_id, target_session_id, payload_digest,
                  intent, status, outcome, owner_epoch, generation, length(payload_json) AS payload_bytes
           FROM session_admissions WHERE status='unknown' AND owner_epoch IS NOT NULL
             AND owner_epoch < ? ORDER BY seq"""
COUNT_CURRENT = "SELECT count(*) FROM session_admissions WHERE status='unknown' AND owner_epoch=?"
COUNT_NULL = "SELECT count(*) FROM session_admissions WHERE status='unknown' AND owner_epoch IS NULL"
EPOCH = 7
VM_BUDGET = 1_000


def seed_admissions(holder, *, release_cases=False):
    create_session(holder, "fixture", source="cli")
    conn = holder._conn
    assert conn is not None
    conn.execute("BEGIN")
    conn.executemany(
        """INSERT INTO session_admissions (
            admission_id, request_id, principal_id, target_session_id,
            lineage_json, payload_json, payload_digest, intent, status, outcome, owner_epoch
        ) VALUES (?, ?, 'fixture', 'fixture', '[]', '{}', 'fixture', 'queue', ?, ?, ?)""",
        (
            (f"finished-{i}", f"finished-{i}", "terminal", "finished", EPOCH)
            for i in range(100_000)
        ),
    )
    rows = [("current", "unknown", EPOCH), ("old", "unknown", EPOCH - 1), ("pending", "queued", EPOCH)]
    if release_cases:
        # Index order across epochs is deliberately different from seq order.
        rows += [("earlier", "unknown", EPOCH - 2), ("missing-1", "unknown", None),
                 ("missing-2", "unknown", None), ("future", "unknown", EPOCH + 1),
                 ("missing-pending", "queued", None), ("stale-pending", "queued", EPOCH - 1)]
    conn.executemany(
        """INSERT INTO session_admissions (
            admission_id, request_id, principal_id, target_session_id,
            lineage_json, payload_json, payload_digest, intent, status, owner_epoch
        ) VALUES (?, ?, 'fixture', 'fixture', '[]', '{}', 'fixture', 'queue', ?, ?)""",
        ((key, key, status, epoch) for key, status, epoch in rows),
    )
    conn.commit()


def probe(path, query=QUERY, params=(EPOCH,), *, budget=None):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + query, params)]
        if budget is not None:
            conn.set_progress_handler(lambda: 1, budget - 1)
        return plan, conn.execute(query, params).fetchall()


class TestUnknownAdmissionsEpoch(unittest.TestCase):
    def test_release_queries_preserve_eligibility_counts_and_seq_order(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "state.db"
            with StoreHolder(path) as holder:
                seed_admissions(holder, release_cases=True)
                conn = holder._conn
                assert conn is not None
                conn.execute(f"DROP INDEX IF EXISTS {INDEX}")
                conn.commit()

            cases = [(STALE, (EPOCH,)), (COUNT_CURRENT, (EPOCH,)), (COUNT_NULL, ())]
            baseline = [probe(path, sql, params)[1] for sql, params in cases]
            self.assertEqual([row[1] for row in baseline[0]], ["old", "earlier"])
            self.assertEqual(baseline[1:], [[(1,)], [(2,)]])
            with StoreHolder(path):
                pass
            for (sql, params), expected in zip(cases, baseline):
                with self.subTest(sql=sql):
                    plan, rows = probe(path, sql, params, budget=VM_BUDGET)
                    self.assertEqual(rows, expected)  # full projections, not just IDs
                    self.assertTrue(any("SEARCH" in p and INDEX in p for p in plan), plan)

    def assert_indexed_probe(self, path, expected):
        plan, rows = probe(path, budget=VM_BUDGET)
        self.assertTrue(
            any("SEARCH" in detail and INDEX in detail and "owner_epoch=?" in detail for detail in plan),
            plan,
        )
        self.assertEqual(rows, expected)

    def test_fresh_store_indexes_unknown_admissions_by_epoch(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "state.db"
            with StoreHolder(path) as holder:
                seed_admissions(holder)
            self.assert_indexed_probe(path, [("current",)])

    def test_existing_store_gets_index_without_changing_results(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "state.db"
            with StoreHolder(path) as holder:
                seed_admissions(holder)
                conn = holder._conn
                assert conn is not None
                conn.execute(f"DROP INDEX IF EXISTS {INDEX}")
                conn.commit()
            plan, expected = probe(path)
            self.assertTrue(any("SCAN session_admissions" in detail for detail in plan), plan)
            self.assertEqual(expected, [("current",)])
            with self.assertRaisesRegex(sqlite3.OperationalError, "interrupted"):
                probe(path, budget=VM_BUDGET)

            for _ in range(2):  # normal upgrade, then idempotent reopen
                with StoreHolder(path):
                    pass
                self.assert_indexed_probe(path, expected)
