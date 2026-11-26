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
EPOCH = 7
VM_BUDGET = 1_000


def seed_admissions(holder):
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
    conn.executemany(
        """INSERT INTO session_admissions (
            admission_id, request_id, principal_id, target_session_id,
            lineage_json, payload_json, payload_digest, intent, status, owner_epoch
        ) VALUES (?, ?, 'fixture', 'fixture', '[]', '{}', 'fixture', 'queue', ?, ?)""",
        [
            ("current", "current", "unknown", EPOCH),
            ("old", "old", "unknown", EPOCH - 1),
            ("pending", "pending", "queued", EPOCH),
        ],
    )
    conn.commit()


def probe(path, *, budget=None):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + QUERY, (EPOCH,))]
        if budget is not None:
            conn.set_progress_handler(lambda: 1, budget - 1)
        return plan, conn.execute(QUERY, (EPOCH,)).fetchall()


class TestUnknownAdmissionsEpoch(unittest.TestCase):
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
