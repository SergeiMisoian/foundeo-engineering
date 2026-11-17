import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from event_inbox import (
    MAX_ATTEMPTS,
    EventConflict,
    EventInbox,
    Outcome,
    temporary_failure,
)


class EventInboxTest(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.database = Path(directory) / "test.db"
        connection = self.enterContext(closing(sqlite3.connect(self.database)))
        self.inbox = EventInbox(connection)
        self.inbox.enqueue("one", {"title": "report", "format": "pdf"})

    def test_repeated_delivery_and_processing_return_one_result(self):
        self.assertFalse(self.inbox.enqueue("one", {"format": "pdf", "title": "report"}))
        completed = self.inbox.process("one", lambda data: data["title"])
        self.assertEqual(completed, Outcome("done", 1, "report"))

        with closing(sqlite3.connect(self.database)) as second_connection:
            after_reopen = EventInbox(second_connection).process("one", temporary_failure)
        self.assertEqual(after_reopen, completed)

    def test_conflicting_payload_cannot_replace_original(self):
        with self.assertRaises(EventConflict):
            self.inbox.enqueue("one", {"title": "different"})
        self.assertEqual(self.inbox.process("one", lambda data: data["title"]).result, "report")

    def test_transient_failure_is_retried(self):
        self.assertEqual(self.inbox.process("one", temporary_failure), Outcome("pending", 1, None))
        self.assertEqual(
            self.inbox.process("one", lambda data: data["title"]), Outcome("done", 2, "report")
        )

    def test_retry_budget_survives_reopen(self):
        for _ in range(MAX_ATTEMPTS):
            self.inbox.process("one", temporary_failure)
        with closing(sqlite3.connect(self.database)) as second_connection:
            outcome = EventInbox(second_connection).process("one", lambda _data: "unexpected")
        self.assertEqual(outcome, Outcome("failed", MAX_ATTEMPTS, None))

    @unittest.skipUnless(hasattr(sqlite3, "LEGACY_TRANSACTION_CONTROL"), "Python 3.12+")
    def test_autocommit_connection_persists_result_and_releases_lock(self):
        with closing(sqlite3.connect(self.database, autocommit=True)) as connection:
            inbox = EventInbox(connection)
            completed = inbox.process("one", lambda data: data["title"])
            self.assertFalse(connection.in_transaction)
            self.assertEqual(inbox.process("one", temporary_failure), completed)
        self.assertEqual(self.inbox.process("one", temporary_failure), completed)

    def test_unexpected_error_rolls_back(self):
        def crash(_data):
            raise RuntimeError("unexpected error")

        with self.assertRaises(RuntimeError):
            self.inbox.process("one", crash)
        self.assertEqual(
            self.inbox.process("one", lambda data: data["title"]), Outcome("done", 1, "report")
        )


if __name__ == "__main__":
    unittest.main()
