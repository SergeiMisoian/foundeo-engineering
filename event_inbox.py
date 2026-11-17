"""A durable inbox for a deterministic document-processing example."""

import json
import sqlite3
import tempfile
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path

MAX_ATTEMPTS = 3

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    result TEXT
)
"""


class EventConflict(ValueError):
    """An existing event ID was reused for a different payload."""


class RetryableError(Exception):
    """A temporary processing failure."""


@dataclass(frozen=True)
class Outcome:
    state: str
    attempts: int
    result: str | None


class EventInbox:
    def __init__(self, connection: sqlite3.Connection):
        if connection.in_transaction:
            raise ValueError("EventInbox requires a connection without an open transaction")
        self.connection = connection
        self.connection.execute(SCHEMA)

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def enqueue(self, event_id: str, payload: dict[str, str]) -> bool:
        encoded_payload = json.dumps(payload, sort_keys=True)
        with self._transaction():
            inserted = self.connection.execute(
                """
                INSERT INTO events (event_id, payload) VALUES (?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (event_id, encoded_payload),
            )
            if inserted.rowcount == 1:
                return True

            (stored_payload,) = self.connection.execute(
                "SELECT payload FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            if stored_payload != encoded_payload:
                raise EventConflict(f"Event ID {event_id!r} has a different payload")
            return False

    def process(
        self, event_id: str, handler: Callable[[dict[str, str]], str]
    ) -> Outcome:
        with self._transaction():
            row = self.connection.execute(
                """
                SELECT payload, state, attempts, result
                FROM events WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()
            if row is None:
                raise KeyError(event_id)
            payload, state, attempts, result = row
            if state != "pending":
                return Outcome(state, attempts, result)

            attempts += 1
            try:
                result = handler(json.loads(payload))
                state = "done"
            except RetryableError:
                state = "failed" if attempts == MAX_ATTEMPTS else "pending"
                result = None

            self.connection.execute(
                "UPDATE events SET state = ?, attempts = ?, result = ? WHERE event_id = ?",
                (state, attempts, result, event_id),
            )
            return Outcome(state, attempts, result)


def temporary_failure(_payload: dict[str, str]) -> str:
    raise RetryableError("Simulated provider timeout")


def demo() -> None:
    with tempfile.TemporaryDirectory() as directory:
        with closing(sqlite3.connect(Path(directory) / "events.db")) as connection:
            inbox = EventInbox(connection)
            event_id = "document-001"
            payload = {"title": " Quarterly report "}
            print("First delivery accepted:", inbox.enqueue(event_id, payload))
            print("Duplicate accepted:", inbox.enqueue(event_id, payload))
            print("First attempt:", inbox.process(event_id, temporary_failure))
            completed = inbox.process(event_id, lambda data: data["title"].strip())
            print("Retry:", completed)


if __name__ == "__main__":
    demo()
