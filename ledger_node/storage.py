"""Where a Raft node keeps its persistent state: current term, vote and log.

MemoryStorage keeps everything in memory (used by the fast unit tests).
PostgresStorage writes every change to the node's own PostgreSQL database before
returning, and keeps a copy in memory so that reads stay fast.

Every entry is linked to the previous one by a SHA-256 hash (a hash chain), so two
nodes with the same last hash are guaranteed to hold exactly the same log.
"""
import hashlib
import json
import threading
from dataclasses import dataclass, replace

import psycopg

GENESIS_HASH = "0" * 64


@dataclass(frozen=True)
class Entry:
    index: int
    term: int
    command: str          # JSON text
    prev_hash: str = ""
    entry_hash: str = ""


class HashMismatch(Exception):
    """An entry received from the leader does not match the hash this node computes."""


def canonical(command: str) -> str:
    """The same command always gives the same text, however its JSON was formatted."""
    return json.dumps(json.loads(command), sort_keys=True, separators=(",", ":"))


def chain_hash(prev_hash: str, index: int, term: int, command: str) -> str:
    data = f"{prev_hash}|{index}|{term}|{canonical(command)}"
    return hashlib.sha256(data.encode()).hexdigest()


class MemoryStorage:
    def __init__(self):
        self.term = 0
        self.voted_for = None
        self.log: list[Entry] = []

    def load_term_vote(self) -> tuple[int, str | None]:
        return self.term, self.voted_for

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        self.term, self.voted_for = term, voted_for

    def last_index(self) -> int:
        return len(self.log)

    def last_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def last_hash(self) -> str:
        return self.log[-1].entry_hash if self.log else GENESIS_HASH

    def term_at(self, index: int) -> int:
        return self.log[index - 1].term if 1 <= index <= len(self.log) else 0

    def entry(self, index: int) -> Entry | None:
        return self.log[index - 1] if 1 <= index <= len(self.log) else None

    def entries_from(self, index: int, limit: int = 100) -> list[Entry]:
        return self.log[index - 1:index - 1 + limit] if index >= 1 else []

    def _link(self, entries: list[Entry]) -> list[Entry]:
        """Compute the hash of each new entry and check it against the leader's hash, if one was sent."""
        prev, linked = self.last_hash(), []
        for e in entries:
            h = chain_hash(prev, e.index, e.term, e.command)
            if e.entry_hash and e.entry_hash != h:
                raise HashMismatch(f"entry {e.index}: leader hash {e.entry_hash[:12]} != local {h[:12]}")
            linked.append(replace(e, prev_hash=prev, entry_hash=h))
            prev = h
        return linked

    def append(self, entries: list[Entry]) -> None:
        self.log.extend(self._link(entries))

    def truncate_from(self, index: int) -> None:
        del self.log[index - 1:]


class PostgresStorage(MemoryStorage):
    """Same behaviour as MemoryStorage, but every change is committed to PostgreSQL first."""

    def __init__(self, dsn: str):
        super().__init__()
        self.conn = psycopg.connect(dsn, autocommit=True)
        self.db_lock = threading.Lock()
        with self.db_lock:
            self.conn.execute("INSERT INTO raft_metadata (id, current_term, voted_for, commit_index, last_applied) "
                              "VALUES (1, 0, NULL, 0, 0) ON CONFLICT (id) DO NOTHING")
            self.term, self.voted_for = self.conn.execute(
                "SELECT current_term, voted_for FROM raft_metadata WHERE id = 1").fetchone()
            rows = self.conn.execute("SELECT log_index, term, command::text, prev_hash, entry_hash "
                                     "FROM raft_log ORDER BY log_index").fetchall()
        self.log = [Entry(*row) for row in rows]
        self._check_chain()

    def _check_chain(self) -> None:
        """On start, recompute every hash: a damaged or edited log is detected immediately."""
        prev = GENESIS_HASH
        for e in self.log:
            if e.prev_hash != prev or chain_hash(prev, e.index, e.term, e.command) != e.entry_hash:
                raise HashMismatch(f"stored log is corrupted at entry {e.index}")
            prev = e.entry_hash

    def save_term_vote(self, term: int, voted_for: str | None) -> None:
        with self.db_lock:
            self.conn.execute("UPDATE raft_metadata SET current_term = %s, voted_for = %s WHERE id = 1",
                              (term, voted_for))
        super().save_term_vote(term, voted_for)

    def append(self, entries: list[Entry]) -> None:
        linked = self._link(entries)
        with self.db_lock, self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.executemany("INSERT INTO raft_log (log_index, term, command, prev_hash, entry_hash) "
                                "VALUES (%s, %s, %s::jsonb, %s, %s)",
                                [(e.index, e.term, e.command, e.prev_hash, e.entry_hash) for e in linked])
        self.log.extend(linked)

    def truncate_from(self, index: int) -> None:
        with self.db_lock:
            self.conn.execute("DELETE FROM raft_log WHERE log_index >= %s", (index,))
        super().truncate_from(index)