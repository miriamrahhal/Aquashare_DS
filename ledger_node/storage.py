"""Where a Raft node keeps its persistent state: current term, vote and log.

MemoryStorage is used while the algorithm is developed and tested; a PostgreSQL
implementation with the same methods replaces it once persistence is added.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Entry:
    index: int
    term: int
    command: str          # JSON text
    prev_hash: str = ""
    entry_hash: str = ""


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

    def term_at(self, index: int) -> int:
        return self.log[index - 1].term if 1 <= index <= len(self.log) else 0

    def entry(self, index: int) -> Entry | None:
        return self.log[index - 1] if 1 <= index <= len(self.log) else None

    def entries_from(self, index: int, limit: int = 100) -> list[Entry]:
        return self.log[index - 1:index - 1 + limit] if index >= 1 else []

    def append(self, entries: list[Entry]) -> None:
        self.log.extend(entries)

    def truncate_from(self, index: int) -> None:
        del self.log[index - 1:]