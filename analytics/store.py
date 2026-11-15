"""The analytics service's own database (SQLite): running totals built from ledger events.

Each event is applied in one transaction together with its log index. An event whose
log index was already processed is ignored, so duplicates from the relay never change
the totals."""
import sqlite3
import threading
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS processed (log_index INTEGER PRIMARY KEY, type TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS days (day TEXT PRIMARY KEY, limit_l INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS farm_day (
    day TEXT NOT NULL, farm_id TEXT NOT NULL,
    allocated INTEGER NOT NULL DEFAULT 0, consumed INTEGER NOT NULL DEFAULT 0,
    sent INTEGER NOT NULL DEFAULT 0, received INTEGER NOT NULL DEFAULT 0,
    consumption_events INTEGER NOT NULL DEFAULT 0, transfers INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, farm_id));
CREATE TABLE IF NOT EXISTS stats (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
"""


class Store:
    def __init__(self, path: Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript(SCHEMA)
        self.lock = threading.Lock()

    def _add(self, day, farm_id, **amounts):
        self.db.execute("INSERT OR IGNORE INTO farm_day (day, farm_id) VALUES (?, ?)", (day, farm_id))
        for column, amount in amounts.items():
            self.db.execute(f"UPDATE farm_day SET {column} = {column} + ? WHERE day = ? AND farm_id = ?",
                            (amount, day, farm_id))

    def _count(self, name):
        self.db.execute("INSERT INTO stats (name, value) VALUES (?, 1) "
                        "ON CONFLICT(name) DO UPDATE SET value = value + 1", (name,))

    def apply(self, event: dict) -> bool:
        """Apply one event. Returns False if it was already processed (a duplicate)."""
        with self.lock, self.db:                                  # one transaction
            if self.db.execute("SELECT 1 FROM processed WHERE log_index = ?", (event["log_index"],)).fetchone():
                self._count("duplicates_ignored")
                return False
            self.db.execute("INSERT INTO processed (log_index, type) VALUES (?, ?)", (event["log_index"], event["type"]))
            kind = event["type"]
            if kind == "open_day":
                self.db.execute("INSERT OR REPLACE INTO days (day, limit_l) VALUES (?, ?)", (event["day"], event["limit_l"]))
                for farm_id, amount in event["allocations"].items():
                    self._add(event["day"], farm_id, allocated=amount)
            elif kind == "record_consumption":
                self._add(event["day"], event["farm_id"], consumed=event["amount_l"], consumption_events=event["events"])
            elif kind == "transfer":
                self._add(event["day"], event["sender"], sent=event["amount_l"], transfers=1)
                self._add(event["day"], event["recipient"], received=event["amount_l"], transfers=1)
            self._count("events_processed")
            return True

    def summary(self, day: str | None = None) -> dict:
        with self.lock:
            stats = dict(self.db.execute("SELECT name, value FROM stats").fetchall())
            last = self.db.execute("SELECT max(log_index) FROM processed").fetchone()[0]
            day = day or self.db.execute("SELECT max(day) FROM days").fetchone()[0]
            limit_l = self.db.execute("SELECT limit_l FROM days WHERE day = ?", (day,)).fetchone()
            rows = self.db.execute(
                "SELECT farm_id, allocated, consumed, sent, received, consumption_events, transfers "
                "FROM farm_day WHERE day = ? ORDER BY farm_id", (day,)).fetchall()
        keys = ("farm_id", "allocated", "consumed", "sent", "received", "consumption_events", "transfers")
        farms = [dict(zip(keys, r)) for r in rows]
        return {"day": day, "limit_l": limit_l[0] if limit_l else None,
                "total_allocated": sum(f["allocated"] for f in farms),
                "total_consumed": sum(f["consumed"] for f in farms),
                "total_transferred": sum(f["sent"] for f in farms),
                "farms": farms, "last_log_index": last,
                "events_processed": stats.get("events_processed", 0),
                "duplicates_ignored": stats.get("duplicates_ignored", 0)}
