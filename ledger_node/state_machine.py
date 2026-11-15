"""The ledger's state machine: turns committed log entries into water-allocation state.

Every ledger node applies the same committed entries in the same order, so every node's
database ends up in exactly the same state. To make that true, apply() must be
deterministic: it never reads the clock, never uses random numbers, and takes every
value (dates, IDs, amounts) from the command itself.

Each entry is applied in one database transaction that also stores last_applied, so after
a crash a node continues exactly where it stopped and never applies an entry twice.
"""
import json
import threading
from datetime import date
from pathlib import Path

import psycopg

SCHEMA = Path(__file__).with_name("schema.sql")


class Rejected(Exception):
    """A command that breaks a rule. It is still part of the log, but changes nothing."""


class StateMachine:
    def __init__(self, dsn: str):
        self.conn = psycopg.connect(dsn, autocommit=True)
        self.lock = threading.Lock()
        with self.lock:
            self.conn.execute(SCHEMA.read_text())     # creates any table that is missing (all IF NOT EXISTS)

    # ------------------------------------------------------------------ applying entries
    def last_applied(self) -> int:
        with self.lock:
            return self.conn.execute("SELECT last_applied FROM raft_metadata WHERE id = 1").fetchone()[0]

    def apply(self, entry) -> dict:
        command = json.loads(entry.command)
        with self.lock, self.conn.transaction():
            last = self.conn.execute(
                "SELECT last_applied FROM raft_metadata WHERE id = 1 FOR UPDATE").fetchone()[0]
            if entry.index <= last:
                return {"ok": True, "already_applied": True}          # safety net: never apply twice
            cmd_id = command.get("cmd_id")
            stored = cmd_id and self.conn.execute(
                "SELECT result FROM applied_commands WHERE cmd_id = %s", (cmd_id,)).fetchone()
            if stored:
                result = {**stored[0], "duplicate": True}             # a retry: same answer, no change
            else:
                result = self._execute(command, entry.index)
                if cmd_id:
                    self.conn.execute("INSERT INTO applied_commands (cmd_id, log_index, result) "
                                      "VALUES (%s, %s, %s)", (cmd_id, entry.index, json.dumps(result)))
            self.conn.execute("UPDATE raft_metadata SET last_applied = %s, commit_index = GREATEST(commit_index, %s) "
                              "WHERE id = 1", (entry.index, entry.index))
        return result

    def _execute(self, command: dict, index: int) -> dict:
        handler = getattr(self, f"_cmd_{command.get('type')}", None)
        if handler is None:
            return {"ok": False, "error": f"unknown command type {command.get('type')!r}"}
        try:
            with self.conn.transaction():                       # savepoint: a rejection undoes partial writes
                return {"ok": True, **(handler(command, index) or {})}
        except Rejected as exc:
            return {"ok": False, "error": str(exc)}
        except psycopg.OperationalError:
            raise                                               # database unreachable: retry later, do not skip
        except Exception as exc:                                # malformed command: same answer on every node
            return {"ok": False, "error": f"invalid command: {type(exc).__name__}: {exc}"}

    # ------------------------------------------------------------------ commands
    def _cmd_noop(self, cmd, index):
        return {}

    _cmd_test = _cmd_noop        # test commands from the earlier steps change nothing

    def _cmd_register_farm(self, cmd, index):
        farm_id, name = cmd["farm_id"], cmd.get("name") or cmd["farm_id"]
        if self._one("SELECT 1 FROM farms WHERE farm_id = %s", farm_id):
            raise Rejected(f"farm {farm_id} is already registered")
        self.conn.execute("INSERT INTO farms (farm_id, name) VALUES (%s, %s)", (farm_id, name))
        return {"farm_id": farm_id}

    def _cmd_open_day(self, cmd, index):
        """Open a new irrigation day: set the reservoir's limit and each farm's allocation."""
        day, limit_l = date.fromisoformat(cmd["day"]), int(cmd["limit_l"])
        allocations = {f: int(a) for f, a in cmd.get("allocations", {}).items()}
        current = self._one("SELECT max(day) FROM daily_limits")
        if current is not None and day <= current:
            raise Rejected(f"day {day} is not after the current day {current}")
        if limit_l < 0 or any(a < 0 for a in allocations.values()):
            raise Rejected("amounts cannot be negative")
        farms = {r[0] for r in self.conn.execute("SELECT farm_id FROM farms").fetchall()}
        unknown = sorted(set(allocations) - farms)
        if unknown:
            raise Rejected(f"unknown farms: {', '.join(unknown)}")
        total = sum(allocations.values())
        if total > limit_l:
            raise Rejected(f"allocations ({total} L) exceed the daily limit ({limit_l} L)")
        self.conn.execute("INSERT INTO daily_limits (day, limit_l) VALUES (%s, %s)", (day, limit_l))
        for farm_id in sorted(farms):
            amount = allocations.get(farm_id, 0)
            self.conn.execute("INSERT INTO allocations (day, farm_id, amount_l) VALUES (%s, %s, %s)",
                              (day, farm_id, amount))
            self.conn.execute("INSERT INTO farm_balances (day, farm_id, allocated) VALUES (%s, %s, %s)",
                              (day, farm_id, amount))
        return {"day": str(day), "limit_l": limit_l, "allocated_l": total}

    def _cmd_record_consumption(self, cmd, index):
        """A farm reports water it used (possibly while it was offline). Events already recorded are skipped."""
        farm_id, day = cmd["farm_id"], date.fromisoformat(cmd["day"])
        balance = self._balance(day, farm_id)
        recorded = skipped = total = 0
        for ev in sorted(cmd["events"], key=lambda e: e["seq"]):
            amount = int(ev["amount_l"])
            if amount <= 0:
                raise Rejected(f"event {ev['event_id']}: amount must be positive")
            if self._one("SELECT 1 FROM consumption_records WHERE event_id = %s", ev["event_id"]):
                skipped += 1
                continue
            if balance["consumed"] + total + amount > balance["allocated"] + balance["received"] - balance["sent"]:
                raise Rejected(f"farm {farm_id} would use more water than it holds on {day}")
            self.conn.execute("INSERT INTO consumption_records (event_id, farm_id, seq, day, amount_l, log_index) "
                              "VALUES (%s, %s, %s, %s, %s, %s)", (ev["event_id"], farm_id, ev["seq"], day, amount, index))
            recorded, total = recorded + 1, total + amount
        self.conn.execute("UPDATE farm_balances SET consumed = consumed + %s WHERE day = %s AND farm_id = %s",
                          (total, day, farm_id))
        return {"recorded": recorded, "skipped_duplicates": skipped, "amount_l": total}

    def _cmd_transfer(self, cmd, index):
        """Move water from one farm's allocation to another's for the same day."""
        tid, day = cmd["transfer_id"], date.fromisoformat(cmd["day"])
        sender, recipient, amount = cmd["sender"], cmd["recipient"], int(cmd["amount_l"])
        existing = self._one("SELECT status FROM transfers WHERE transfer_id = %s", tid)
        if existing:
            return {"transfer_id": tid, "status": existing, "duplicate": True}
        if sender == recipient:
            raise Rejected("a farm cannot transfer water to itself")
        if amount <= 0:
            raise Rejected("amount must be positive")
        s = self._balance(day, sender)
        self._balance(day, recipient)
        available = s["allocated"] + s["received"] - s["sent"] - s["consumed"]
        if amount > available:
            raise Rejected(f"farm {sender} has only {available} L available on {day}")
        self.conn.execute("INSERT INTO transfers (transfer_id, day, sender, recipient, amount_l, status, log_index) "
                          "VALUES (%s, %s, %s, %s, %s, 'committed', %s)", (tid, day, sender, recipient, amount, index))
        self.conn.execute("UPDATE farm_balances SET sent = sent + %s WHERE day = %s AND farm_id = %s",
                          (amount, day, sender))
        self.conn.execute("UPDATE farm_balances SET received = received + %s WHERE day = %s AND farm_id = %s",
                          (amount, day, recipient))
        return {"transfer_id": tid, "status": "committed"}

    # ------------------------------------------------------------------ helpers and queries
    def _one(self, sql, *params):
        row = self.conn.execute(sql, params).fetchone()
        return row[0] if row else None

    def _balance(self, day, farm_id) -> dict:
        row = self.conn.execute("SELECT allocated, received, sent, consumed FROM farm_balances "
                                "WHERE day = %s AND farm_id = %s", (day, farm_id)).fetchone()
        if row is None:
            raise Rejected(f"no allocation for farm {farm_id} on {day} (unknown farm or day not opened)")
        return dict(zip(("allocated", "received", "sent", "consumed"), row))

    def events(self, after: int = 0, limit: int = 200) -> list[dict]:
        """Commands applied after a log index, with their results (read by the event relay).

        Only entries up to last_applied are returned, so an event is never published
        before it is committed and applied. Rejected commands are included with ok=false,
        so the relay can move past them without publishing them."""
        with self.lock:
            rows = self.conn.execute(
                "SELECT r.log_index, r.command::text, a.result::text FROM raft_log r "
                "JOIN applied_commands a ON a.log_index = r.log_index "
                "WHERE r.log_index > %s AND r.log_index <= (SELECT last_applied FROM raft_metadata WHERE id = 1) "
                "ORDER BY r.log_index LIMIT %s", (after, limit)).fetchall()
        return [{"log_index": i, "command": json.loads(c), "result": json.loads(r)} for i, c, r in rows]

    def transfers(self, farm_id: str, day: str | None = None) -> list[dict]:
        """Committed transfers sent or received by a farm (all days, or one day)."""
        with self.lock:
            rows = self.conn.execute(
                "SELECT transfer_id::text, day::text, sender, recipient, amount_l, status, log_index FROM transfers "
                "WHERE (sender = %s OR recipient = %s) AND (%s::date IS NULL OR day = %s::date) ORDER BY log_index",
                (farm_id, farm_id, day, day)).fetchall()
        keys = ("transfer_id", "day", "sender", "recipient", "amount_l", "status", "log_index")
        return [dict(zip(keys, r)) for r in rows]

    def snapshot(self, day: str | None = None) -> dict:
        """Current state as seen by this node (used by the API and to compare nodes)."""
        with self.lock:
            day = day or self._one("SELECT max(day)::text FROM daily_limits")
            if day is None:
                return {"day": None, "farms": []}
            limit_l = self._one("SELECT limit_l FROM daily_limits WHERE day = %s", day)
            rows = self.conn.execute(
                "SELECT farm_id, allocated, received, sent, consumed, allocated + received - sent - consumed "
                "FROM farm_balances WHERE day = %s ORDER BY farm_id", (day,)).fetchall()
        keys = ("farm_id", "allocated", "received", "sent", "consumed", "available")
        farms = [dict(zip(keys, r)) for r in rows]
        return {"day": day, "limit_l": limit_l, "farms": farms,
                "total_allocated": sum(f["allocated"] for f in farms),
                "total_consumed": sum(f["consumed"] for f in farms)}