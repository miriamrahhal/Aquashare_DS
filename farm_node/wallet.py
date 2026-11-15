"""A farm's escrow wallet: the water rights it may spend locally, online or offline.

The wallet is event-sourced: every operation is first written to the journal, and
the wallet's state is the result of replaying the journal. All operations run under
one lock, so two concurrent operations can never spend the same water.
"""
import threading
import time
import uuid
from dataclasses import dataclass, field

from farm_node.journal import Journal


class InsufficientWater(Exception):
    pass


class UnknownReservation(Exception):
    pass


@dataclass
class DayBalance:
    allocated: int = 0
    received: int = 0
    sent: int = 0
    consumed: int = 0
    reserved: dict = field(default_factory=dict)   # transfer_id -> litres

    @property
    def available(self) -> int:
        return self.allocated + self.received - self.sent - self.consumed - sum(self.reserved.values())

    def as_dict(self) -> dict:
        return {
            "allocated": self.allocated,
            "received": self.received,
            "sent": self.sent,
            "consumed": self.consumed,
            "reserved": sum(self.reserved.values()),
            "available": self.available,
        }


class Wallet:
    def __init__(self, farm_id: str, journal: Journal):
        self.farm_id = farm_id
        self.journal = journal
        self._lock = threading.Lock()
        self._days: dict[str, DayBalance] = {}
        self._seq = 0
        self._ledger_days: set[str] = set()   # days whose allocation came from the ledger
        self._synced_upto = 0                 # every event up to this seq is known to the ledger
        self._outgoing: dict[str, dict] = {}  # open reservations: transfer_id -> {day, recipient, amount}
        self._credited: set[str] = set()      # incoming transfers already added to the wallet
        for event in journal.read_all():
            self._apply(event)

    # ------------------------------------------------------------ internals
    def _day(self, day: str) -> DayBalance:
        return self._days.setdefault(day, DayBalance())

    def _apply(self, event: dict) -> None:
        """Update in-memory state from one event. Used live and on replay."""
        self._seq = max(self._seq, event["seq"])
        day = self._day(event["day"])
        kind, amount = event["type"], event.get("amount", 0)
        if kind == "allocation":
            day.allocated = amount
            if event.get("source") == "ledger":
                self._ledger_days.add(event["day"])
        elif kind == "sync_ack":
            self._synced_upto = max(self._synced_upto, event["upto_seq"])
        elif kind == "consume":
            day.consumed += amount
        elif kind == "reserve":
            day.reserved[event["transfer_id"]] = amount
            self._outgoing[event["transfer_id"]] = {"day": event["day"], "amount": amount,
                                                    "recipient": event.get("recipient")}
        elif kind == "release":
            day.reserved.pop(event["transfer_id"], None)
            self._outgoing.pop(event["transfer_id"], None)
        elif kind == "transfer_sent":
            day.reserved.pop(event["transfer_id"], None)
            self._outgoing.pop(event["transfer_id"], None)
            day.sent += amount
        elif kind == "transfer_received":
            day.received += amount
            self._credited.add(event["transfer_id"])

    def _record(self, kind: str, day: str, amount: int = 0, **extra) -> dict:
        """Journal an event, then apply it. Must be called with the lock held."""
        event = {
            "event_id": str(uuid.uuid4()),
            "seq": self._seq + 1,
            "farm_id": self.farm_id,
            "type": kind,
            "day": day,
            "amount": amount,
            "ts": time.time(),
            **extra,
        }
        self.journal.append(event)   # durable first ...
        self._apply(event)           # ... then visible
        return event

    # ------------------------------------------------------------ operations
    def set_allocation(self, day: str, amount: int) -> dict:
        if amount < 0:
            raise ValueError("allocation cannot be negative")
        with self._lock:
            return self._record("allocation", day, amount)

    def consume(self, day: str, amount: int) -> dict:
        if amount <= 0:
            raise ValueError("amount must be positive")
        with self._lock:
            if amount > self._day(day).available:
                raise InsufficientWater(f"{amount} L requested, {self._day(day).available} L available")
            return self._record("consume", day, amount)

    def reserve(self, day: str, transfer_id: str, amount: int, recipient: str | None = None) -> dict:
        if amount <= 0:
            raise ValueError("amount must be positive")
        with self._lock:
            balance = self._day(day)
            if transfer_id in balance.reserved:
                return {"transfer_id": transfer_id, "already_reserved": True}
            if amount > balance.available:
                raise InsufficientWater(f"{amount} L requested, {balance.available} L available")
            return self._record("reserve", day, amount, transfer_id=transfer_id, recipient=recipient)

    def release(self, day: str, transfer_id: str) -> dict:
        with self._lock:
            if transfer_id not in self._day(day).reserved:
                raise UnknownReservation(transfer_id)
            return self._record("release", day, transfer_id=transfer_id)

    def confirm_sent(self, day: str, transfer_id: str) -> dict:
        with self._lock:
            amount = self._day(day).reserved.get(transfer_id)
            if amount is None:
                raise UnknownReservation(transfer_id)
            return self._record("transfer_sent", day, amount, transfer_id=transfer_id)

    def credit(self, day: str, transfer_id: str, amount: int) -> dict | None:
        """Add water received from another farm. A transfer is credited only once."""
        with self._lock:
            if transfer_id in self._credited:
                return None
            return self._record("transfer_received", day, amount, transfer_id=transfer_id)

    def open_reservations(self) -> dict[str, dict]:
        """Outgoing transfers reserved locally whose outcome on the ledger is not known yet."""
        with self._lock:
            return {tid: dict(info) for tid, info in self._outgoing.items()}

    # ------------------------------------------------------------ synchronisation with the ledger
    def set_ledger_allocation(self, day: str, amount: int) -> dict | None:
        """Record the allocation the ledger granted for a day (once per day)."""
        with self._lock:
            if day in self._ledger_days:
                return None
            return self._record("allocation", day, amount, source="ledger")

    def current_day(self) -> str | None:
        """The latest day opened by the ledger. The farm never decides the day from its own clock."""
        with self._lock:
            return max(self._ledger_days) if self._ledger_days else None

    def pending_consumption(self, limit: int = 200) -> tuple[str | None, list[dict]]:
        """The oldest consumption events the ledger has not confirmed yet, all from the same day."""
        with self._lock:
            pending = [e for e in self.journal.read_all()
                       if e["type"] == "consume" and e["seq"] > self._synced_upto and e["day"] in self._ledger_days]
        if not pending:
            return None, []
        day = pending[0]["day"]
        batch = []
        for e in pending:
            if e["day"] != day or len(batch) == limit:
                break
            batch.append(e)
        return day, batch

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self.journal.read_all()
                       if e["type"] == "consume" and e["seq"] > self._synced_upto and e["day"] in self._ledger_days)

    def mark_synced(self, day: str, upto_seq: int) -> dict:
        """The ledger has committed every consumption event up to upto_seq."""
        with self._lock:
            return self._record("sync_ack", day, upto_seq=upto_seq)

    def balance(self, day: str) -> dict:
        with self._lock:
            return {"farm_id": self.farm_id, "day": day, **self._day(day).as_dict()}

    def has_allocation(self, day: str) -> bool:
        with self._lock:
            return day in self._days and self._days[day].allocated > 0

    def events(self, limit: int = 50) -> list[dict]:
        return self.journal.read_all()[-limit:]