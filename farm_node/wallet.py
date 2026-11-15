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
        elif kind == "consume":
            day.consumed += amount
        elif kind == "reserve":
            day.reserved[event["transfer_id"]] = amount
        elif kind == "release":
            day.reserved.pop(event["transfer_id"], None)
        elif kind == "transfer_sent":
            day.reserved.pop(event["transfer_id"], None)
            day.sent += amount
        elif kind == "transfer_received":
            day.received += amount

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

    def reserve(self, day: str, transfer_id: str, amount: int) -> dict:
        if amount <= 0:
            raise ValueError("amount must be positive")
        with self._lock:
            balance = self._day(day)
            if transfer_id in balance.reserved:
                return {"transfer_id": transfer_id, "already_reserved": True}
            if amount > balance.available:
                raise InsufficientWater(f"{amount} L requested, {balance.available} L available")
            return self._record("reserve", day, amount, transfer_id=transfer_id)

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

    def credit(self, day: str, transfer_id: str, amount: int) -> dict:
        with self._lock:
            return self._record("transfer_received", day, amount, transfer_id=transfer_id)

    def balance(self, day: str) -> dict:
        with self._lock:
            return {"farm_id": self.farm_id, "day": day, **self._day(day).as_dict()}

    def has_allocation(self, day: str) -> bool:
        with self._lock:
            return day in self._days and self._days[day].allocated > 0

    def events(self, limit: int = 50) -> list[dict]:
        return self.journal.read_all()[-limit:]