"""Farm-to-ledger synchronisation tests, with a simulated gateway (no Docker needed).

The fake gateway behaves like the real ledger: it opens a day with an allocation and
counts each consumption event only once, by its event_id."""
import json

import httpx

from farm_node.journal import Journal
from farm_node.sync import LedgerSync
from farm_node.wallet import InsufficientWater, Wallet

DAY = "2026-06-01"


class FakeLedger:
    def __init__(self, allocation=1000):
        self.allocation, self.events = allocation, {}     # event_id -> amount
        self.lose_next_answer = False                     # commit, but the reply never reaches the farm

    def handler(self, request: httpx.Request):
        if request.method == "GET" and request.url.path.endswith("/transfers"):
            return httpx.Response(200, json=[])                # no transfers in these tests
        if request.method == "GET":
            return httpx.Response(200, json={"day": DAY, "allocated": self.allocation,
                                             "consumed": sum(self.events.values())})
        body = json.loads(request.content)
        for e in body["events"]:
            self.events.setdefault(e["event_id"], e["amount_l"])
        if self.lose_next_answer:
            self.lose_next_answer = False
            raise httpx.ReadTimeout("answer lost")
        return httpx.Response(200, json={"result": {"ok": True}})


def farm(tmp_path, fake):
    wallet = Wallet("farm-a", Journal(tmp_path / "journal.jsonl"))
    return wallet, LedgerSync(wallet, "farm-a", "http://gw", transport=httpx.MockTransport(fake.handler))


def test_farm_uses_only_the_allocation_granted_by_the_ledger(tmp_path):
    fake = FakeLedger(allocation=1000)
    wallet, sync = farm(tmp_path, fake)
    assert wallet.current_day() is None                       # nothing granted yet: no water
    sync.sync_once()
    assert wallet.current_day() == DAY and wallet.balance(DAY)["allocated"] == 1000
    try:
        wallet.consume(DAY, 1001)
        assert False, "consumed more than the ledger granted"
    except InsufficientWater:
        pass


def test_consumption_is_sent_once(tmp_path):
    fake = FakeLedger()
    wallet, sync = farm(tmp_path, fake)
    sync.sync_once()
    for _ in range(3):
        wallet.consume(DAY, 20)
    assert sync.sync_once()["sent"] == 3
    assert sync.sync_once()["sent"] == 0                      # nothing left to send
    assert sum(fake.events.values()) == 60


def test_offline_farm_keeps_irrigating_and_catches_up(tmp_path):
    fake = FakeLedger()
    wallet, sync = farm(tmp_path, fake)
    sync.sync_once()
    sync.link_up = False                                      # the farm loses its connection
    for _ in range(10):
        wallet.consume(DAY, 20)                               # ... but keeps irrigating from its wallet
    status = sync.sync_once()
    assert not status["online"] and status["pending"] == 10 and fake.events == {}
    sync.link_up = True                                       # connection restored
    assert sync.sync_once() == {"online": True, "sent": 10, "pending": 0}
    assert sum(fake.events.values()) == wallet.balance(DAY)["consumed"] == 200


def test_lost_answer_does_not_double_count(tmp_path):
    fake = FakeLedger()
    wallet, sync = farm(tmp_path, fake)
    sync.sync_once()
    wallet.consume(DAY, 50)
    fake.lose_next_answer = True                              # the ledger commits, the reply is lost
    assert sync.sync_once()["pending"] == 1                   # the farm does not know it was committed
    sync.sync_once()                                          # so it sends the event again
    assert sum(fake.events.values()) == 50                    # ... and it is still counted once


def test_sync_position_survives_a_restart(tmp_path):
    fake = FakeLedger()
    wallet, sync = farm(tmp_path, fake)
    sync.sync_once()
    wallet.consume(DAY, 30)
    sync.sync_once()
    wallet.consume(DAY, 40)                                   # not yet synced when the farm restarts
    restarted = Wallet("farm-a", Journal(tmp_path / "journal.jsonl"))
    assert restarted.pending_count() == 1
    assert restarted.current_day() == DAY


def test_days_not_opened_by_the_ledger_are_never_sent(tmp_path):
    fake = FakeLedger()
    wallet, sync = farm(tmp_path, fake)
    wallet.set_allocation("2020-01-01", 500)                  # a local allocation, not from the ledger
    wallet.consume("2020-01-01", 100)
    sync.sync_once()
    assert fake.events == {} and wallet.pending_count() == 0