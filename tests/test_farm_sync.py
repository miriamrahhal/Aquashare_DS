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

# ------------------------------------------------------------ a farm that lost its data
class LedgerWithHistory(FakeLedger):
    """A ledger that already holds 300 L used and 200 L sent by this farm today."""
    def handler(self, request: httpx.Request):
        if request.method == "GET" and not request.url.path.endswith("/transfers"):
            return httpx.Response(200, json={"day": DAY, "allocated": self.allocation,
                                             "consumed": 300 + sum(self.events.values()), "sent": 200})
        return super().handler(request)


def test_a_farm_that_lost_its_journal_starts_from_the_ledger(tmp_path):
    wallet, sync = farm(tmp_path, LedgerWithHistory(allocation=1000))   # empty journal, like after a loss
    sync.sync_once()
    b = wallet.balance(DAY)
    assert b["consumed"] == 300 and b["sent"] == 200 and b["available"] == 500   # not the whole 1000 L
    try:
        wallet.consume(DAY, 600)
        assert False, "the farm spent water the ledger already counted"
    except InsufficientWater:
        pass


def test_the_recovery_survives_a_restart_and_is_not_applied_twice(tmp_path):
    fake = LedgerWithHistory(allocation=1000)
    wallet, sync = farm(tmp_path, fake)
    sync.sync_once()
    sync.sync_once()                                          # the day is known now: nothing more to recover
    again = Wallet("farm-a", Journal(tmp_path / "journal.jsonl"))   # the farm restarts and replays its journal
    assert again.balance(DAY)["available"] == wallet.balance(DAY)["available"] == 500


def test_a_normal_farm_recovers_nothing(tmp_path):
    wallet, sync = farm(tmp_path, FakeLedger(allocation=1000))
    sync.sync_once()
    assert not any(e["type"] == "recovered" for e in wallet.events())
    assert wallet.balance(DAY)["available"] == 1000


def test_a_refused_key_is_tried_again_with_a_new_cmd_id(tmp_path):
    """After the operator replaces a lost key, the farm's next attempt must be a new command,
    not a retry that the ledger would answer with its first refusal."""
    from shared import signing
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    seen, answers = [], [409, 200]

    def handler(request: httpx.Request):
        if request.url.path.endswith("/key"):
            seen.append(json.loads(request.content)["cmd_id"])
            return httpx.Response(answers.pop(0), json={})
        if request.url.path.endswith("/transfers"):
            return httpx.Response(200, json=[])
        return httpx.Response(200, json={"day": DAY, "allocated": 1000, "consumed": 0})

    wallet = Wallet("farm-a", Journal(tmp_path / "journal.jsonl"))
    sync = LedgerSync(wallet, "farm-a", "http://gw", transport=httpx.MockTransport(handler),
                      key=Ed25519PrivateKey.generate())
    assert not sync.sync_once()["online"]                   # refused: the old key is still registered
    assert sync.sync_once()["online"] and sync.key_registered   # the operator replaced it: accepted now
    assert len(seen) == 2 and seen[0] != seen[1]
