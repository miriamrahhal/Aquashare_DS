"""Transfers between farms: reserve (sender's wallet) -> commit (ledger) -> confirm (sender's wallet),
then the recipient is credited from the ledger. The fake gateway applies the ledger's rules:
it checks the sender's balance, and a transfer_id is committed only once."""
import json

import httpx

from farm_node.journal import Journal
from farm_node.sync import LedgerSync
from farm_node.wallet import InsufficientWater, Wallet

DAY = "2026-06-01"


class FakeLedger:
    def __init__(self, allocation=1000):
        self.allocation = allocation
        self.transfers = {}            # transfer_id -> transfer
        self.down = False              # the ledger cannot be reached
        self.lose_next_answer = False  # commit, but the reply never arrives

    def balance(self, farm):
        sent = sum(t["amount_l"] for t in self.transfers.values() if t["sender"] == farm)
        received = sum(t["amount_l"] for t in self.transfers.values() if t["recipient"] == farm)
        return {"day": DAY, "allocated": self.allocation, "received": received, "sent": sent, "consumed": 0}

    def handler(self, request: httpx.Request):
        if self.down:
            raise httpx.ConnectError("ledger unreachable")
        path = request.url.path
        if path.endswith("/balance"):
            return httpx.Response(200, json=self.balance(path.split("/")[2]))
        if path.endswith("/transfers") and request.method == "GET":
            farm = path.split("/")[2]
            return httpx.Response(200, json=[t for t in self.transfers.values()
                                             if farm in (t["sender"], t["recipient"])])
        if path == "/transfers":
            t = json.loads(request.content)
            if t["transfer_id"] not in self.transfers:
                b = self.balance(t["sender"])
                if t["recipient"] not in ("farm-a", "farm-b"):
                    return httpx.Response(409, json={"detail": {"error": "unknown farm"}})
                if t["amount_l"] > b["allocated"] + b["received"] - b["sent"]:
                    return httpx.Response(409, json={"detail": {"error": "not enough water"}})
                self.transfers[t["transfer_id"]] = {**t, "status": "committed"}
            if self.lose_next_answer:
                self.lose_next_answer = False
                raise httpx.ReadTimeout("answer lost")
            return httpx.Response(200, json={"committed_index": len(self.transfers)})
        return httpx.Response(200, json={"result": {"ok": True}})


def make_farm(tmp_path, name, fake):
    wallet = Wallet(name, Journal(tmp_path / f"{name}.jsonl"))
    sync = LedgerSync(wallet, name, "http://gw", transport=httpx.MockTransport(fake.handler))
    sync.sync_once()                                   # learn the day and allocation
    return wallet, sync


def send(wallet, sync, recipient, amount, tid):
    wallet.reserve(DAY, tid, amount, recipient=recipient)
    return sync.submit_transfer(tid)


def test_transfer_moves_water_between_farms(tmp_path):
    fake = FakeLedger()
    a, sync_a = make_farm(tmp_path, "farm-a", fake)
    b, sync_b = make_farm(tmp_path, "farm-b", fake)
    assert send(a, sync_a, "farm-b", 300, "t1")["status"] == "committed"
    assert a.balance(DAY)["sent"] == 300 and a.balance(DAY)["reserved"] == 0
    sync_b.sync_once()                                 # the recipient learns about it from the ledger
    assert b.balance(DAY)["received"] == 300 and b.balance(DAY)["available"] == 1300


def test_reserved_water_cannot_be_spent_while_the_transfer_is_open(tmp_path):
    fake = FakeLedger()
    a, sync_a = make_farm(tmp_path, "farm-a", fake)
    fake.down = True
    assert send(a, sync_a, "farm-b", 800, "t1")["status"] == "pending"
    try:
        a.consume(DAY, 300)                            # only 200 L are free while 800 L are reserved
        assert False, "spent reserved water"
    except InsufficientWater:
        pass


def test_rejected_transfer_releases_the_reservation(tmp_path):
    fake = FakeLedger()
    a, sync_a = make_farm(tmp_path, "farm-a", fake)
    result = send(a, sync_a, "farm-z", 300, "t1")      # the ledger does not know farm-z
    assert result["status"] == "rejected"
    assert a.balance(DAY)["available"] == 1000 and a.open_reservations() == {}


def test_offline_sender_keeps_the_reservation_and_settles_later(tmp_path):
    fake = FakeLedger()
    a, sync_a = make_farm(tmp_path, "farm-a", fake)
    b, sync_b = make_farm(tmp_path, "farm-b", fake)
    fake.down = True
    assert send(a, sync_a, "farm-b", 400, "t1")["status"] == "pending"
    assert a.balance(DAY)["reserved"] == 400           # not released: the outcome is unknown
    fake.down = False
    sync_a.sync_once()                                 # the next sync sends it again
    sync_b.sync_once()
    assert a.balance(DAY)["sent"] == 400 and b.balance(DAY)["received"] == 400


def test_lost_answer_is_not_applied_twice(tmp_path):
    fake = FakeLedger()
    a, sync_a = make_farm(tmp_path, "farm-a", fake)
    b, sync_b = make_farm(tmp_path, "farm-b", fake)
    fake.lose_next_answer = True                       # the ledger commits, the reply is lost
    assert send(a, sync_a, "farm-b", 250, "t1")["status"] == "pending"
    sync_a.sync_once()                                 # sent again with the same transfer_id
    sync_b.sync_once()
    sync_b.sync_once()                                 # syncing twice does not credit twice
    assert len(fake.transfers) == 1
    assert a.balance(DAY)["sent"] == 250 and b.balance(DAY)["received"] == 250


def test_open_transfer_survives_a_sender_restart(tmp_path):
    fake = FakeLedger()
    a, sync_a = make_farm(tmp_path, "farm-a", fake)
    fake.down = True
    send(a, sync_a, "farm-b", 100, "t1")
    restarted = Wallet("farm-a", Journal(tmp_path / "farm-a.jsonl"))
    assert restarted.open_reservations()["t1"]["amount"] == 100
    fake.down = False
    LedgerSync(restarted, "farm-a", "http://gw", transport=httpx.MockTransport(fake.handler)).sync_once()
    assert restarted.balance(DAY)["sent"] == 100 and restarted.open_reservations() == {}