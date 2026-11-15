"""Gateway tests with simulated ledger nodes (no Docker needed).

Each fake node answers like a real ledger node would: the leader commits, a node that
is down refuses the connection, and a node without a leader answers 503."""
import json

import httpx
from fastapi.testclient import TestClient

from gateway.main import LedgerClient, create_app

NODES = {"ledger-1": "http://l1", "ledger-2": "http://l2", "ledger-3": "http://l3"}


class FakeLedger:
    def __init__(self, leader="ledger-2", down=(), no_leader_for=0):
        self.leader, self.down = leader, set(down)
        self.no_leader_for = no_leader_for          # number of requests answered 503 (election in progress)
        self.calls, self.applied = [], {}           # applied: cmd_id -> result (exactly-once, like the ledger)

    def handler(self, request: httpx.Request):
        node = {v: k for k, v in NODES.items()}[f"http://{request.url.host}"]
        self.calls.append(node)
        if node in self.down:
            raise httpx.ConnectError("connection refused")
        if request.url.path == "/state":
            return httpx.Response(200, json={"node_id": node, "day": "2026-06-01",
                                             "farms": [{"farm_id": "farm-a", "available": 900}]})
        if self.no_leader_for > 0:
            self.no_leader_for -= 1
            return httpx.Response(503, json={"detail": "no leader is currently known"})
        command = json.loads(request.content)["command"]
        if command.get("amount_l", 0) > 1000:
            result = {"ok": False, "error": "farm farm-a has only 1000 L available"}
        else:
            result = self.applied.setdefault(command["cmd_id"], {"ok": True})
        body = {"committed_index": len(self.applied), "leader_id": self.leader, "result": result}
        if node != self.leader:
            body["forwarded_by"] = node
        return httpx.Response(200, json=body)


def client(fake, **kw):
    ledger = LedgerClient(NODES, transport=httpx.MockTransport(fake.handler), pause=0.01, **kw)
    return TestClient(create_app(ledger)), ledger


TRANSFER = {"day": "2026-06-01", "sender": "farm-a", "recipient": "farm-b", "amount_l": 500}


def test_command_goes_through_and_leader_is_remembered():
    fake = FakeLedger(leader="ledger-2")
    api, ledger = client(fake)
    assert api.post("/transfers", json=TRANSFER).status_code == 200
    assert ledger.leader_url == "http://l2"
    fake.calls.clear()
    api.post("/transfers", json=TRANSFER)
    assert fake.calls == ["ledger-2"]                # the second request goes straight to the leader


def test_node_that_is_down_is_skipped():
    fake = FakeLedger(leader="ledger-3", down={"ledger-1"})
    api, _ = client(fake)
    reply = api.post("/transfers", json=TRANSFER)
    assert reply.status_code == 200 and reply.json()["leader_id"] == "ledger-3"


def test_retries_during_an_election_reuse_the_same_cmd_id():
    fake = FakeLedger(no_leader_for=5)               # the first 5 attempts arrive during an election
    api, _ = client(fake)
    reply = api.post("/transfers", json=TRANSFER)
    assert reply.status_code == 200 and reply.json()["attempts"] == 6
    assert len(fake.applied) == 1                    # applied once, although sent six times


def test_rejected_command_returns_409_with_the_reason():
    api, _ = client(FakeLedger())
    reply = api.post("/transfers", json={**TRANSFER, "amount_l": 5000})
    assert reply.status_code == 409 and "only 1000 L" in reply.json()["detail"]["error"]


def test_no_majority_returns_503_after_the_retry_window():
    fake = FakeLedger(down={"ledger-1", "ledger-2", "ledger-3"})
    api, _ = client(fake, retry_seconds=0.2)
    assert api.post("/transfers", json=TRANSFER).status_code == 503


def test_balance_is_read_from_a_live_node():
    api, _ = client(FakeLedger(down={"ledger-1"}))
    reply = api.get("/farms/farm-a/balance")
    assert reply.status_code == 200 and reply.json()["available"] == 900