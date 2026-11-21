"""Majority reads: the gateway only returns data that at least 2 of the 3 ledger nodes agree on.

The fake nodes below answer /state. One of them can be made to "lie" (its database was
altered), to be behind (it has applied fewer entries), or to be down."""
import httpx
from fastapi.testclient import TestClient

from gateway.main import LedgerClient, create_app

NODES = {"ledger-1": "http://l1", "ledger-2": "http://l2", "ledger-3": "http://l3"}
TRUE_STATE = {"day": "2026-11-20", "farms": [{"farm_id": "farm-a", "available": 900}]}


def fake(liars=(), down=(), behind=()):
    def handler(request: httpx.Request):
        node = {v: k for k, v in NODES.items()}[f"http://{request.url.host}"]
        if node in down:
            raise httpx.ConnectError("connection refused")
        if node in liars:                            # this node's database says farm-a has much more water
            return httpx.Response(200, json={"node_id": node, "last_applied": 50, "day": "2026-11-20",
                                             "farms": [{"farm_id": "farm-a", "available": 99999}]})
        if node in behind:                           # an honest node that has not applied the last entries yet
            return httpx.Response(200, json={"node_id": node, "last_applied": 48, "day": "2026-11-20",
                                             "farms": [{"farm_id": "farm-a", "available": 1000}]})
        return httpx.Response(200, json={"node_id": node, "last_applied": 50, **TRUE_STATE})
    return httpx.MockTransport(handler)


def api(**kw):
    ledger = LedgerClient(NODES, transport=fake(**kw), pause=0.01)
    return TestClient(create_app(ledger))


def test_all_nodes_agree():
    r = api().get("/state").json()
    assert r["farms"][0]["available"] == 900 and r["agreed_by"] == ["ledger-1", "ledger-2", "ledger-3"]
    assert r["outvoted"] == []


def test_a_lying_node_is_outvoted():
    r = api(liars={"ledger-3"}).get("/state").json()
    assert r["farms"][0]["available"] == 900            # the true value, not 99999
    assert r["outvoted"] == ["ledger-3"]


def test_the_balance_of_a_farm_ignores_the_liar():
    r = api(liars={"ledger-1"}).get("/farms/farm-a/balance").json()
    assert r["available"] == 900


def test_a_node_that_is_only_behind_is_not_called_a_liar():
    r = api(behind={"ledger-2"}).get("/state").json()
    assert r["outvoted"] == [] and r["behind"] == ["ledger-2"]


def test_one_node_down_still_gives_a_majority():
    r = api(down={"ledger-2"}).get("/state")
    assert r.status_code == 200 and r.json()["agreed_by"] == ["ledger-1", "ledger-3"]


def test_one_liar_and_one_node_down_means_no_majority():
    r = api(liars={"ledger-3"}, down={"ledger-1"}).get("/state")
    assert r.status_code == 503                         # 1 against 1: the gateway refuses rather than guess


def test_two_nodes_down_means_no_majority():
    assert api(down={"ledger-1", "ledger-2"}).get("/state").status_code == 503


def test_leader_mode_keeps_the_old_behaviour():
    ledger = LedgerClient(NODES, transport=fake(liars={"ledger-1"}), pause=0.01, read_mode="leader")
    r = TestClient(create_app(ledger)).get("/state").json()
    assert r["farms"][0]["available"] == 99999          # trusts the first node: why majority reads matter
