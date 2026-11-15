"""Event relay and analytics store tests (no RabbitMQ or Docker needed).

The relay reads applied commands from simulated ledger nodes and publishes them to a
fake broker; the store applies each event once, by its log index."""
import httpx

from analytics.events import to_event
from analytics.relay import Relay
from analytics.store import Store

DAY = "2026-06-01"
LOG = [
    {"log_index": 2, "command": {"type": "register_farm", "farm_id": "farm-a"}, "result": {"ok": True}},
    {"log_index": 3, "command": {"type": "open_day", "day": DAY, "limit_l": 10000,
                                 "allocations": {"farm-a": 5000, "farm-b": 5000}}, "result": {"ok": True}},
    {"log_index": 4, "command": {"type": "record_consumption", "farm_id": "farm-a", "day": DAY, "events": []},
     "result": {"ok": True, "recorded": 2, "amount_l": 40}},
    {"log_index": 5, "command": {"type": "transfer", "transfer_id": "t1", "day": DAY, "sender": "farm-a",
                                 "recipient": "farm-b", "amount_l": 500}, "result": {"ok": True}},
    {"log_index": 6, "command": {"type": "transfer", "transfer_id": "t2", "day": DAY, "sender": "farm-b",
                                 "recipient": "farm-a", "amount_l": 99999}, "result": {"ok": False, "error": "no"}},
    {"log_index": 7, "command": {"type": "record_consumption", "farm_id": "farm-b", "day": DAY, "events": []},
     "result": {"ok": True, "recorded": 1, "amount_l": 20}},
]


class FakeLedger:
    def __init__(self, entries, down=()):
        self.entries, self.down = entries, set(down)

    def handler(self, request):
        if request.url.host in self.down:
            raise httpx.ConnectError("node down")
        after = int(request.url.params["after"])
        return httpx.Response(200, json=[e for e in self.entries if e["log_index"] > after])


class FakeBroker:
    def __init__(self, fail_after=None):
        self.messages, self.fail_after = [], fail_after

    def publish(self, event):
        if self.fail_after is not None and len(self.messages) >= self.fail_after:
            raise OSError("broker unreachable")
        self.messages.append(event)


def relay(tmp_path, ledger, broker):
    return Relay(["http://l1", "http://l2"], broker, tmp_path / "position.txt",
                 transport=httpx.MockTransport(ledger.handler))


def test_only_successful_changes_become_events():
    events = [to_event(e) for e in LOG]
    assert [e["type"] if e else None for e in events] == [
        "register_farm", "open_day", "record_consumption", "transfer", None, "record_consumption"]
    assert to_event({"log_index": 9, "command": {"type": "noop"}, "result": {"ok": True}}) is None
    assert to_event({**LOG[3], "result": {"ok": True, "duplicate": True}}) is None


def test_relay_publishes_in_order_and_skips_rejected(tmp_path):
    broker = FakeBroker()
    r = relay(tmp_path, FakeLedger(LOG, down={"l1"}), broker)     # first node down: uses the second
    assert r.run_once() == {"ledger": "ok", "published": 5, "position": 7}
    assert [m["log_index"] for m in broker.messages] == [2, 3, 4, 5, 7]
    assert r.run_once()["published"] == 0                          # nothing new


def test_relay_position_survives_a_restart(tmp_path):
    broker = FakeBroker()
    relay(tmp_path, FakeLedger(LOG[:3]), broker).run_once()
    restarted = relay(tmp_path, FakeLedger(LOG), broker)
    assert restarted.position == 4
    restarted.run_once()
    assert [m["log_index"] for m in broker.messages] == [2, 3, 4, 5, 7]   # each published once


def test_broker_outage_resumes_where_it_stopped(tmp_path):
    broker = FakeBroker(fail_after=2)
    r = relay(tmp_path, FakeLedger(LOG), broker)
    try:
        r.run_once()
    except OSError:
        pass
    assert r.position == 3                                         # stopped after the last confirmed event
    broker.fail_after = None
    r.run_once()
    assert [m["log_index"] for m in broker.messages] == [2, 3, 4, 5, 7]


def test_store_totals_and_duplicates(tmp_path):
    store = Store(tmp_path / "a.db")
    events = [e for e in map(to_event, LOG) if e]
    for e in events + events[2:4]:                                 # two events delivered twice
        store.apply(e)
    s = store.summary(DAY)
    farms = {f["farm_id"]: f for f in s["farms"]}
    assert (farms["farm-a"]["consumed"], farms["farm-a"]["sent"]) == (40, 500)
    assert (farms["farm-b"]["consumed"], farms["farm-b"]["received"]) == (20, 500)
    assert (s["events_processed"], s["duplicates_ignored"], s["last_log_index"]) == (5, 2, 7)


def test_store_survives_a_restart(tmp_path):
    first = Store(tmp_path / "a.db")
    for e in filter(None, map(to_event, LOG[:4])):
        first.apply(e)
    again = Store(tmp_path / "a.db")
    assert again.apply(to_event(LOG[3])) is False                  # already processed before the restart
    assert again.summary(DAY)["total_consumed"] == 40
