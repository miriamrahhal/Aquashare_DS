"""State machine tests: the rules for days, allocations, consumption and transfers.
They use the same separate test database as the storage tests (ledger_test)."""
import json
import uuid

import psycopg

from ledger_node.state_machine import StateMachine
from ledger_node.storage import Entry
from tests.test_storage import ADMIN_DSN, SCHEMA, dsn  # noqa: F401  (dsn is a pytest fixture)

DAY = "2026-06-01"


class Log:
    """Feeds commands to a state machine as consecutive committed log entries."""

    def __init__(self, sm):
        self.sm, self.index = sm, 0

    def run(self, **command):
        command.setdefault("cmd_id", str(uuid.uuid4()))
        self.index += 1
        return self.sm.apply(Entry(self.index, 1, json.dumps(command)))


def setup_day(log, limit=10_000, allocations=None):
    for farm in ("farm-a", "farm-b", "farm-c"):
        assert log.run(type="register_farm", farm_id=farm, name=farm.upper())["ok"]
    allocations = allocations or {"farm-a": 4000, "farm-b": 3000, "farm-c": 2000}
    return log.run(type="open_day", day=DAY, limit_l=limit, allocations=allocations)


def balances(sm):
    return {f["farm_id"]: f for f in sm.snapshot(DAY)["farms"]}


def events(*amounts, start=1):
    return [{"event_id": str(uuid.uuid4()), "seq": start + i, "amount_l": a} for i, a in enumerate(amounts)]


def test_a_full_day(dsn):
    sm = StateMachine(dsn)
    log = Log(sm)
    assert setup_day(log)["allocated_l"] == 9000
    assert log.run(type="record_consumption", farm_id="farm-a", day=DAY, events=events(500, 700))["amount_l"] == 1200
    assert log.run(type="transfer", transfer_id=str(uuid.uuid4()), day=DAY,
                   sender="farm-a", recipient="farm-b", amount_l=1000)["status"] == "committed"
    b = balances(sm)
    assert (b["farm-a"]["consumed"], b["farm-a"]["sent"], b["farm-a"]["available"]) == (1200, 1000, 1800)
    assert (b["farm-b"]["received"], b["farm-b"]["available"]) == (1000, 4000)


def test_allocations_above_the_daily_limit_are_rejected(dsn):
    sm = StateMachine(dsn)
    result = setup_day(Log(sm), limit=5000)
    assert not result["ok"] and "exceed" in result["error"]
    assert sm.snapshot()["day"] is None                     # nothing was written


def test_farm_cannot_use_more_than_it_holds(dsn):
    sm = StateMachine(dsn)
    log = Log(sm)
    setup_day(log)
    result = log.run(type="record_consumption", farm_id="farm-c", day=DAY, events=events(1500, 600))
    assert not result["ok"]
    assert balances(sm)["farm-c"]["consumed"] == 0          # the whole batch was undone, not just the second event


def test_transfer_larger_than_available_is_rejected(dsn):
    sm = StateMachine(dsn)
    log = Log(sm)
    setup_day(log)
    result = log.run(type="transfer", transfer_id=str(uuid.uuid4()), day=DAY,
                     sender="farm-c", recipient="farm-a", amount_l=2001)
    assert not result["ok"] and "only 2000 L" in result["error"]
    assert balances(sm)["farm-a"]["received"] == 0


def test_retried_command_is_applied_once(dsn):
    sm = StateMachine(dsn)
    log = Log(sm)
    setup_day(log)
    cmd = dict(type="transfer", transfer_id=str(uuid.uuid4()), day=DAY,
               sender="farm-a", recipient="farm-b", amount_l=300, cmd_id="retry-me")
    first, second = log.run(**cmd), log.run(**cmd)          # the client sent the same command twice
    assert first["ok"] and second["duplicate"]
    assert balances(sm)["farm-a"]["sent"] == 300


def test_offline_farm_resending_events_is_not_double_counted(dsn):
    sm = StateMachine(dsn)
    log = Log(sm)
    setup_day(log)
    batch = events(100, 200, 300)
    log.run(type="record_consumption", farm_id="farm-b", day=DAY, events=batch[:2])
    result = log.run(type="record_consumption", farm_id="farm-b", day=DAY, events=batch)  # resends all three
    assert (result["recorded"], result["skipped_duplicates"]) == (1, 2)
    assert balances(sm)["farm-b"]["consumed"] == 600


def test_restart_continues_after_last_applied_entry(dsn):
    sm = StateMachine(dsn)
    log = Log(sm)
    setup_day(log)
    log.run(type="record_consumption", farm_id="farm-a", day=DAY, events=events(250))
    restarted = StateMachine(dsn)
    assert restarted.last_applied() == log.index
    old_entry = Entry(log.index, 1, json.dumps({"type": "record_consumption", "farm_id": "farm-a",
                                                "day": DAY, "events": events(250, start=9)}))
    assert restarted.apply(old_entry)["already_applied"]    # an old entry delivered again changes nothing
    assert balances(restarted)["farm-a"]["consumed"] == 250


def test_days_only_move_forward_and_bad_commands_do_not_crash(dsn):
    sm = StateMachine(dsn)
    log = Log(sm)
    setup_day(log)
    assert not log.run(type="open_day", day="2026-05-31", limit_l=1, allocations={})["ok"]
    assert not log.run(type="transfer", day=DAY)["ok"]               # missing fields
    assert not log.run(type="flood_the_valley")["ok"]                # unknown command
    assert log.run(type="open_day", day="2026-06-02", limit_l=8000, allocations={"farm-a": 8000})["ok"]


def test_same_log_gives_same_state_on_every_node(dsn):
    second = dsn.rsplit("/", 1)[0] + "/ledger_test_b"
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        admin.execute("DROP DATABASE IF EXISTS ledger_test_b WITH (FORCE)")
        admin.execute("CREATE DATABASE ledger_test_b")
    node1, node2 = StateMachine(dsn), StateMachine(second)
    entries, log = [], Log(node1)
    original = node1.apply
    node1.apply = lambda e: (entries.append(e), original(e))[1]       # record what node 1 applied
    setup_day(log)
    log.run(type="record_consumption", farm_id="farm-a", day=DAY, events=events(400, 100))
    log.run(type="transfer", transfer_id=str(uuid.uuid4()), day=DAY, sender="farm-b", recipient="farm-c", amount_l=750)
    log.run(type="transfer", transfer_id=str(uuid.uuid4()), day=DAY, sender="farm-c", recipient="farm-a", amount_l=99999)
    for e in entries:
        node2.apply(e)                                                # node 2 replays the same log
    assert node1.snapshot(DAY) == node2.snapshot(DAY)
    node2.conn.close()
    with psycopg.connect(ADMIN_DSN, autocommit=True) as admin:
        admin.execute("DROP DATABASE IF EXISTS ledger_test_b WITH (FORCE)")