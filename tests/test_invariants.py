"""Invariant checker tests: a correct ledger passes, and each kind of corruption is detected."""
import json
import uuid

import psycopg

from ledger_node.state_machine import StateMachine
from ledger_node.storage import Entry, PostgresStorage
from scripts.check_invariants import check_cluster, check_node
from tests.test_storage import dsn  # noqa: F401  (pytest fixture)

DAY = "2026-06-01"


def build_ledger(dsn):
    storage, sm = PostgresStorage(dsn), StateMachine(dsn)
    commands = [{"type": "register_farm", "farm_id": f} for f in ("farm-a", "farm-b")]
    commands += [{"type": "open_day", "day": DAY, "limit_l": 2000, "allocations": {"farm-a": 1000, "farm-b": 1000}},
                 {"type": "record_consumption", "farm_id": "farm-a", "day": DAY,
                  "events": [{"event_id": str(uuid.uuid4()), "seq": 1, "amount_l": 300}]},
                 {"type": "transfer", "transfer_id": str(uuid.uuid4()), "day": DAY, "sender": "farm-a",
                  "recipient": "farm-b", "amount_l": 400}]
    for i, c in enumerate(commands, start=1):
        entry = Entry(i, 1, json.dumps({**c, "cmd_id": str(uuid.uuid4())}))
        storage.append([entry])
        sm.apply(entry)
    return psycopg.connect(dsn, autocommit=True)


def failed(results):
    return [name for name, passed, _ in results if not passed]


def test_correct_ledger_passes_every_check(dsn):
    conn = build_ledger(dsn)
    assert failed(check_node(conn)) == []
    assert failed(check_cluster({"a": conn, "b": psycopg.connect(dsn, autocommit=True)}, attempts=1)) == []


def test_water_created_out_of_nothing_is_detected(dsn):
    conn = build_ledger(dsn)
    conn.execute("UPDATE farm_balances SET received = received + 50 WHERE farm_id = 'farm-b'")
    assert {"I4 transfers conserve water", "I5 balances match the records"} <= set(failed(check_node(conn)))


def test_edited_log_and_over_allocation_are_detected(dsn):
    conn = build_ledger(dsn)
    conn.execute("""UPDATE raft_log SET command = '{"type": "noop"}' WHERE log_index = 2""")
    conn.execute("UPDATE daily_limits SET limit_l = 1500")
    assert {"I1 hash chain unbroken", "I2 allocations <= daily limit"} <= set(failed(check_node(conn)))
