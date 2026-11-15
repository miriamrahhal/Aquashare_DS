"""Tests for the escrow wallet: local limits hold under concurrency and after a crash."""
import threading

import pytest

from farm_node.journal import Journal
from farm_node.wallet import InsufficientWater, Wallet

DAY = "2026-11-15"


def make_wallet(tmp_path, quota=1000):
    wallet = Wallet("farm-a", Journal(tmp_path / "journal.jsonl"))
    wallet.set_allocation(DAY, quota)
    return wallet


def test_cannot_consume_more_than_quota(tmp_path):
    wallet = make_wallet(tmp_path)
    wallet.consume(DAY, 900)
    with pytest.raises(InsufficientWater):
        wallet.consume(DAY, 200)
    assert wallet.balance(DAY)["available"] == 100


def test_concurrent_consumption_never_exceeds_quota(tmp_path):
    wallet = make_wallet(tmp_path, quota=1000)
    successes, failures = [], []

    def worker():
        try:
            wallet.consume(DAY, 100)
            successes.append(1)
        except InsufficientWater:
            failures.append(1)

    threads = [threading.Thread(target=worker) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(successes) == 10 and len(failures) == 20
    assert wallet.balance(DAY)["consumed"] == 1000


def test_reserved_water_cannot_be_consumed(tmp_path):
    wallet = make_wallet(tmp_path, quota=1000)
    wallet.reserve(DAY, "t1", 500)
    wallet.consume(DAY, 500)
    with pytest.raises(InsufficientWater):
        wallet.consume(DAY, 1)


def test_release_and_confirm(tmp_path):
    wallet = make_wallet(tmp_path, quota=1000)
    wallet.reserve(DAY, "t1", 300)
    wallet.release(DAY, "t1")
    assert wallet.balance(DAY)["available"] == 1000
    wallet.reserve(DAY, "t2", 300)
    wallet.confirm_sent(DAY, "t2")
    balance = wallet.balance(DAY)
    assert balance["sent"] == 300 and balance["reserved"] == 0 and balance["available"] == 700


def test_reserving_twice_is_idempotent(tmp_path):
    wallet = make_wallet(tmp_path, quota=1000)
    wallet.reserve(DAY, "t1", 300)
    wallet.reserve(DAY, "t1", 300)
    assert wallet.balance(DAY)["reserved"] == 300


def test_state_survives_restart(tmp_path):
    wallet = make_wallet(tmp_path, quota=1000)
    wallet.consume(DAY, 250)
    wallet.reserve(DAY, "t1", 100)
    restarted = Wallet("farm-a", Journal(tmp_path / "journal.jsonl"))
    assert restarted.balance(DAY) == wallet.balance(DAY)
    event = restarted.consume(DAY, 10)
    assert event["seq"] == 4


def test_partial_last_line_is_ignored(tmp_path):
    wallet = make_wallet(tmp_path, quota=1000)
    wallet.consume(DAY, 100)
    with open(tmp_path / "journal.jsonl", "a") as f:
        f.write('{"event_id": "x", "seq": 3, "type": "cons')   # simulated crash mid-write
    restarted = Wallet("farm-a", Journal(tmp_path / "journal.jsonl"))
    assert restarted.balance(DAY)["consumed"] == 100