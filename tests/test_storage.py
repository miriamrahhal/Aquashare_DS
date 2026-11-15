"""PostgresStorage tests. They need a PostgreSQL server; by default they use the
database container of ledger node 1 (localhost:5441) and work in a separate
database called ledger_test, so the real ledger data is never touched."""
import os
from pathlib import Path

import psycopg
import pytest

from ledger_node.storage import (GENESIS_HASH, Entry, HashMismatch, MemoryStorage,
                                 PostgresStorage, chain_hash)

ADMIN_DSN = os.getenv("TEST_PG_ADMIN_DSN", "postgresql://aquashare:aquashare_dev@localhost:5441/postgres")
TEST_DSN = ADMIN_DSN.rsplit("/", 1)[0] + "/ledger_test"
SCHEMA = Path(__file__).resolve().parent.parent / "ledger_node" / "schema.sql"


@pytest.fixture
def dsn():
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL is not reachable; start aq-ledger-db-1 first")
    admin.execute("DROP DATABASE IF EXISTS ledger_test WITH (FORCE)")
    admin.execute("CREATE DATABASE ledger_test")
    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        conn.execute(SCHEMA.read_text())
    yield TEST_DSN
    admin.execute("DROP DATABASE IF EXISTS ledger_test WITH (FORCE)")
    admin.close()


def cmd(n):
    return f'{{"type": "test", "n": {n}}}'


def test_empty_database_starts_at_term_zero(dsn):
    s = PostgresStorage(dsn)
    assert s.load_term_vote() == (0, None)
    assert s.last_index() == 0 and s.last_hash() == GENESIS_HASH


def test_term_and_vote_survive_restart(dsn):
    PostgresStorage(dsn).save_term_vote(7, "ledger-2")
    assert PostgresStorage(dsn).load_term_vote() == (7, "ledger-2")


def test_log_survives_restart(dsn):
    s = PostgresStorage(dsn)
    s.append([Entry(1, 1, cmd(1)), Entry(2, 1, cmd(2))])
    s.append([Entry(3, 2, cmd(3))])
    again = PostgresStorage(dsn)                 # a "restarted" node reads its log back
    assert again.last_index() == 3 and again.last_term() == 2
    assert [e.term for e in again.entries_from(1)] == [1, 1, 2]
    assert again.last_hash() == s.last_hash()


def test_truncate_survives_restart(dsn):
    s = PostgresStorage(dsn)
    s.append([Entry(i, 1, cmd(i)) for i in range(1, 6)])
    s.truncate_from(3)
    again = PostgresStorage(dsn)
    assert again.last_index() == 2
    again.append([Entry(3, 2, cmd(30))])         # the chain continues correctly after a truncate
    assert PostgresStorage(dsn).last_index() == 3


def test_hash_chain_is_identical_on_every_node(dsn):
    pg, mem = PostgresStorage(dsn), MemoryStorage()
    entries = [Entry(i, 1, cmd(i)) for i in range(1, 11)]
    pg.append(entries)
    mem.append(entries)
    assert pg.last_hash() == mem.last_hash()     # same log -> same last hash
    first = pg.entry(1)
    assert first.prev_hash == GENESIS_HASH
    assert first.entry_hash == chain_hash(GENESIS_HASH, 1, 1, cmd(1))
    assert pg.entry(2).prev_hash == first.entry_hash


def test_entry_with_wrong_hash_is_rejected(dsn):
    s = PostgresStorage(dsn)
    with pytest.raises(HashMismatch):
        s.append([Entry(1, 1, cmd(1), entry_hash="f" * 64)])
    assert PostgresStorage(dsn).last_index() == 0   # nothing was written


def test_edited_log_is_detected_on_start(dsn):
    PostgresStorage(dsn).append([Entry(i, 1, cmd(i)) for i in range(1, 4)])
    with psycopg.connect(dsn, autocommit=True) as conn:   # someone changes an entry directly in the database
        conn.execute("""UPDATE raft_log SET command = '{"type": "test", "n": 999}' WHERE log_index = 2""")
    with pytest.raises(HashMismatch):
        PostgresStorage(dsn)