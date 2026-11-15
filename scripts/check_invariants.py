"""Invariant checker: verifies AquaShare's correctness rules on all three ledger databases.

Run it at any time (best when the system is quiet):  python scripts/check_invariants.py
Each check prints PASS or FAIL. The exit code is 0 only if every check passes.

Rules checked on every node:
  I1  the hash chain of the Raft log is unbroken
  I2  for every day, the allocations add up to at most the daily limit
  I3  no farm ever used more than allocated + received - sent
  I4  transfers neither create nor destroy water: total sent = total received = committed transfers
  I5  each farm's balance matches the detailed records (allocations, consumption records, transfers)
Rules checked across nodes:
  I6  all nodes hold the same entry hash at the highest index they have all applied
  I7  all nodes hold exactly the same balances
"""
import os
import sys
import time

import psycopg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ledger_node.storage import GENESIS_HASH, chain_hash  # noqa: E402

NODES = {f"ledger-{i}": f"postgresql://aquashare:aquashare_dev@localhost:{5440 + i}/ledger" for i in (1, 2, 3)}
if os.getenv("LEDGER_DSNS"):                     # optional override: comma-separated connection strings
    NODES = {f"ledger-{i}": d for i, d in enumerate(os.environ["LEDGER_DSNS"].split(","), start=1)}


def q(conn, sql, *params):
    return conn.execute(sql, params).fetchall()


def check_node(conn) -> list[tuple[str, bool, str]]:
    """The single-node invariants I1-I5. Returns (name, passed, detail) for each."""
    results = []

    prev, broken = GENESIS_HASH, None
    rows = q(conn, "SELECT log_index, term, command::text, prev_hash, entry_hash FROM raft_log ORDER BY log_index")
    for index, term, command, prev_hash, entry_hash in rows:
        if prev_hash != prev or chain_hash(prev, index, term, command) != entry_hash:
            broken = index
            break
        prev = entry_hash
    results.append(("I1 hash chain unbroken", broken is None,
                    f"{len(rows)} entries verified" if broken is None else f"broken at entry {broken}"))

    bad = q(conn, "SELECT d.day::text, d.limit_l, coalesce(sum(a.amount_l), 0) FROM daily_limits d "
                  "LEFT JOIN allocations a ON a.day = d.day GROUP BY d.day, d.limit_l "
                  "HAVING coalesce(sum(a.amount_l), 0) > d.limit_l")
    days = q(conn, "SELECT count(*) FROM daily_limits")[0][0]
    results.append(("I2 allocations <= daily limit", not bad, f"{days} days checked" if not bad else f"violated: {bad}"))

    bad = q(conn, "SELECT day::text, farm_id FROM farm_balances WHERE consumed > allocated + received - sent "
                  "OR consumed < 0 OR sent < 0 OR received < 0")
    rows_n = q(conn, "SELECT count(*) FROM farm_balances")[0][0]
    results.append(("I3 no farm over its water", not bad, f"{rows_n} farm-days checked" if not bad else f"violated: {bad}"))

    bad = q(conn, "SELECT b.day::text, b.sent, b.received, coalesce(t.total, 0) FROM "
                  "(SELECT day, sum(sent) sent, sum(received) received FROM farm_balances GROUP BY day) b "
                  "LEFT JOIN (SELECT day, sum(amount_l) total FROM transfers WHERE status = 'committed' GROUP BY day) t "
                  "ON t.day = b.day WHERE b.sent <> b.received OR b.sent <> coalesce(t.total, 0)")
    total = q(conn, "SELECT coalesce(sum(amount_l), 0), count(*) FROM transfers WHERE status = 'committed'")[0]
    results.append(("I4 transfers conserve water", not bad,
                    f"{total[1]} transfers, {total[0]} L moved" if not bad else f"violated: {bad}"))

    bad = q(conn, """
        SELECT b.day::text, b.farm_id FROM farm_balances b
        LEFT JOIN allocations a ON a.day = b.day AND a.farm_id = b.farm_id
        LEFT JOIN (SELECT day, farm_id, sum(amount_l) s FROM consumption_records GROUP BY day, farm_id) c
               ON c.day = b.day AND c.farm_id = b.farm_id
        LEFT JOIN (SELECT day, sender, sum(amount_l) s FROM transfers WHERE status = 'committed' GROUP BY day, sender) o
               ON o.day = b.day AND o.sender = b.farm_id
        LEFT JOIN (SELECT day, recipient, sum(amount_l) s FROM transfers WHERE status = 'committed' GROUP BY day, recipient) i
               ON i.day = b.day AND i.recipient = b.farm_id
        WHERE b.allocated <> coalesce(a.amount_l, 0) OR b.consumed <> coalesce(c.s, 0)
           OR b.sent <> coalesce(o.s, 0) OR b.received <> coalesce(i.s, 0)""")
    records = q(conn, "SELECT count(*) FROM consumption_records")[0][0]
    results.append(("I5 balances match the records", not bad,
                    f"{records} consumption records" if not bad else f"mismatch: {bad[:5]}"))
    return results


def snapshot(conn):
    applied = q(conn, "SELECT last_applied FROM raft_metadata WHERE id = 1")[0][0]
    balances = q(conn, "SELECT day::text, farm_id, allocated, received, sent, consumed FROM farm_balances "
                       "ORDER BY day, farm_id")
    return applied, balances


def check_cluster(conns: dict, attempts: int = 5) -> list[tuple[str, bool, str]]:
    """The cross-node invariants I6-I7. Retries briefly, since followers may be a heartbeat behind."""
    for attempt in range(attempts):
        snaps = {n: snapshot(c) for n, c in conns.items()}
        common = min(applied for applied, _ in snaps.values())
        hashes = {n: (q(c, "SELECT entry_hash FROM raft_log WHERE log_index = %s", common) or [[None]])[0][0]
                  for n, c in conns.items()}
        same_hash = len(set(hashes.values())) == 1
        same_balances = len({tuple(b) for _, b in snaps.values()}) == 1
        if same_hash and same_balances:
            break
        time.sleep(1)
    applied = ", ".join(f"{n}={a}" for n, (a, _) in snaps.items())
    return [("I6 same log on every node", same_hash, f"entry {common}: hash {next(iter(hashes.values()))[:16]}"
             if same_hash else f"entry {common}: {hashes}"),
            ("I7 same balances on every node", same_balances, f"last applied {applied}")]


def main() -> int:
    conns, ok = {}, True
    for name, dsn in NODES.items():
        try:
            conns[name] = psycopg.connect(dsn, autocommit=True, connect_timeout=3)
        except psycopg.OperationalError:
            print(f"{name}: database not reachable")
            ok = False
    for name, conn in conns.items():
        print(f"\n{name}")
        for check, passed, detail in check_node(conn):
            print(f"  {'PASS' if passed else 'FAIL'}  {check:34} {detail}")
            ok &= passed
    if len(conns) > 1:
        print("\ncluster")
        for check, passed, detail in check_cluster(conns):
            print(f"  {'PASS' if passed else 'FAIL'}  {check:34} {detail}")
            ok &= passed
    print(f"\n{'ALL INVARIANTS HOLD' if ok else 'INVARIANT VIOLATED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
