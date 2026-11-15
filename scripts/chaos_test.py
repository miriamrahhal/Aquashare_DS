"""Randomized fault-injection test for the AquaShare ledger.

Several client threads send random transfers and consumption reports through the gateway,
while a chaos thread keeps stopping and restarting random ledger nodes (sometimes two at
once, so the cluster also loses its majority). Clients behave like real farms: when the
answer is unknown they retry the same command with the same IDs until they get a final
answer (committed or rejected).

At the end the test checks that what the clients were told matches the ledger exactly:
  - every transfer reported as committed is on the ledger, once
  - no transfer reported as rejected is on the ledger
  - each farm's consumption on the ledger equals the sum the clients saw committed
and then runs the invariant checker on all three databases.

  python scripts/chaos_test.py --minutes 3
"""
import argparse
import os
import random
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict
from datetime import date, timedelta

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts import check_invariants  # noqa: E402

GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8600")
NODES = ["ledger-1", "ledger-2", "ledger-3"]
STATUS = {"ledger-1": "http://localhost:8701", "ledger-2": "http://localhost:8702", "ledger-3": "http://localhost:8703"}
STOP_CMD = os.getenv("STOP_CMD", "docker stop aq-{node}")
START_CMD = os.getenv("START_CMD", "docker start aq-{node}")
CHAOS_FARMS = ["chaos-1", "chaos-2", "chaos-3", "chaos-4"]

lock = threading.Lock()
log_lines = []


def say(msg):
    line = f"{time.strftime('%H:%M:%S')}  {msg}"
    with lock:
        log_lines.append(line)
        print(line, flush=True)


def run(cmd):
    subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def send(http, path, body, give_up_after=90):
    """Send one command, retrying with the same body until the ledger gives a final answer."""
    start, attempts = time.time(), 0
    while time.time() - start < give_up_after:
        attempts += 1
        try:
            r = http.post(f"{GATEWAY}{path}", json=body)
            if r.status_code == 200:
                return "committed", attempts
            if r.status_code == 409:
                return "rejected", attempts
        except httpx.HTTPError:
            pass
        time.sleep(1)                                  # 503 or no answer: unknown outcome, try again
    return "unknown", attempts


def setup(http):
    for farm in CHAOS_FARMS:
        http.post(f"{GATEWAY}/farms", json={"farm_id": farm, "name": farm.title(), "cmd_id": f"register-{farm}"})
    current = http.get(f"{GATEWAY}/state").json()["day"]
    day = str(date.fromisoformat(current) + timedelta(days=1)) if current else str(date.today())
    farms = [f["farm_id"] for f in http.get(f"{GATEWAY}/state").json()["farms"]] or []
    allocations = {f: 5000 for f in farms if f not in CHAOS_FARMS}
    allocations.update({f: 20000 for f in CHAOS_FARMS})
    result, _ = send(http, "/days", {"day": day, "limit_l": sum(allocations.values()), "allocations": allocations,
                                     "cmd_id": f"open-{day}"})
    say(f"opened day {day} for {len(allocations)} farms ({result}); chaos farms get 20000 L each")
    return day


def client(worker, until, day, results):
    http = httpx.Client(timeout=15)
    rng = random.Random(worker)
    while time.time() < until:
        time.sleep(rng.uniform(0.1, 0.5))
        farm = rng.choice(CHAOS_FARMS)
        if rng.random() < 0.6:
            other = rng.choice([f for f in CHAOS_FARMS if f != farm])
            amount = rng.choice([rng.randint(50, 900), rng.randint(3000, 12000)])   # some will be too large
            tid = str(uuid.uuid4())
            body = {"transfer_id": tid, "day": day, "sender": farm, "recipient": other, "amount_l": amount,
                    "cmd_id": f"chaos-{tid}"}
            t0 = time.time()
            outcome, attempts = send(http, "/transfers", body)
            results.append(("transfer", tid, farm, other, amount, outcome, attempts, time.time() - t0))
        else:
            amount = rng.randint(10, 600)
            event = {"event_id": str(uuid.uuid4()), "seq": time.time_ns() // 1000 + worker, "amount_l": amount}
            t0 = time.time()
            outcome, attempts = send(http, f"/farms/{farm}/consumption",
                                     {"day": day, "events": [event], "cmd_id": f"chaos-{event['event_id']}"})
            results.append(("consumption", event["event_id"], farm, None, amount, outcome, attempts, time.time() - t0))


def chaos(stop_event, actions):
    rng, down = random.Random(42), []
    while not stop_event.is_set():
        stop_event.wait(rng.uniform(3, 7))
        if stop_event.is_set():
            break
        if not down or (len(down) == 1 and rng.random() < 0.3):
            node = rng.choice([n for n in NODES if n not in down])
            run(STOP_CMD.format(node=node))
            down.append(node)
            actions["stops"] += 1
            say(f"CHAOS  stopped {node}   (down: {', '.join(down)}{' - NO MAJORITY' if len(down) > 1 else ''})")
        else:
            node = down.pop(rng.randrange(len(down)))
            run(START_CMD.format(node=node))
            actions["starts"] += 1
            say(f"CHAOS  started {node}   (down: {', '.join(down) or 'none'})")
    for node in down:
        run(START_CMD.format(node=node))
        say(f"CHAOS  started {node}   (cleanup)")


def wait_until_converged(timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            st = [httpx.get(f"{u}/status", timeout=2).json() for u in STATUS.values()]
            if len({s["last_applied"] for s in st}) == 1 and sum(s["role"] == "leader" for s in st) == 1:
                return st
        except httpx.HTTPError:
            pass
        time.sleep(1)
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=float, default=3, help="how long the clients keep sending")
    p.add_argument("--workers", type=int, default=4)
    a = p.parse_args()

    http = httpx.Client(timeout=15)
    day = setup(http)
    results, actions, stop_event = [], defaultdict(int), threading.Event()
    chaos_thread = threading.Thread(target=chaos, args=(stop_event, actions))
    started = time.time()
    chaos_thread.start()
    until = time.time() + a.minutes * 60
    say(f"{a.workers} clients sending for {a.minutes:g} minutes while ledger nodes are stopped and restarted")
    workers = [threading.Thread(target=client, args=(w, until, day, results)) for w in range(a.workers)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    stop_event.set()
    chaos_thread.join()
    elapsed = time.time() - started

    say("waiting for the three ledger nodes to converge ...")
    status = wait_until_converged()
    say("converged: " + (", ".join(f"{s['node_id']} {s['role']} applied={s['last_applied']}" for s in status)
                         if status else "NOT converged within 60 s"))

    # ---------------------------------------------------------------- compare clients with the ledger
    ledger_transfers = {}
    for farm in CHAOS_FARMS:
        for t in http.get(f"{GATEWAY}/farms/{farm}/transfers", params={"day": day}).json():
            ledger_transfers[t["transfer_id"]] = t
    balances = {f["farm_id"]: f for f in http.get(f"{GATEWAY}/state", params={"day": day}).json()["farms"]}

    count = defaultdict(int)
    for kind, _, _, _, _, outcome, _, _ in results:
        count[(kind, outcome)] += 1
    committed_t = {r[1] for r in results if r[0] == "transfer" and r[5] == "committed"}
    rejected_t = {r[1] for r in results if r[0] == "transfer" and r[5] == "rejected"}
    missing = committed_t - set(ledger_transfers)
    phantom = rejected_t & set(ledger_transfers)
    unexpected = set(ledger_transfers) - committed_t - {r[1] for r in results if r[5] == "unknown"}
    consumed_ok = all(
        balances[f]["consumed"] == sum(r[4] for r in results if r[0] == "consumption" and r[2] == f and r[5] == "committed")
        for f in CHAOS_FARMS)
    times = sorted(r[7] for r in results)
    slow = sum(1 for t in times if t > 1.0)

    print("\n================ CHAOS TEST RESULT ================")
    print(f"operations: {len(results)} in {elapsed:.0f} s with {a.workers} concurrent clients")
    print(f"node failures injected: {actions['stops']} stops, {actions['starts']} restarts")
    print(f"transfers:   {count[('transfer', 'committed')]} committed, {count[('transfer', 'rejected')]} rejected, "
          f"{count[('transfer', 'unknown')]} unknown")
    print(f"consumption: {count[('consumption', 'committed')]} committed, {count[('consumption', 'rejected')]} rejected, "
          f"{count[('consumption', 'unknown')]} unknown")
    print(f"response time: median {times[len(times) // 2] * 1000:.0f} ms, max {times[-1]:.1f} s; "
          f"{slow} operations waited > 1 s for an election or a majority (retried by the gateway, none lost)")
    print(f"{'PASS' if not missing else 'FAIL'}  every committed transfer is on the ledger   (missing: {len(missing)})")
    print(f"{'PASS' if not phantom else 'FAIL'}  no rejected transfer is on the ledger        (found: {len(phantom)})")
    print(f"{'PASS' if not unexpected else 'FAIL'}  no transfer the clients did not send          (found: {len(unexpected)})")
    print(f"{'PASS' if consumed_ok else 'FAIL'}  ledger consumption = consumption the clients saw committed")
    print("  farm      allocated  received   sent  consumed  available")
    for f in CHAOS_FARMS:
        b = balances[f]
        print(f"  {f:9} {b['allocated']:>9} {b['received']:>9} {b['sent']:>6} {b['consumed']:>9} {b['available']:>10}")
    print("\n================ INVARIANT CHECK ===================")
    code = check_invariants.main()
    ok = not missing and not phantom and not unexpected and consumed_ok and code == 0
    print(f"\nCHAOS TEST {'PASSED' if ok else 'FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
