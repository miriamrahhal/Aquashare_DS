"""Performance and scalability benchmark for AquaShare.

Three experiments, run on the live system:
  A. load scaling     - 1, 2, 4, 8, 16 concurrent clients through the load balancer:
                        throughput (committed operations per second) and response times
  B. gateway layer    - the same load sent straight to the leader, through one gateway,
                        and through the load balancer with two gateways
  C. failover time    - the leader is stopped several times; we measure how long until the
                        cluster commits again with a new leader

Every operation is a real ledger command (a 1 L consumption report on a benchmark farm),
committed by a majority and applied to PostgreSQL on all three nodes.

  pip install matplotlib
  python scripts/benchmark.py
Results: results/benchmark.csv, results/failover.csv and four PNG graphs in results/.
"""
import argparse
import csv
import itertools
import os
import random
import statistics
import subprocess
import threading
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import httpx

LB = os.getenv("GATEWAY_URL", "http://localhost:8600")
GATEWAY_1 = "http://localhost:8601"
NODES = {"ledger-1": "http://localhost:8701", "ledger-2": "http://localhost:8702", "ledger-3": "http://localhost:8703"}
STOP_CMD = os.getenv("STOP_CMD", "docker stop aq-{node}")
START_CMD = os.getenv("START_CMD", "docker start aq-{node}")
FARMS = ["bench-1", "bench-2", "bench-3", "bench-4"]
OUT = Path("results")
seq = itertools.count(time.time_ns() // 1000)
seq_lock = threading.Lock()


def leader():
    for node, url in NODES.items():
        try:
            s = httpx.get(f"{url}/status", timeout=1).json()
            if s["role"] == "leader":
                return node, s
        except httpx.HTTPError:
            pass
    return None, None


def setup(day_holder):
    http = httpx.Client(timeout=15)
    for f in FARMS:
        http.post(f"{LB}/farms", json={"farm_id": f, "name": f.title(), "cmd_id": f"register-{f}"})
    state = http.get(f"{LB}/state").json()
    day = str(date.fromisoformat(state["day"]) + timedelta(days=1)) if state["day"] else str(date.today())
    farms = [f["farm_id"] for f in state["farms"]]
    allocations = {f: 1_000_000_000 for f in FARMS}
    allocations.update({f: 5000 for f in farms if f not in FARMS})
    r = http.post(f"{LB}/days", json={"day": day, "limit_l": sum(allocations.values()), "allocations": allocations,
                                      "cmd_id": f"open-{day}"})
    print(f"benchmark day {day} opened ({r.status_code}); every operation records 1 L for a benchmark farm")
    day_holder.append(day)


def one_operation(http, base, day, path_style):
    with seq_lock:
        s = next(seq)
    farm = random.choice(FARMS)
    event = {"event_id": str(uuid.uuid4()), "seq": s, "amount_l": 1}
    if path_style == "ledger":                                   # straight to a ledger node
        body = {"command": {"type": "record_consumption", "farm_id": farm, "day": day, "events": [event],
                            "cmd_id": str(uuid.uuid4())}}
        return http.post(f"{base}/command", json=body)
    return http.post(f"{base}/farms/{farm}/consumption", json={"day": day, "events": [event]})


def run_load(base, clients, seconds, day, path_style="gateway"):
    latencies, errors, lock = [], [0], threading.Lock()
    stop_at = time.time() + seconds

    def worker():
        http = httpx.Client(timeout=20)
        while time.time() < stop_at:
            t0 = time.perf_counter()
            try:
                ok = one_operation(http, base, day, path_style).status_code == 200
            except httpx.HTTPError:
                ok = False
            dt = time.perf_counter() - t0
            with lock:
                (latencies.append(dt) if ok else errors.__setitem__(0, errors[0] + 1))

    threads = [threading.Thread(target=worker) for _ in range(clients)]
    started = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - started
    lat = sorted(latencies) or [0]
    pct = lambda p: lat[min(len(lat) - 1, int(p * len(lat)))] * 1000
    return {"clients": clients, "operations": len(latencies), "errors": errors[0],
            "throughput": round(len(latencies) / elapsed, 1), "p50_ms": round(pct(0.50), 1),
            "p95_ms": round(pct(0.95), 1), "p99_ms": round(pct(0.99), 1),
            "mean_ms": round(statistics.mean(lat) * 1000, 1)}


def failover_trial(trial):
    old, before = leader()
    if old is None:
        return None
    subprocess.run(STOP_CMD.format(node=old), shell=True, capture_output=True)
    stopped = time.time()
    others = [u for n, u in NODES.items() if n != old]
    while time.time() - stopped < 30:
        for url in others:
            try:
                r = httpx.post(f"{url}/command", json={"command": {"type": "test", "value": f"failover {trial}",
                                                                   "cmd_id": str(uuid.uuid4())}}, timeout=1.5)
                if r.status_code == 200:
                    took = time.time() - stopped
                    new = r.json()["leader_id"]
                    subprocess.run(START_CMD.format(node=old), shell=True, capture_output=True)
                    return {"trial": trial, "old_leader": old, "old_term": before["term"], "new_leader": new,
                            "new_term": httpx.get(f"{NODES[new]}/status", timeout=2).json()["term"],
                            "failover_s": round(took, 2)}
            except httpx.HTTPError:
                pass
        time.sleep(0.05)
    subprocess.run(START_CMD.format(node=old), shell=True, capture_output=True)
    return {"trial": trial, "old_leader": old, "failover_s": None}


def wait_for_cluster(timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            st = [httpx.get(f"{u}/status", timeout=1).json() for u in NODES.values()]
            if sum(s["role"] == "leader" for s in st) == 1 and len({s["commit_index"] for s in st}) == 1:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(1)
    return False


def plot(load_rows, layer_rows, failovers):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is not installed: CSV files written, graphs skipped (pip install matplotlib)")
        return
    blue, orange, grey = "#2563eb", "#ea580c", "#6b7280"
    clients = [r["clients"] for r in load_rows]

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(clients, [r["throughput"] for r in load_rows], marker="o", color=blue)
    ax.set(xlabel="concurrent clients", ylabel="committed operations per second",
           title="Throughput of the ledger (through the load balancer)")
    ax.set_ylim(bottom=0)
    ax.set_xscale("log", base=2); ax.set_xticks(clients, [str(c) for c in clients]); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(OUT / "throughput.png", dpi=150); plt.close(fig)

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(clients, [r["p50_ms"] for r in load_rows], marker="o", color=blue, label="median (p50)")
    ax.plot(clients, [r["p95_ms"] for r in load_rows], marker="s", color=orange, label="p95")
    ax.plot(clients, [r["p99_ms"] for r in load_rows], marker="^", color=grey, label="p99")
    ax.set(xlabel="concurrent clients", ylabel="response time (ms)", title="Response time under load")
    ax.set_ylim(bottom=0)
    ax.set_xscale("log", base=2); ax.set_xticks(clients, [str(c) for c in clients]); ax.grid(alpha=0.3); ax.legend()
    fig.tight_layout(); fig.savefig(OUT / "latency.png", dpi=150); plt.close(fig)

    if layer_rows:
        fig, ax1 = plt.subplots(figsize=(7, 4))
        names = [r["setup"] for r in layer_rows]
        ax1.bar(names, [r["throughput"] for r in layer_rows], color=blue)
        ax1.set(ylabel="committed operations per second",
                title=f"Cost of the gateway layer ({layer_rows[0]['clients']} clients)")
        for i, r in enumerate(layer_rows):
            ax1.text(i, r["throughput"], f"{r['throughput']} ops/s\np50 {r['p50_ms']} ms", ha="center", va="bottom")
        ax1.set_ylim(0, max(r["throughput"] for r in layer_rows) * 1.3); ax1.grid(axis="y", alpha=0.3)
        fig.tight_layout(); fig.savefig(OUT / "gateway_layer.png", dpi=150); plt.close(fig)

    ok = [f for f in failovers if f and f["failover_s"] is not None]
    if ok:
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.bar([f"trial {f['trial']}\n{f['old_leader']} -> {f['new_leader']}" for f in ok],
               [f["failover_s"] for f in ok], color=orange)
        mean = statistics.mean(f["failover_s"] for f in ok)
        ax.axhline(mean, color=grey, linestyle="--", label=f"mean {mean:.2f} s")
        ax.set(ylabel="seconds until a new leader commits", title="Leader failover time"); ax.legend()
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout(); fig.savefig(OUT / "failover.png", dpi=150); plt.close(fig)
    print(f"graphs written to {OUT.resolve()}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=int, default=15, help="duration of each load level")
    p.add_argument("--clients", default="1,2,4,8,16")
    p.add_argument("--layer-clients", type=int, default=8)
    p.add_argument("--failovers", type=int, default=5)
    a = p.parse_args()
    OUT.mkdir(exist_ok=True)
    days = []
    setup(days)
    day = days[0]

    print("\nA. LOAD SCALING (through the load balancer)")
    print(f"{'clients':>8} {'ops':>6} {'errors':>7} {'ops/s':>7} {'p50 ms':>7} {'p95 ms':>7} {'p99 ms':>7}")
    load_rows = []
    for c in [int(x) for x in a.clients.split(",")]:
        r = run_load(LB, c, a.seconds, day)
        load_rows.append(r)
        print(f"{r['clients']:>8} {r['operations']:>6} {r['errors']:>7} {r['throughput']:>7} "
              f"{r['p50_ms']:>7} {r['p95_ms']:>7} {r['p99_ms']:>7}")

    print(f"\nB. GATEWAY LAYER ({a.layer_clients} clients)")
    layer_rows = []
    node, _ = leader()
    for name, base, style in [("direct to leader", NODES[node], "ledger"), ("1 gateway", GATEWAY_1, "gateway"),
                              ("load balancer\n+ 2 gateways", LB, "gateway")]:
        try:
            httpx.get(f"{base}/health", timeout=2)
        except httpx.HTTPError:
            print(f"  {name}: not reachable, skipped")
            continue
        r = run_load(base, a.layer_clients, a.seconds, day, style)
        r["setup"] = name
        layer_rows.append(r)
        print(f"  {name.replace(chr(10), ' '):28} {r['throughput']:>7} ops/s   p50 {r['p50_ms']} ms   p95 {r['p95_ms']} ms")

    print(f"\nC. LEADER FAILOVER ({a.failovers} trials)")
    failovers = []
    for t in range(1, a.failovers + 1):
        wait_for_cluster()
        f = failover_trial(t)
        failovers.append(f)
        if f and f["failover_s"] is not None:
            print(f"  trial {t}: {f['old_leader']} (term {f['old_term']}) stopped -> {f['new_leader']} "
                  f"(term {f['new_term']}) committed after {f['failover_s']} s")
        else:
            print(f"  trial {t}: no new commit within 30 s")
    wait_for_cluster()
    ok = [f["failover_s"] for f in failovers if f and f["failover_s"] is not None]
    if ok:
        print(f"  failover: mean {statistics.mean(ok):.2f} s, min {min(ok):.2f} s, max {max(ok):.2f} s")

    with open(OUT / "benchmark.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["experiment", "setup", "clients", "operations", "errors", "throughput",
                                           "p50_ms", "p95_ms", "p99_ms", "mean_ms"])
        w.writeheader()
        for r in load_rows:
            w.writerow({"experiment": "load", "setup": "load balancer", **r})
        for r in layer_rows:
            w.writerow({"experiment": "gateway layer", **{**r, "setup": r["setup"].replace("\n", " ")}})
    with open(OUT / "failover.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["trial", "old_leader", "old_term", "new_leader", "new_term", "failover_s"])
        w.writeheader()
        for f in failovers:
            if f:
                w.writerow(f)
    plot(load_rows, layer_rows, failovers)


if __name__ == "__main__":
    main()
