"""Sends a steady stream of commands through the gateway and reports every result.

Run it, then stop the ledger leader while it is running:
  python scripts/load_test.py --url http://localhost:8600 --farm farm-c --count 60
Each request records 1 L of consumption for the farm, with a fresh event ID.
"""
import argparse
import time
import uuid

import httpx


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8600")
    p.add_argument("--farm", default="farm-c")
    p.add_argument("--count", type=int, default=60)
    p.add_argument("--interval", type=float, default=0.25)
    a = p.parse_args()

    http = httpx.Client(timeout=20)
    day = http.get(f"{a.url}/state").json()["day"]
    before = http.get(f"{a.url}/farms/{a.farm}/balance").json()["consumed"]
    ok = failed = 0
    latencies = []
    print(f"sending {a.count} commands for {a.farm} on {day} through {a.url}")
    for i in range(1, a.count + 1):
        event = {"event_id": str(uuid.uuid4()), "seq": time.time_ns() // 1000, "amount_l": 1}
        start = time.perf_counter()
        try:
            r = http.post(f"{a.url}/farms/{a.farm}/consumption", json={"day": day, "events": [event], "cmd_id": str(uuid.uuid4())})
            ms = (time.perf_counter() - start) * 1000
            if r.status_code == 200:
                b = r.json()
                ok += 1
                latencies.append(ms)
                note = "  <-- retried" if b["attempts"] > 1 else ""
                print(f"{i:3}  OK   entry {b['committed_index']:>4}  leader {b['leader_id']}  "
                      f"{b['gateway']}  attempts {b['attempts']}  {ms:7.0f} ms{note}")
            else:
                failed += 1
                print(f"{i:3}  FAIL {r.status_code} {r.text[:80]}")
        except httpx.HTTPError as exc:
            failed += 1
            print(f"{i:3}  FAIL {type(exc).__name__}")
        time.sleep(a.interval)
    after = http.get(f"{a.url}/farms/{a.farm}/balance").json()["consumed"]
    latencies.sort()
    print(f"\n{ok} succeeded, {failed} failed")
    print(f"latency: median {latencies[len(latencies) // 2]:.0f} ms, max {latencies[-1]:.0f} ms")
    print(f"{a.farm} consumed before {before} L, after {after} L, difference {after - before} L "
          f"({'matches' if after - before == ok else 'DOES NOT match'} the {ok} successful commands)")


if __name__ == "__main__":
    main()