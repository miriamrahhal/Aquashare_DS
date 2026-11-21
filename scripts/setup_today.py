"""Prepare a fresh ledger for a demo: register the 4 farms and open today's day.

Use it after wiping the data (docker compose down -v, then docker compose up -d --build):

  python scripts/setup_today.py
"""
import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared import tls  # noqa: E402

tls.for_host()                       # acts as the operator (with mutual TLS when .env turns it on)
GATEWAY = "http://localhost:8600"
SHARES = {"farm-a": 6000, "farm-b": 5000, "farm-c": 4500, "farm-d": 4000}


def post(path, body):
    for _ in range(30):              # the cluster may still be electing its first leader
        try:
            r = tls.post(GATEWAY + path, json=body, timeout=10)
            if r.status_code != 503:
                return r
        except Exception:
            pass
        time.sleep(2)
    sys.exit(f"the gateway did not answer {path}: is the system running?")


for farm in SHARES:
    r = post("/farms", {"farm_id": farm, "name": farm.title(), "cmd_id": f"register-{farm}"})
    print(f"register {farm}: HTTP {r.status_code}")

today = dt.date.today().isoformat()
r = post("/days", {"day": today, "limit_l": 20000, "allocations": SHARES, "cmd_id": f"day-{today}"})
print(f"open day {today}: HTTP {r.status_code} {r.text[:120]}")
