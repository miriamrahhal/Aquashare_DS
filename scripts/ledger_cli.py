"""Small command-line client for the AquaShare ledger.

Examples:
  python scripts/ledger_cli.py register farm-a "North Farm"
  python scripts/ledger_cli.py open-day 2026-06-01 10000 farm-a=4000 farm-b=3000
  python scripts/ledger_cli.py consume farm-a 2026-06-01 500 700
  python scripts/ledger_cli.py transfer farm-a farm-b 2026-06-01 1000
  python scripts/ledger_cli.py state
Add --port 8702 to talk to another node, and --cmd-id X to send a command with a fixed ID (to test retries).
"""
import argparse
import json
import time
import uuid

import httpx  # noqa: F401  (kept for its exception classes)
import os as _os
import sys as _sys

_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from shared import tls  # noqa: E402

tls.for_host()          # follow the project's .env: use mutual TLS (as the operator) when it is on


def send(port, command, cmd_id=None):
    command["cmd_id"] = cmd_id or str(uuid.uuid4())
    reply = tls.post(f"http://localhost:{port}/command", json={"command": command}, timeout=10)
    body = reply.json()
    if reply.status_code != 200:
        print(f"REFUSED ({reply.status_code}): {body.get('detail')}")
        return
    result = body["result"]
    status = "OK      " if result.get("ok") else "REJECTED"
    via = f" via {body['forwarded_by']}" if "forwarded_by" in body else ""
    print(f"{status} entry {body['committed_index']} (leader {body['leader_id']}{via}): {json.dumps(result)}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8701)
    p.add_argument("--cmd-id")
    sub = p.add_subparsers(dest="action", required=True)
    r = sub.add_parser("register"); r.add_argument("farm_id"); r.add_argument("name")
    o = sub.add_parser("open-day"); o.add_argument("day"); o.add_argument("limit_l", type=int); o.add_argument("allocations", nargs="*")
    c = sub.add_parser("consume"); c.add_argument("farm_id"); c.add_argument("day"); c.add_argument("amounts", type=int, nargs="+")
    t = sub.add_parser("transfer"); t.add_argument("sender"); t.add_argument("recipient"); t.add_argument("day"); t.add_argument("amount_l", type=int)
    s = sub.add_parser("state"); s.add_argument("--day")
    a = p.parse_args()

    if a.action == "register":
        send(a.port, {"type": "register_farm", "farm_id": a.farm_id, "name": a.name}, a.cmd_id)
    elif a.action == "open-day":
        allocations = {k: int(v) for k, v in (x.split("=") for x in a.allocations)}
        send(a.port, {"type": "open_day", "day": a.day, "limit_l": a.limit_l, "allocations": allocations}, a.cmd_id)
    elif a.action == "consume":
        # real farm nodes number events from their journal; this test client numbers them by time
        base = time.time_ns() // 1000
        events = [{"event_id": str(uuid.uuid4()), "seq": base + i, "amount_l": amt}
                  for i, amt in enumerate(a.amounts)]
        send(a.port, {"type": "record_consumption", "farm_id": a.farm_id, "day": a.day, "events": events}, a.cmd_id)
    elif a.action == "transfer":
        send(a.port, {"type": "transfer", "transfer_id": str(uuid.uuid5(uuid.NAMESPACE_URL, a.cmd_id)) if a.cmd_id
                      else str(uuid.uuid4()), "day": a.day, "sender": a.sender, "recipient": a.recipient,
                      "amount_l": a.amount_l}, a.cmd_id)
    else:
        params = {"day": a.day} if a.day else {}
        st = tls.get(f"http://localhost:{a.port}/state", params=params, timeout=5).json()
        print(f"{st['node_id']}  day {st['day']}  limit {st.get('limit_l')} L  "
              f"allocated {st.get('total_allocated')} L  consumed {st.get('total_consumed')} L  (last applied entry {st['last_applied']})")
        print(f"  {'farm':8} {'allocated':>10} {'received':>9} {'sent':>6} {'consumed':>9} {'available':>10}")
        for f in st["farms"]:
            print(f"  {f['farm_id']:8} {f['allocated']:>10} {f['received']:>9} {f['sent']:>6} {f['consumed']:>9} {f['available']:>10}")


if __name__ == "__main__":
    main()