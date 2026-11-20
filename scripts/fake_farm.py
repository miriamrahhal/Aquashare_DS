"""Pretend to be a farm and send fake consumption reports in its name.

  python scripts/fake_farm.py --farm farm-a --amount 500

Sends two reports to the gateway: one with no signature, one signed with a brand-new
"attacker" key. Once the farm has registered its own key, the ledger must refuse both.
"""
import argparse
import sys
import uuid
from pathlib import Path

import httpx  # noqa: F401  (kept for its exception classes)
import os as _os
import sys as _sys

_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from shared import tls  # noqa: E402

tls.for_host()          # follow the project's .env: use mutual TLS (as the operator) when it is on
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared import signing  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--farm", default="farm-a")
    p.add_argument("--amount", type=int, default=500)
    p.add_argument("--gateway", default="http://localhost:8600")
    a = p.parse_args()

    day = tls.get(f"{a.gateway}/state").json()["day"]
    attacker = Ed25519PrivateKey.generate()
    for i, (label, key) in enumerate((("no signature", None), ("signed with the attacker's own key", attacker))):
        events = [{"event_id": str(uuid.uuid4()), "seq": 900_000_000 + i, "amount_l": a.amount}]
        body = {"day": day, "events": events, "cmd_id": str(uuid.uuid4())}
        if key:
            body["signature"] = signing.sign(key, {"type": "record_consumption", "farm_id": a.farm,
                                                   "day": day, "events": events})
        r = tls.post(f"{a.gateway}/farms/{a.farm}/consumption", json=body, timeout=30)
        answer = r.json().get("detail", r.json())
        print(f"Fake report ({label}), {a.amount} L for {a.farm}:")
        print(f"   HTTP {r.status_code}  {'REFUSED' if r.status_code != 200 else 'ACCEPTED'}  {answer}")


if __name__ == "__main__":
    main()
