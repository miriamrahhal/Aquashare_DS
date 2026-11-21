"""Register a farm's new key after the farm lost its data (operator only).

  python scripts/replace_farm_key.py --farm farm-a

A farm that loses its data creates a new key when it restarts. The ledger still holds its
old key and refuses to let the farm swap it by itself, so all its reports are refused.
This script asks the farm for its new public key and has the ledger replace the old one.
It runs with the operator certificate: a farm's own certificate is refused (403).
"""
import argparse
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared import tls  # noqa: E402

tls.for_host()
FARM_PORTS = {"farm-a": 8801, "farm-b": 8802, "farm-c": 8803, "farm-d": 8804}


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--farm", required=True, choices=FARM_PORTS)
    a.add_argument("--gateway", default="http://localhost:8600")
    args = a.parse_args()
    info = tls.get(f"http://localhost:{FARM_PORTS[args.farm]}/key", timeout=5).json()
    if not info.get("public_key"):
        sys.exit(f"{args.farm} does not sign its reports, so it has no key to register")
    print(f"{args.farm}'s current public key: {info['public_key']}")
    r = tls.post(f"{args.gateway}/farms/{args.farm}/key/replace",
                 json={"public_key": info["public_key"], "cmd_id": f"replace-key-{uuid.uuid4()}"}, timeout=15)
    print(f"HTTP {r.status_code}  {r.text[:300]}")
    if r.status_code == 200:
        print(f"Done: within a few seconds {args.farm} registers again and its reports are accepted.")


if __name__ == "__main__":
    main()
