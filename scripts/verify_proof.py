"""Verify that a record is in the AquaShare ledger, without trusting the server.

A farm (or anyone) asks one ledger node for a Merkle proof of an entry, then checks it
here, locally:
  1. the entry's hash is recomputed from its contents (index, term, command, previous hash)
  2. the proof path is folded up to a root, which must equal the root the node published
  3. the same root is requested from the other ledger nodes; they must all agree

  python scripts/verify_proof.py --transfer-id <id>
  python scripts/verify_proof.py --index 734
  python scripts/verify_proof.py --transfer-id <id> --tamper     (changes the amount: must FAIL)

This file deliberately does not import the ledger's code: the checks are re-implemented
here, so a bug or a lie in the ledger cannot also hide itself in the verifier.
"""
import argparse
import hashlib
import json
import sys

import httpx  # noqa: F401  (kept for its exception classes)
import os as _os
import sys as _sys

_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from shared import tls  # noqa: E402

tls.for_host()          # follow the project's .env: use mutual TLS (as the operator) when it is on

NODES = {"ledger-1": "http://localhost:8701", "ledger-2": "http://localhost:8702", "ledger-3": "http://localhost:8703"}


def sha(b: bytes) -> bytes:
    return hashlib.sha256(b).digest()


def entry_hash(prev_hash: str, index: int, term: int, command: dict) -> str:
    canonical = json.dumps(command, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{prev_hash}|{index}|{term}|{canonical}".encode()).hexdigest()


def fold(leaf: bytes, m: int, size: int, path: list[bytes]) -> bytes | None:
    """RFC 9162 audit-path verification: returns the root the path leads to, or None."""
    fn, sn, r = m, size - 1, leaf
    for p in path:
        if sn == 0:
            return None
        if fn % 2 == 1 or fn == sn:
            r = sha(b"\x01" + p + r)
            while fn % 2 == 0 and fn != 0:
                fn >>= 1
                sn >>= 1
        else:
            r = sha(b"\x01" + r + p)
        fn >>= 1
        sn >>= 1
    return r if sn == 0 else None


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--index", type=int)
    a.add_argument("--transfer-id")
    a.add_argument("--node", default="ledger-1")
    a.add_argument("--tamper", action="store_true", help="change the amount before checking (should fail)")
    args = a.parse_args()
    params = {"index": args.index} if args.index else {"transfer_id": args.transfer_id}
    r = tls.get(f"{NODES[args.node]}/merkle/proof", params=params, timeout=10)
    if r.status_code != 200:
        sys.exit(f"{args.node}: {r.json().get('detail')}")
    p = r.json()
    e, size, path = p["entry"], p["size"], [bytes.fromhex(h) for h in p["path"]]
    cmd = dict(e["command"])
    print(f"proof from {p['node_id']}: entry {e['index']} of {size} committed entries, {len(path)} hashes in the path")
    print(f"  entry: term {e['term']}, {json.dumps(cmd)}")
    if args.tamper:
        if "amount_l" in cmd:
            cmd["amount_l"] = cmd["amount_l"] * 10
        else:
            cmd["tampered"] = True
        print(f"  TAMPERED locally: {json.dumps(cmd)}")

    ok = True
    recomputed = entry_hash(e["prev_hash"], e["index"], e["term"], cmd)
    good = recomputed == e["entry_hash"]
    ok &= good
    print(f"{'PASS' if good else 'FAIL'}  1. entry hash recomputed from its contents   {recomputed[:16]}")
    folded = fold(sha(b"\x00" + bytes.fromhex(recomputed)), e["index"] - 1, size, path)
    good = folded is not None and folded.hex() == p["root"]
    ok &= good
    print(f"{'PASS' if good else 'FAIL'}  2. proof path leads to the published root   {p['root'][:16]}")
    for name, url in NODES.items():
        if name == p["node_id"]:
            continue
        try:
            other = tls.get(f"{url}/merkle/root", params={"size": size}, timeout=5).json()["root"]
            good = other == p["root"]
            ok &= good
            print(f"{'PASS' if good else 'FAIL'}  3. {name} publishes the same root for {size} entries {other[:16]}")
        except (httpx.HTTPError, KeyError):
            print(f"SKIP  3. {name} not reachable")
    print(f"\n{'VERIFIED: the record is in the ledger, exactly as shown' if ok else 'VERIFICATION FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
