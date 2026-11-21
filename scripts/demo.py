"""Terminal demo of AquaShare, one scenario per command (works with mutual TLS on or off).

  python scripts/demo.py status          the 3 ledger servers: who leads, term, commit
  python scripts/demo.py day             open the next day and show each farm's share
  python scripts/demo.py offline         farm-d loses its link, keeps watering, then syncs
  python scripts/demo.py duplicate       the same transfer sent twice is counted once
  python scripts/demo.py overspend       farm-a tries to send more water than it has
  python scripts/demo.py transfer        farm-a sends 100 L to farm-b (used during "lose the majority")
  python scripts/demo.py proof           Merkle proof of farm-a's latest transfer, then of a falsified copy
  python scripts/demo.py forbidden       farm-a's certificate tries to open a day -> 403
  python scripts/demo.py nocert          a request without any certificate -> refused
  python scripts/demo.py state           each farm's water today, and which ledger nodes agreed on it
  python scripts/demo.py lie             ledger-3's database is altered: farm-a gets 5000 L more on that node only
  python scripts/demo.py honest          undo the lie
"""
import datetime as dt
import os
import ssl
import sys
import time

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared import tls  # noqa: E402

tls.for_host()
GW = "http://localhost:8600"
LEDGERS = {"ledger-1": 8701, "ledger-2": 8702, "ledger-3": 8703}
FARMS = {"farm-a": 8801, "farm-b": 8802, "farm-c": 8803, "farm-d": 8804}
SHARES = {"farm-a": 6000, "farm-b": 5000, "farm-c": 4500, "farm-d": 4000}


def show(r):
    print(f"  HTTP {r.status_code}  {r.text[:300]}")
    return r


def state():
    s = tls.get(f"{GW}/state", timeout=10).json()
    print(f"  day {s.get('day')}   (answered by {s.get('gateway')})")
    if "agreed_by" in s:
        print(f"  agreed by: {', '.join(s['agreed_by'])}"
              + (f"   OUTVOTED: {', '.join(s['outvoted'])}" if s.get("outvoted") else "")
              + (f"   behind: {', '.join(s['behind'])}" if s.get("behind") else ""))
    print(f"  {'farm':8} {'share':>7} {'used':>7} {'sent':>6} {'recv':>6} {'left':>7}  signs")
    for f in s.get("farms", []):
        if f["farm_id"] in SHARES:
            print(f"  {f['farm_id']:8} {f['allocated']:>7} {f['consumed']:>7} {f['sent']:>6} {f['received']:>6} "
                  f"{f['available']:>7}  {f.get('signs_reports')}")
    return s


def status():
    for name, port in LEDGERS.items():
        try:
            s = tls.get(f"http://localhost:{port}/status", timeout=2).json()
            print(f"  {name}  {s['role']:9} term={s['term']}  commit={s['commit_index']}")
        except Exception:
            print(f"  {name}  DOWN")


def day():
    current = tls.get(f"{GW}/state", timeout=10).json().get("day")
    nxt = (dt.date.fromisoformat(current) + dt.timedelta(days=1)).isoformat() if current else dt.date.today().isoformat()
    print(f"Opening {nxt} as the operator:")
    show(tls.post(f"{GW}/days", json={"day": nxt, "limit_l": 20000, "allocations": SHARES, "cmd_id": f"day-{nxt}"},
                  timeout=15))
    time.sleep(5)
    state()


def sync(farm):
    s = tls.get(f"http://localhost:{FARMS[farm]}/sync", timeout=5).json()
    print(f"  {farm}: link={s.get('link')} online={s.get('online')} pending={s.get('pending_events')} "
          f"local_used={s.get('local_consumed')} ledger_used={s.get('ledger_consumed')}")


def offline():
    farm = "farm-d"
    print(f"Cutting {farm}'s network link:")
    tls.post(f"http://localhost:{FARMS[farm]}/link/down", json={}, timeout=5)
    for _ in range(3):
        time.sleep(4)
        sync(farm)
    print(f"Link back:")
    tls.post(f"http://localhost:{FARMS[farm]}/link/up", json={}, timeout=5)
    time.sleep(8)
    sync(farm)


def last_transfer(farm="farm-a"):
    d = tls.get(f"{GW}/state", timeout=10).json()["day"]
    ts = tls.get(f"{GW}/farms/{farm}/transfers", params={"day": d}, timeout=10).json()
    return ts[-1] if ts else None


def transfer(amount=100):
    print(f"farm-a sends {amount} L to farm-b (through farm-a's own node, which signs it):")
    show(tls.post(f"http://localhost:{FARMS['farm-a']}/transfers", json={"recipient": "farm-b", "amount_l": amount},
                  timeout=30))


def duplicate():
    transfer(50)
    time.sleep(3)
    t = last_transfer()
    if not t:
        sys.exit("no transfer found yet: wait a few seconds and run it again")
    body = {k: t[k] for k in ("transfer_id", "day", "sender", "recipient", "amount_l")}
    body["cmd_id"] = f"transfer-{t['transfer_id']}"
    print(f"Sending transfer {t['transfer_id'][:8]}... again, twice:")
    show(tls.post(f"{GW}/transfers", json=body, timeout=15))
    show(tls.post(f"{GW}/transfers", json=body, timeout=15))


def proof():
    import subprocess
    t = last_transfer()
    if not t:
        sys.exit("no transfer today yet: run 'python scripts/demo.py transfer' first")
    script = str(tls.ROOT / "scripts" / "verify_proof.py")
    print(f"Real record (transfer {t['transfer_id'][:8]}...):")
    subprocess.run([sys.executable, script, "--transfer-id", t["transfer_id"]])
    print("\nFalsified copy (amount changed):")
    subprocess.run([sys.executable, script, "--transfer-id", t["transfer_id"], "--tamper"])


def overspend():
    transfer(999999)


def forbidden():
    tls.CERT, tls.KEY = str(tls.ROOT / "certs/farm-a.crt"), str(tls.ROOT / "certs/farm-a.key")
    tls._shared = None
    print("farm-a's certificate tries to open a day:")
    show(tls.post(f"{GW}/days", json={"day": "2099-01-01", "limit_l": 1, "allocations": {"farm-a": 1}}, timeout=10))


def nocert():
    print("A request with no client certificate:")
    url = tls.fix(f"{GW}/state")
    try:
        ctx = ssl.create_default_context(cafile=tls.CA) if tls.ENABLED else None
        show(httpx.get(url, verify=ctx or True, timeout=5))
    except httpx.HTTPError as e:
        print(f"  REFUSED  {type(e).__name__}: {e}")


LIE_SQL = ("UPDATE farm_balances SET allocated = allocated {} 5000 "
           "WHERE farm_id='farm-a' AND day=(SELECT max(day) FROM farm_balances);")


def psql(sql):
    import subprocess
    subprocess.run(["docker", "exec", "aq-ledger-db-3", "psql", "-U", "aquashare", "-d", "ledger", "-c", sql])


def lie():
    print("ledger-3's database now says farm-a has 5000 L more (the other two are unchanged):")
    psql(LIE_SQL.format("+"))
    state()


def honest():
    print("Undoing the change on ledger-3:")
    psql(LIE_SQL.format("-"))
    state()


ACTIONS = {"status": status, "day": day, "offline": offline, "duplicate": duplicate, "overspend": overspend,
           "transfer": transfer, "proof": proof, "forbidden": forbidden, "nocert": nocert, "state": state,
           "lie": lie, "honest": honest}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ACTIONS:
        sys.exit(__doc__)
    ACTIONS[sys.argv[1]]()
