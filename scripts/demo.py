"""Terminal demo of AquaShare, one scenario per command (works with mutual TLS on or off).

  python scripts/demo.py status          the 3 ledger servers: who leads, term, commit
  python scripts/demo.py day             open the next day and show each farm's share
  python scripts/demo.py offline         the farm with the most water loses its link, keeps watering, then syncs
  python scripts/demo.py duplicate       the same transfer sent twice is counted once
  python scripts/demo.py overspend       a farm tries to send far more water than it has
  python scripts/demo.py transfer        the farm with the most water sends 100 L to another farm
  python scripts/demo.py proof           Merkle proof of the latest transfer, then of a falsified copy
  python scripts/demo.py forbidden       farm-a's certificate tries to open a day -> 403
  python scripts/demo.py nocert          a request without any certificate -> refused
  python scripts/demo.py state           each farm's water today, and which ledger nodes agreed on it
  python scripts/demo.py lie             ledger-3's database is altered: farm-a gets 5000 L more on that node only
  python scripts/demo.py honest          undo the lie
  python scripts/demo.py crash           stop the leader: a new one is elected; restart it: it catches up
  python scripts/demo.py majority        stop 2 of 3 ledgers: a transfer waits (not lost); restart: it completes
  python scripts/demo.py fake            a fake farm sends reports without the right signature -> refused
  python scripts/demo.py check           the 7 rules (invariants) on all 3 databases
  python scripts/demo.py recover         farm-a loses all its data, rebuilds from the ledger, operator replaces its key
  python scripts/demo.py partition       cut the leader off the network, then heal it (about 1 minute)
  python scripts/demo.py chaos           random crashes while clients write (about 3 minutes)
  python scripts/demo.py benchmark       throughput, latency and failover time (several minutes)
  python scripts/demo.py spark           Spark season report on a simulated 120-day season
  python scripts/demo.py tests           the 108 automated tests
  python scripts/demo.py kubernetes      the ledger on Kubernetes: delete the leader pod, Kubernetes recreates it
                                         (stops the Docker version first, and starts it again at the end)
  python scripts/demo.py all             the live demo in order: everything above that takes under a minute
"""
import datetime as dt
import subprocess
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


def richest(exclude=()):
    """The farm with the most water left today, so a scenario never fails for lack of water."""
    farms = [f for f in tls.get(f"{GW}/state", timeout=10).json().get("farms", [])
             if f["farm_id"] in FARMS and f["farm_id"] not in exclude]
    best = max(farms, key=lambda f: f["available"]) if farms else None
    if not best or best["available"] < 200:
        sys.exit("no farm has enough water left today: run 'python scripts/demo.py day' first")
    return best["farm_id"]


def status(quiet=False):
    roles = {}
    for name, port in LEDGERS.items():
        try:
            s = tls.get(f"http://localhost:{port}/status", timeout=2).json()
            roles[name] = s["role"]
            if not quiet:
                print(f"  {name}  {s['role']:9} term={s['term']}  commit={s['commit_index']}")
        except Exception:
            roles[name] = "down"
            if not quiet:
                print(f"  {name}  DOWN")
    return roles


def run(*cmd, check=False):
    """Run a command (docker, a script) and show its output as it comes."""
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(list(cmd), check=check).returncode


def py(script, *args):
    return run(sys.executable, str(tls.ROOT / "scripts" / script), *args)


def title(text):
    print(f"\n=== {text} ===")


def wait(seconds, why=""):
    print(f"  (waiting {seconds} s{': ' + why if why else ''})")
    time.sleep(seconds)


def leader():
    for _ in range(20):
        found = [n for n, r in status(quiet=True).items() if r == "leader"]
        if found:
            return found[0]
        time.sleep(1)
    sys.exit("no leader found: is the system running? (docker compose up -d)")


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
    farm = richest()                                  # a farm with water keeps irrigating while offline
    print(f"Cutting {farm}'s network link:")
    tls.post(f"http://localhost:{FARMS[farm]}/link/down", json={}, timeout=5)
    for _ in range(3):
        time.sleep(4)
        sync(farm)
    print(f"Link back:")
    tls.post(f"http://localhost:{FARMS[farm]}/link/up", json={}, timeout=5)
    time.sleep(8)
    sync(farm)


LAST = {"sender": None}


def last_transfer():
    """The latest transfer sent today (by the farm used last, or by any farm)."""
    d = tls.get(f"{GW}/state", timeout=10).json()["day"]
    found = []
    for farm in ([LAST["sender"]] if LAST["sender"] else list(FARMS)):
        found += [t for t in tls.get(f"{GW}/farms/{farm}/transfers", params={"day": d}, timeout=10).json()
                  if t.get("sender") == farm and t.get("status") == "committed"]
    return max(found, key=lambda t: t.get("log_index", 0)) if found else None


def transfer(amount=100, sender=None):
    sender = sender or richest()
    recipient = next(f for f in FARMS if f != sender)
    LAST["sender"] = sender
    print(f"{sender} sends {amount} L to {recipient} (through {sender}'s own node, which signs it):")
    return show(tls.post(f"http://localhost:{FARMS[sender]}/transfers",
                         json={"recipient": recipient, "amount_l": amount}, timeout=30))


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
    transfer(999999)                                  # far more than any farm holds: refused


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


def crash():
    title("Crash the leader")
    status()
    old = leader()
    print(f"Stopping the leader, {old}, without warning:")
    run("docker", "stop", f"aq-{old}")
    wait(4, "the other two elect a new leader")
    status()
    print(f"Starting {old} again:")
    run("docker", "start", f"aq-{old}")
    wait(8, "it rejoins as a follower and catches up")
    status()


def majority():
    title("Lose the majority")
    status()
    sender = richest()                                # chosen now: with one node left, reads are refused
    print("Stopping ledger-2 and ledger-3: only one node is left, no majority:")
    run("docker", "stop", "aq-ledger-2", "aq-ledger-3")
    status()
    print("A transfer now cannot be saved, so it stays pending (the water is on hold, not lost):")
    transfer(100, sender)
    print("Starting them again:")
    run("docker", "start", "aq-ledger-2", "aq-ledger-3")
    wait(15, "a leader is elected and the farm sends the transfer again by itself")
    status()
    t = last_transfer()
    if t:
        print(f"  latest transfer {t['transfer_id'][:8]}...  {t['sender']} -> {t['recipient']}  "
              f"{t['amount_l']} L  {t['status']}")


def fake():
    title("A fake farm")
    py("fake_farm.py", "--farm", "farm-a", "--amount", "500")


def check():
    title("The 7 rules on the 3 databases")
    py("check_invariants.py")


def recover():
    title("A farm loses all its data")
    state()
    for cmd in (("docker", "compose", "stop", "farm-a"), ("docker", "compose", "rm", "-f", "farm-a")):
        run(*cmd)
    volumes = subprocess.run(["docker", "volume", "ls", "-q"], capture_output=True, text=True).stdout.split()
    volume = next((v for v in volumes if v.endswith("farm-a-data")), None)
    if volume:
        run("docker", "volume", "rm", volume)
    run("docker", "compose", "up", "-d", "farm-a")
    wait(12, "farm-a starts with an empty journal and a new key")
    run("docker", "logs", "aq-farm-a", "--tail", "4")
    print("The operator registers farm-a's new key:")
    py("replace_farm_key.py", "--farm", "farm-a")
    wait(15, "farm-a registers again and sends the reports that waited")
    run("docker", "logs", "aq-farm-a", "--tail", "4")
    state()


def partition():
    title("Network partition")
    py("partition_test.py")


def chaos():
    title("Chaos test (about 3 minutes)")
    py("chaos_test.py", "--minutes", "2")


def benchmark():
    title("Benchmark (several minutes)")
    py("benchmark.py", "--seconds", "8", "--failovers", "3")


def spark():
    title("Spark season report")
    py("../spark/simulate_season.py", "--days", "120")
    run("docker", "compose", "run", "--rm", "spark", "/app/spark/water_report.py",
        "/app/spark/data_simulated", "/app/spark/output_simulated")


def tests():
    title("Automated tests")
    run(sys.executable, "-m", "pytest", "-q", str(tls.ROOT / "tests"))


def kubernetes():
    title("Kubernetes (stops the Docker version, then starts it again)")
    run("docker", "compose", "down")
    k8s = str(tls.ROOT / "k8s")
    ps = ["powershell", "-ExecutionPolicy", "Bypass", "-File"]
    run(*ps, f"{k8s}/deploy.ps1")
    run(*ps, f"{k8s}/status.ps1")
    run("kubectl", "delete", "pod", "ledger-0")
    wait(4)
    run(*ps, f"{k8s}/status.ps1")
    wait(20, "Kubernetes recreates the pod")
    run(*ps, f"{k8s}/status.ps1")
    run("kubectl", "get", "pods")
    run("kubectl", "delete", "-f", f"{k8s}/gateway.yaml", "-f", f"{k8s}/ledger.yaml")
    run("docker", "compose", "up", "-d")
    wait(30, "the Docker version starts again")
    status()


def everything():
    """The live demo, in order. Each step stops to let you explain, then press Enter."""
    steps = [("Start a new day", day), ("A farm goes offline", offline), ("Same transfer twice", duplicate),
             ("Spend too much", overspend), (None, crash), (None, majority), ("Prove a record is real", proof),
             (None, fake), ("No certificate", nocert), ("Wrong role", forbidden), ("A lying ledger node", lie),
             (None, check), ("Undo the lie", honest), (None, check)]
    for name, step in steps:
        if name:
            title(name)
        step()
        try:
            input("\n  [Enter] next step ")
        except EOFError:
            pass
    print("\nDone. Longer tests: partition, chaos, benchmark, spark, tests, kubernetes, recover.")


ACTIONS = {"status": status, "day": day, "offline": offline, "duplicate": duplicate, "overspend": overspend,
           "transfer": transfer, "proof": proof, "forbidden": forbidden, "nocert": nocert, "state": state,
           "lie": lie, "honest": honest, "crash": crash, "majority": majority, "fake": fake, "check": check,
           "recover": recover, "partition": partition, "chaos": chaos, "benchmark": benchmark, "spark": spark,
           "tests": tests, "kubernetes": kubernetes, "all": everything}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ACTIONS:
        sys.exit(__doc__)
    ACTIONS[sys.argv[1]]()
