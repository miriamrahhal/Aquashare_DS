"""Network partition test on the running Docker cluster.

The current leader is disconnected from the Docker network while it keeps running.
  1. the isolated leader still believes it is the leader, but cannot commit anything
  2. the two other nodes (a majority) elect a new leader in a higher term and keep working
  3. when the network heals, the old leader steps down, discards the entry it could not
     commit, and copies the majority's log, so all three nodes end with the same hash

  python scripts/partition_test.py
"""
import json
import subprocess
import sys
import time
import uuid

import httpx  # noqa: F401  (kept for its exception classes)
import os as _os
import sys as _sys

_sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
from shared import tls  # noqa: E402

tls.for_host()          # follow the project's .env: use mutual TLS (as the operator) when it is on

PORTS = {"ledger-1": 8701, "ledger-2": 8702, "ledger-3": 8703}


def sh(*args, check=True):
    return subprocess.run(args, capture_output=True, text=True, check=check).stdout.strip()


def inside(container, path, body=None, timeout=10):
    """Call a ledger node's API from inside its own container (it is unreachable from outside)."""
    scheme = "https" if tls.ENABLED else "http"
    code = ("import json,os,ssl,sys,urllib.request\n"
            # with mutual TLS on, the node calls itself with its own certificate (mounted in /certs)
            "ctx=None\n"
            "if os.getenv('TLS_ENABLED')=='1':\n"
            "    ctx=ssl.create_default_context(cafile=os.environ['TLS_CA']); ctx.load_cert_chain(os.environ['TLS_CERT'], os.environ['TLS_KEY'])\n"
            f"req=urllib.request.Request('{scheme}://localhost:8000{path}', data={json.dumps(body).encode() if body else None!r},"
            " headers={'content-type':'application/json'})\n"
            "try:\n"
            f"    print(urllib.request.urlopen(req, timeout={timeout}, context=ctx).read().decode())\n"
            "except urllib.error.HTTPError as e:\n"
            "    print(json.dumps({'http_error': e.code, 'body': e.read().decode()}))\n"
            "except Exception as e:\n"
            "    print(json.dumps({'error': type(e).__name__, 'body': str(e)}))\n")
    return json.loads(sh("docker", "exec", container, "python", "-c", code))


def status(node):
    try:
        return tls.get(f"http://localhost:{PORTS[node]}/status", timeout=2).json()
    except httpx.HTTPError:
        return None


def step(text):
    print(f"\n{time.strftime('%H:%M:%S')}  {text}")


def main():
    leader = next((n for n in PORTS if (s := status(n)) and s["role"] == "leader"), None)
    if leader is None:
        sys.exit("no leader found: start the three ledger nodes first")
    container = f"aq-{leader}"
    networks = sh("docker", "inspect", container, "--format",
                  "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}").split()
    network = next(n for n in networks if n.endswith("_default"))     # the cluster network, not the database one
    before = status(leader)
    step(f"1. {leader} is the leader in term {before['term']} (commit index {before['commit_index']})")

    sh("docker", "network", "disconnect", network, container)
    step(f"2. PARTITION: {leader} disconnected from network {network} (the process keeps running)")
    time.sleep(1)

    reply = inside(container, "/command", {"command": {"type": "test", "value": "written by the isolated leader",
                                                       "cmd_id": str(uuid.uuid4())}})
    iso = inside(container, "/status")
    lost_index = iso["last_log_index"]
    step(f"3. command sent to the isolated {leader}: {reply.get('body', reply)}")
    print(f"   {leader} still says role={iso['role']} term={iso['term']}, "
          f"but last log index {iso['last_log_index']} > commit index {iso['commit_index']}: not committed")

    step("4. waiting for the majority to elect a new leader ...")
    deadline, new = time.time() + 15, None
    while time.time() < deadline and not new:
        for n in PORTS:
            s = status(n) if n != leader else None
            if s and s["role"] == "leader" and s["term"] > before["term"]:
                new = s
        time.sleep(0.5)
    if not new:
        sys.exit("FAIL: no new leader elected by the majority")
    print(f"   {new['node_id']} elected leader in term {new['term']}  ->  two nodes believe they lead, "
          f"but only one has a majority")

    committed = tls.post(f"http://localhost:{PORTS[new['node_id']]}/command",
                           json={"command": {"type": "test", "value": "committed by the majority",
                                             "cmd_id": str(uuid.uuid4())}}, timeout=10).json()
    step(f"5. the majority keeps working: command committed at index {committed['committed_index']} "
         f"by {committed['leader_id']}")
    old_entry = inside(container, f"/log?start={lost_index}&limit=1")[0]

    sh("docker", "network", "connect", "--alias", leader, network, container)
    step(f"6. HEAL: {leader} reconnected to the network")
    deadline, healed = time.time() + 20, None
    while time.time() < deadline:
        s = status(leader)
        others = [status(n) for n in PORTS if n != leader]
        if s and all(others) and s["role"] == "follower" and s["term"] == others[0]["term"] \
                and s["commit_index"] == max(o["commit_index"] for o in others):
            healed = s
            break
        time.sleep(0.5)
    if not healed:
        sys.exit("FAIL: the old leader did not rejoin as a follower")
    new_entry = tls.get(f"http://localhost:{PORTS[leader]}/log", params={"start": lost_index, "limit": 1}).json()[0]
    print(f"   {leader} is now a follower in term {healed['term']} (leader {healed['leader_id']})")
    print(f"   entry {lost_index} on {leader} before healing: term {old_entry['term']}, {old_entry['command']}")
    print(f"   entry {lost_index} on {leader} after healing:  term {new_entry['term']}, {new_entry['command']}")

    final = {n: status(n) for n in PORTS}
    same = len({(s["commit_index"], s["last_log_hash"]) for s in final.values()}) == 1
    step("7. final state")
    for n, s in final.items():
        print(f"   {n}  {s['role']:9} term={s['term']}  commit_index={s['commit_index']}  last_hash={s['last_log_hash']}")
    lost = "isolated leader" not in json.dumps(new_entry["command"])
    print(f"\n{'PASS' if same else 'FAIL'}  all three nodes hold the same log")
    print(f"{'PASS' if lost else 'FAIL'}  the isolated leader's uncommitted entry was discarded")
    print(f"\nPARTITION TEST {'PASSED' if same and lost else 'FAILED'}")


if __name__ == "__main__":
    main()
