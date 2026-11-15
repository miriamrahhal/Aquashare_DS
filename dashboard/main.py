"""AquaShare web dashboard.

A small server that serves one page and gathers live data for it from the rest of the
system: the ledger (through the gateway and from each ledger node), the four farms and the
analytics service. The browser only talks to this server, so no other component needs to
allow cross-origin requests. It stores nothing itself.
"""
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time
import uuid
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel


def parse(text):
    return dict(item.split("=", 1) for item in text.split(",") if item.strip())


GATEWAY = os.getenv("GATEWAY_URL", "http://localhost:8600")
LEDGERS = parse(os.getenv("LEDGER_NODES",
                          "ledger-1=http://localhost:8701,ledger-2=http://localhost:8702,ledger-3=http://localhost:8703"))
FARMS = parse(os.getenv("FARM_NODES", "farm-a=http://localhost:8801,farm-b=http://localhost:8802,"
                                      "farm-c=http://localhost:8803,farm-d=http://localhost:8804"))
ANALYTICS = os.getenv("ANALYTICS_URL", "http://localhost:8901")

http = httpx.Client(timeout=2.0)
pool = ThreadPoolExecutor(max_workers=16)
app = FastAPI(title="AquaShare dashboard")


def get(url, params=None):
    try:
        r = http.get(url, params=params)
        return r.json() if r.status_code == 200 else None
    except (httpx.HTTPError, ValueError):
        return None


def ledger_node(name, url):
    status = get(f"{url}/status")
    if status:
        root = get(f"{url}/merkle/root")
        status["merkle_root"] = root["root"] if root else None
    return name, status


def farm_node(name, url):
    return name, {"sync": get(f"{url}/sync"), "wallet": get(f"{url}/wallet"), "sensors": get(f"{url}/sensors")}


@app.get("/api/overview")
def overview():
    started = time.perf_counter()
    jobs = {"state": pool.submit(get, f"{GATEWAY}/state"),
            "analytics": pool.submit(get, f"{ANALYTICS}/summary")}
    ledger_jobs = [pool.submit(ledger_node, n, u) for n, u in LEDGERS.items()]
    farm_jobs = [pool.submit(farm_node, n, u) for n, u in FARMS.items()]
    ledgers = dict(j.result() for j in ledger_jobs)
    farms = dict(j.result() for j in farm_jobs)
    state = jobs["state"].result()

    leader = next((s for s in ledgers.values() if s and s["role"] == "leader"), None)
    activity = []
    if leader:
        url = LEDGERS[leader["node_id"]]
        after = max(0, leader["last_applied"] - 40)
        for item in reversed(get(f"{url}/events", {"after": after, "limit": 40}) or []):
            c, r = item["command"], item["result"]
            if c.get("type") in ("noop", "test") or not r.get("ok") and c.get("type") == "noop":
                continue
            activity.append({"index": item["log_index"], "type": c.get("type"), "ok": bool(r.get("ok")),
                             "farm": c.get("farm_id") or c.get("sender"), "recipient": c.get("recipient"),
                             "amount": c.get("amount_l") or r.get("amount_l"), "error": r.get("error"),
                             "day": c.get("day")})
            if len(activity) == 12:
                break
    return {"state": state, "ledgers": ledgers, "farms": farms, "analytics": jobs["analytics"].result(),
            "activity": activity, "took_ms": round((time.perf_counter() - started) * 1000)}


class TransferBody(BaseModel):
    sender: str
    recipient: str
    amount_l: int


@app.post("/api/transfer")
def transfer(body: TransferBody):
    if body.sender not in FARMS:
        raise HTTPException(404, "unknown farm")
    try:
        r = http.post(f"{FARMS[body.sender]}/transfers", json={"recipient": body.recipient, "amount_l": body.amount_l},
                      timeout=20)
    except httpx.HTTPError:
        raise HTTPException(503, f"{body.sender} is not reachable")
    return r.json() if r.status_code == 200 else {"status": "refused", "reason": r.json().get("detail")}


@app.post("/api/farms/{farm}/{action}/{state}")
def farm_action(farm: str, action: str, state: str):
    if farm not in FARMS or action not in ("link", "irrigation"):
        raise HTTPException(404, "unknown farm or action")
    try:
        return http.post(f"{FARMS[farm]}/{action}/{state}").json()
    except httpx.HTTPError:
        raise HTTPException(503, f"{farm} is not reachable")





# ===================================================================== demo scenarios
# The buttons of the "Demo Scenarios" panel. Each one performs a scenario of the report
# and returns what happened, so it can be explained on screen without a terminal.
#
# Stopping and starting ledger nodes needs control over the containers. In Docker the
# dashboard talks to the Docker Engine through its socket (mounted read-write in
# docker-compose.yml). This gives the dashboard full control of Docker, which is acceptable
# for a local demonstration tool but would never be done in production.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
DOCKER_SOCKET = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")
CONTAINER_PREFIX = os.getenv("CONTAINER_PREFIX", "aq-")
CONTROL_CMD = os.getenv("CONTROL_CMD")        # optional, outside Docker: e.g. "./ctl.sh {action} {node}"
RESULTS_DIR = Path(os.getenv("RESULTS_DIR", ROOT / "results"))
LEDGER_DSNS = os.getenv("LEDGER_DSNS")


def control(node: str, action: str) -> None:
    """Stop (like a crash: no grace period) or start a ledger node."""
    if node not in LEDGERS or action not in ("stop", "start"):
        raise HTTPException(404, "unknown node or action")
    if CONTROL_CMD:
        subprocess.run(shlex.split(CONTROL_CMD.format(action=action, node=node)), check=True, timeout=20)
        return
    if not Path(DOCKER_SOCKET).exists():
        raise HTTPException(501, "the dashboard has no access to Docker (see the docker.sock volume in docker-compose.yml)")
    docker = httpx.Client(transport=httpx.HTTPTransport(uds=DOCKER_SOCKET), base_url="http://docker", timeout=20)
    query = "?t=0" if action == "stop" else ""
    r = docker.post(f"/containers/{CONTAINER_PREFIX}{node}/{action}{query}")
    if r.status_code not in (204, 304):
        raise HTTPException(502, f"Docker refused to {action} {node}: {r.text[:200]}")


def statuses() -> dict:
    return dict(pool.map(lambda kv: (kv[0], get(f"{kv[1]}/status")), LEDGERS.items()))


def leader_of(sts: dict):
    return next((n for n, s in sts.items() if s and s.get("role") == "leader"), None)


def wait_for_leader(exclude=None, seconds=15.0):
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        sts = statuses()
        leader = leader_of(sts)
        if leader and leader != exclude:
            return leader, sts[leader]["term"]
        time.sleep(0.1)
    return None, None


@app.get("/api/demo/capabilities")
def capabilities():
    return {"containers": bool(CONTROL_CMD) or Path(DOCKER_SOCKET).exists(), "invariants": bool(LEDGER_DSNS)}


@app.post("/api/demo/ledger/{node}/{action}")
def ledger_action(node: str, action: str):
    control(node, action)
    return {"node": node, "action": action}


@app.post("/api/demo/kill-leader")
def kill_leader():
    """Scenario 1: crash the leader and measure how long the cluster takes to elect another one."""
    sts = statuses()
    old = leader_of(sts)
    if not old:
        raise HTTPException(409, "there is no leader to stop right now")
    old_term = sts[old]["term"]
    started = time.perf_counter()
    control(old, "stop")
    new, term = wait_for_leader(exclude=old)
    took = round(time.perf_counter() - started, 2)
    if not new:
        return {"ok": False, "old_leader": old, "message": f"No new leader after {took} s. Is a majority running?"}
    return {"ok": True, "old_leader": old, "old_term": old_term, "new_leader": new, "new_term": term, "seconds": took,
            "message": f"{old} crashed. {new} was elected leader for election round {term} after {took} s. "
                       f"Nothing that was saved is lost: the two remaining servers are a majority."}


@app.post("/api/demo/lose-majority")
def lose_majority():
    """Scenario 2: stop two of the three ledger nodes, then try a transfer."""
    sts = statuses()
    running = [n for n, s in sts.items() if s]
    if len(running) < 2:
        return {"ok": True, "stopped": [], "message": "A majority is already missing."}
    keep = leader_of(sts) or running[0]
    stopped = [n for n in running if n != keep][:2] if len(running) == 3 else [n for n in running if n != keep]
    for n in stopped:
        control(n, "stop")
    return {"ok": True, "stopped": stopped, "kept": keep,
            "message": f"Stopped {', '.join(stopped)}. Only {keep} is left, which is not a majority of 3, "
                       f"so the ledger cannot save anything until a server comes back."}


@app.post("/api/demo/restart-all")
def restart_all():
    sts = statuses()
    started = [n for n, s in sts.items() if not s]
    for n in started:
        control(n, "start")
    leader, term = wait_for_leader(seconds=25) if started else (leader_of(sts), None)
    return {"ok": bool(leader), "started": started, "leader": leader,
            "message": (f"Restarted {', '.join(started)}. " if started else "All servers were already running. ")
                       + (f"{leader} leads; the restarted servers copy the records they missed from it."
                          if leader else "Still waiting for a leader.")}


def ledger_balance(farm: str, day: str):
    st = get(f"{GATEWAY}/state", {"day": day}) or {}
    return next((f for f in st.get("farms", []) if f["farm_id"] == farm), None)


@app.post("/api/demo/duplicate")
def duplicate():
    """Scenario 4: re-send a transfer that is already committed, with the same identifiers, twice."""
    st = get(f"{GATEWAY}/state")
    if not st or not st.get("day"):
        raise HTTPException(409, "no day is open")
    day = st["day"]
    sender, recipient = list(FARMS)[0], list(FARMS)[1]
    log = []
    try:
        mine = http.get(f"{GATEWAY}/farms/{sender}/transfers", params={"day": day}, timeout=10).json()
    except (httpx.HTTPError, ValueError):
        mine = []
    original = next((t for t in reversed(mine) if t["sender"] == sender and t["status"] == "committed"), None)
    if not original:
        r = http.post(f"{FARMS[sender]}/transfers", json={"recipient": recipient, "amount_l": 50}, timeout=30).json()
        if r.get("status") != "committed":
            return {"ok": False, "message": f"Could not create a first transfer: {r.get('reason') or r}"}
        original = {"transfer_id": r["transfer_id"], "day": day, "sender": sender, "recipient": recipient, "amount_l": 50}
        log.append(f"Sent a first transfer of 50 L from {sender} to {recipient}.")
    before = ledger_balance(sender, day)
    body = {k: original[k] for k in ("transfer_id", "day", "sender", "recipient", "amount_l")}
    body["cmd_id"] = f"transfer-{body['transfer_id']}"
    replies = []
    for attempt in (1, 2):
        r = http.post(f"{GATEWAY}/transfers", json=body, timeout=30).json()
        result = r.get("result", {})
        replies.append({"attempt": attempt, "index": r.get("committed_index"), "duplicate": bool(result.get("duplicate")),
                        "status": result.get("status")})
    after = ledger_balance(sender, day)
    unchanged = before and after and before["sent"] == after["sent"]
    log += [f"Re-sent transfer {body['transfer_id'][:8]}… ({body['amount_l']} L) twice, with the same identifiers."]
    return {"ok": bool(unchanged), "transfer": body, "replies": replies,
            "sent_before": before and before["sent"], "sent_after": after and after["sent"], "log": log,
            "message": f"The ledger recognised both copies as duplicates and changed nothing: {sender} has sent "
                       f"{after['sent']:,} L before and after." if unchanged else "The balance changed: duplicate applied!"}


@app.post("/api/demo/overspend")
def overspend():
    """Scenario 5: ask for more water than a farm has, at the farm and directly at the ledger."""
    st = get(f"{GATEWAY}/state")
    if not st or not st.get("day"):
        raise HTTPException(409, "no day is open")
    day, sender, recipient = st["day"], list(FARMS)[0], list(FARMS)[1]
    wallet = get(f"{FARMS[sender]}/wallet") or {}
    amount = (wallet.get("available") or 0) + 5000
    r = http.post(f"{FARMS[sender]}/transfers", json={"recipient": recipient, "amount_l": amount}, timeout=30)
    farm_answer = r.json().get("detail") if r.status_code != 200 else r.json().get("status")
    led = ledger_balance(sender, day) or {}
    big = (led.get("allocated", 0) + led.get("received", 0)) + 5000
    r2 = http.post(f"{GATEWAY}/transfers", json={"day": day, "sender": sender, "recipient": recipient,
                                                  "amount_l": big, "cmd_id": str(uuid.uuid4())}, timeout=30).json()
    if isinstance(r2.get("detail"), dict):              # the gateway answers 409 with the ledger's reason
        r2 = {"committed_index": r2["detail"].get("committed_index"),
              "result": {"ok": False, "error": r2["detail"].get("error")}}
    res = r2.get("result", {})
    after = ledger_balance(sender, day) or {}
    ok = r.status_code != 200 and not res.get("ok", False) and after.get("sent") == led.get("sent")
    return {"ok": ok, "farm": {"amount": amount, "answer": farm_answer},
            "ledger": {"amount": big, "answer": res.get("error") or res, "index": r2.get("committed_index")},
            "message": f"{sender}'s wallet refused {amount:,} L ({farm_answer}). Sent directly to the ledger, "
                       f"{big:,} L was also refused: {res.get('error')}. No water moved."}


# ---- Merkle proof, re-implemented here like scripts/verify_proof.py (independent of the ledger code)
def _sha(b: bytes) -> bytes:
    return hashlib.sha256(b).digest()


def _entry_hash(prev_hash, index, term, command) -> str:
    canonical = json.dumps(command, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{prev_hash}|{index}|{term}|{canonical}".encode()).hexdigest()


def _fold(leaf, m, size, path):
    fn, sn, r = m, size - 1, leaf
    for p in path:
        if sn == 0:
            return None
        if fn % 2 == 1 or fn == sn:
            r = _sha(b"\x01" + p + r)
            while fn % 2 == 0 and fn != 0:
                fn >>= 1
                sn >>= 1
        else:
            r = _sha(b"\x01" + r + p)
        fn >>= 1
        sn >>= 1
    return r if sn == 0 else None


@app.get("/api/demo/proof")
def proof(tamper: bool = False):
    """Scenario 6: prove that the latest transfer is in the ledger, optionally after falsifying it."""
    sts = statuses()
    node = leader_of(sts) or next((n for n, s in sts.items() if s), None)
    if not node:
        raise HTTPException(503, "no ledger node is reachable")
    url = LEDGERS[node]
    commit = sts[node]["commit_index"]
    # prove against the size every running node has already committed, so all of them can answer
    size_all = min(s["commit_index"] for s in sts.values() if s)
    events = get(f"{url}/events", {"after": max(0, commit - 400), "limit": 400}) or []
    events = [e for e in events if e["log_index"] <= size_all]
    target = next((e for e in reversed(events) if e["command"].get("type") == "transfer" and e["result"].get("ok")),
                  None) or next((e for e in reversed(events) if e["command"].get("type") != "noop"), None)
    if not target:
        raise HTTPException(404, "no record to prove yet")
    p = get(f"{url}/merkle/proof", {"index": target["log_index"], "size": size_all})
    if not p:
        raise HTTPException(503, "the node did not return a proof")
    e, size = p["entry"], p["size"]
    cmd = dict(e["command"])
    original_amount = cmd.get("amount_l")
    if tamper:
        if "amount_l" in cmd:
            cmd["amount_l"] = cmd["amount_l"] * 10
        else:
            cmd["tampered"] = True
    checks = []
    recomputed = _entry_hash(e["prev_hash"], e["index"], e["term"], cmd)
    checks.append({"name": "Record's hash recomputed from its contents", "ok": recomputed == e["entry_hash"],
                   "detail": recomputed[:16] + "…"})
    path = [bytes.fromhex(h) for h in p["path"]]
    folded = _fold(_sha(b"\x00" + bytes.fromhex(recomputed)), e["index"] - 1, size, path)
    checks.append({"name": f"{len(path)} proof hashes lead to the published root", "ok": bool(folded) and folded.hex() == p["root"],
                   "detail": p["root"][:16] + "…"})
    for name, other_url in LEDGERS.items():
        if name == node:
            continue
        other = get(f"{other_url}/merkle/root", {"size": size})
        checks.append({"name": f"{name} publishes the same root", "ok": bool(other) and other["root"] == p["root"],
                       "detail": "not reachable" if not other else other["root"][:16] + "…", "skipped": not other})
    ok = all(c["ok"] for c in checks if not c.get("skipped"))
    c = e["command"]
    what = (f"{c.get('sender')} gave {original_amount} L to {c.get('recipient')}" if c.get("type") == "transfer"
            else c.get("type", "record"))
    shown = f"{c.get('sender')} gave {cmd.get('amount_l')} L to {c.get('recipient')}" if tamper and c.get("type") == "transfer" else what
    return {"ok": ok, "node": node, "index": e["index"], "size": size, "record": what, "checked": shown,
            "tampered": tamper, "path_length": len(path), "checks": checks,
            "message": ("Verified: record #{} is in the ledger exactly as shown, and all servers agree.".format(e["index"])
                        if ok else "Verification FAILED: the record shown is not the one in the ledger.")}


@app.get("/api/demo/invariants")
def invariants():
    """Scenario 7: run the invariant checker (I1-I7) on the three ledger databases."""
    if not LEDGER_DSNS:
        raise HTTPException(501, "LEDGER_DSNS is not set, so the dashboard cannot reach the databases")
    import psycopg
    from scripts.check_invariants import check_cluster, check_node
    conns, rows = {}, []
    for i, dsn in enumerate(LEDGER_DSNS.split(","), start=1):
        try:
            conns[f"ledger-{i}"] = psycopg.connect(dsn, autocommit=True, connect_timeout=3)
        except psycopg.OperationalError:
            rows.append({"node": f"ledger-{i}", "check": "database reachable", "ok": False, "detail": "not reachable"})
    try:
        per_node = {n: check_node(c) for n, c in conns.items()}
        if per_node:
            first = next(iter(per_node.values()))
            for k, (chk, _, detail) in enumerate(first):
                results = {n: r[k] for n, r in per_node.items()}
                all_ok = all(v[1] for v in results.values())
                rows.append({"node": "each node", "check": chk, "ok": all_ok, "nodes": {n: v[1] for n, v in results.items()},
                             "detail": detail if all_ok else " · ".join(f"{n}: {v[2]}" for n, v in results.items())})
        if len(conns) > 1:
            for chk, passed, detail in check_cluster(conns, attempts=3):
                rows.append({"node": "across nodes", "check": chk, "ok": passed, "detail": detail})
    finally:
        for c in conns.values():
            c.close()
    ok = all(r["ok"] for r in rows)
    return {"ok": ok, "rows": rows, "databases": len(conns),
            "message": f"All 7 rules hold on all {len(conns)} databases." if ok else "A rule is violated!"}


class NewDay(BaseModel):
    limit_l: int = 20000
    allocations: dict[str, int] | None = None


@app.post("/api/demo/new-day")
def new_day(body: NewDay | None = None):
    """Scenario 8: open the next day with the four demo farms, so every demo starts clean."""
    body = body or NewDay()
    st = get(f"{GATEWAY}/state") or {}
    current = st.get("day")
    nxt = (date.fromisoformat(current) + timedelta(days=1)) if current else date.today()
    names = list(FARMS)
    allocations = body.allocations or dict(zip(names, (6000, 5000, 4500, 4000)))
    r = http.post(f"{GATEWAY}/days", json={"day": nxt.isoformat(), "limit_l": body.limit_l,
                                           "allocations": allocations, "cmd_id": str(uuid.uuid4())}, timeout=30)
    if r.status_code != 200:
        raise HTTPException(r.status_code, r.json().get("detail", "the ledger refused to open the day"))
    result = r.json().get("result", {})
    if not result.get("ok"):
        return {"ok": False, "message": f"The ledger refused: {result.get('error')}"}
    for n, u in FARMS.items():                               # start watering again on every farm
        try:
            http.post(f"{u}/irrigation/on")
            http.post(f"{u}/link/up")
        except httpx.HTTPError:
            pass
    return {"ok": True, "day": nxt.isoformat(), "allocations": allocations,
            "message": f"Day {nxt.isoformat()} is open: {', '.join(f'{k} {v:,} L' for k, v in allocations.items())}. "
                       f"Farms receive their share on their next sync (a few seconds)."}


# ---- saved results of the long tests (run from the terminal)
@app.get("/api/results")
def results():
    out = {"files": sorted(p.name for p in RESULTS_DIR.glob("*.png")) if RESULTS_DIR.exists() else []}
    for name in ("benchmark", "failover"):
        f = RESULTS_DIR / f"{name}.csv"
        if f.exists():
            lines = [l.split(",") for l in f.read_text().strip().splitlines()]
            out[name] = [dict(zip(lines[0], row)) for row in lines[1:]]
    f = RESULTS_DIR / "recorded_runs.json"
    if f.exists():
        out["recorded"] = json.loads(f.read_text())
    return out


@app.get("/results/{name}")
def result_file(name: str):
    f = RESULTS_DIR / name
    if "/" in name or ".." in name or f.suffix not in (".png", ".csv") or not f.exists():
        raise HTTPException(404, "no such result")
    return FileResponse(f)


@app.get("/flowchart.png")
def flowchart():
    return FileResponse(Path(__file__).with_name("flowchart.png"))


@app.get("/")
def index():
    return FileResponse(Path(__file__).with_name("index.html"))
