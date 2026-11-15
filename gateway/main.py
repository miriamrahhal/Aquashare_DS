"""Gateway: the single entry point for farms and operators.

A gateway keeps no data of its own. It turns simple REST requests into ledger commands,
finds the current Raft leader, and retries on another node when a node is down or a new
leader is being elected. Because every command carries a cmd_id that the ledger applies
only once, retrying is always safe, even if the first attempt was committed after all.
Several gateways can run side by side behind a load balancer.
"""
import logging
import os
import time
import uuid

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

GATEWAY_ID = os.getenv("GATEWAY_ID", "gateway-1")
LEDGER_NODES = dict(item.split("=", 1) for item in os.getenv(
    "LEDGER_NODES",
    "ledger-1=http://localhost:8701,ledger-2=http://localhost:8702,ledger-3=http://localhost:8703").split(",") if item)
RETRY_SECONDS = float(os.getenv("RETRY_SECONDS", "10"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(GATEWAY_ID)


class LedgerUnavailable(Exception):
    pass


class LedgerClient:
    """Finds the leader and retries. The only thing it remembers is which node led last time."""

    def __init__(self, nodes: dict, transport=None, retry_seconds=RETRY_SECONDS, pause=0.2):
        self.nodes = list(nodes.values())                  # URLs, in a fixed order
        self.url_of = dict(nodes)                          # node id -> URL
        self.http = httpx.Client(timeout=6.0, transport=transport)
        self.retry_seconds, self.pause = retry_seconds, pause
        self.leader_url = None

    def _order(self):
        return ([self.leader_url] if self.leader_url else []) + [n for n in self.nodes if n != self.leader_url]

    def submit(self, command: dict) -> dict:
        command.setdefault("cmd_id", str(uuid.uuid4()))       # the same cmd_id is used for every retry
        deadline, attempts = time.monotonic() + self.retry_seconds, 0
        while True:
            for url in self._order():
                attempts += 1
                try:
                    reply = self.http.post(f"{url}/command", json={"command": command})
                except httpx.HTTPError:
                    if url == self.leader_url:
                        self.leader_url = None
                    continue
                if reply.status_code == 200:
                    body = reply.json()
                    self.leader_url = self.url_of.get(body["leader_id"], url)
                    return {**body, "gateway": GATEWAY_ID, "attempts": attempts, "cmd_id": command["cmd_id"]}
                if url == self.leader_url:
                    self.leader_url = None                     # 503: no leader or no majority right now
            if time.monotonic() >= deadline:
                raise LedgerUnavailable(f"no ledger majority reachable after {attempts} attempts")
            time.sleep(self.pause)

    def read(self, path: str, params=None) -> dict:
        """Reads go to the leader when it is known (most recent state), otherwise to any live node."""
        for url in self._order():
            try:
                reply = self.http.get(f"{url}{path}", params=params)
                if reply.status_code == 200:
                    return reply.json()
            except httpx.HTTPError:
                continue
        raise LedgerUnavailable("no ledger node reachable")


# ---------------------------------------------------------------------------- API
class Farm(BaseModel):
    farm_id: str
    name: str
    cmd_id: str | None = None


class Day(BaseModel):
    day: str
    limit_l: int
    allocations: dict[str, int]
    cmd_id: str | None = None


class ConsumptionEvent(BaseModel):
    event_id: str
    seq: int
    amount_l: int


class Consumption(BaseModel):
    day: str
    events: list[ConsumptionEvent]
    cmd_id: str | None = None


class Transfer(BaseModel):
    transfer_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    day: str
    sender: str
    recipient: str
    amount_l: int
    cmd_id: str | None = None


def create_app(ledger: LedgerClient) -> FastAPI:
    app = FastAPI(title=f"AquaShare {GATEWAY_ID}")


    def run(command: dict) -> dict:
        try:
            body = ledger.submit({k: v for k, v in command.items() if v is not None})
        except LedgerUnavailable as exc:
            raise HTTPException(503, str(exc))
        if not body["result"].get("ok"):
            raise HTTPException(409, {"error": body["result"].get("error"), "committed_index": body["committed_index"]})
        return body

    def read(path, params=None):
        try:
            return ledger.read(path, params)
        except LedgerUnavailable as exc:
            raise HTTPException(503, str(exc))

    @app.get("/health")
    def health():
        return {"status": "ok", "gateway": GATEWAY_ID}

    @app.get("/cluster")
    def cluster():
        nodes = []
        for url in ledger.nodes:
            try:
                nodes.append(ledger.http.get(f"{url}/status", timeout=1.0).json())
            except httpx.HTTPError:
                nodes.append({"url": url, "role": "down"})
        return {"gateway": GATEWAY_ID, "nodes": nodes}

    @app.post("/farms")
    def register_farm(farm: Farm):
        return run({"type": "register_farm", **farm.model_dump()})

    @app.post("/days")
    def open_day(day: Day):
        return run({"type": "open_day", **day.model_dump()})

    @app.post("/farms/{farm_id}/consumption")
    def record_consumption(farm_id: str, c: Consumption):
        return run({"type": "record_consumption", "farm_id": farm_id, **c.model_dump()})

    @app.post("/transfers")
    def transfer(t: Transfer):
        return run({"type": "transfer", **t.model_dump()})

    @app.get("/state")
    def state(day: str | None = None):
        return {"gateway": GATEWAY_ID, **read("/state", {"day": day} if day else None)}

    @app.get("/farms/{farm_id}/balance")
    def balance(farm_id: str, day: str | None = None):
        st = read("/state", {"day": day} if day else None)
        for f in st.get("farms", []):
            if f["farm_id"] == farm_id:
                return {"gateway": GATEWAY_ID, "day": st["day"], **f}
        raise HTTPException(404, f"no balance for {farm_id} on {st.get('day')}")

    @app.get("/farms/{farm_id}/transfers")
    def farm_transfers(farm_id: str, day: str | None = None):
        params = {"farm": farm_id, **({"day": day} if day else {})}
        return read("/transfers", params)

    return app


app = create_app(LedgerClient(LEDGER_NODES))