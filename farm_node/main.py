"""Farm node: simulated sensors and irrigation, spending only from the local escrow wallet.

The water the farm may use comes from the ledger: the sync client learns each day's
allocation through the gateway and reports consumption back. The farm keeps irrigating
while it is offline, from the water it already holds.
"""
import logging
import os
import random
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from farm_node.journal import Journal
from farm_node.sync import LedgerSync
from farm_node.wallet import InsufficientWater, Wallet
from shared import signing

FARM_ID = os.getenv("FARM_ID", "farm-a")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")) / FARM_ID
GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8600")
SYNC_SECONDS = float(os.getenv("SYNC_SECONDS", "5"))
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "2"))
FLOW_L_PER_TICK = int(os.getenv("FLOW_L_PER_TICK", "20"))
MOISTURE_ON, MOISTURE_OFF = 35.0, 60.0   # start irrigating below 35%, stop above 60%

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)     # keep the farm's own messages readable
log = logging.getLogger(FARM_ID)

wallet = Wallet(FARM_ID, Journal(DATA_DIR / "journal.jsonl"))
SIGN_REPORTS = os.getenv("SIGN_REPORTS", "1") == "1"
key = signing.load_or_create_key(DATA_DIR / "farm_key.pem") if SIGN_REPORTS else None
sync = LedgerSync(wallet, FARM_ID, GATEWAY_URL, interval=SYNC_SECONDS, key=key)
sensors = {"soil_moisture": 50.0, "flow_l_per_tick": 0, "valve_open": False, "irrigation_enabled": True,
           "quota_exhausted": False}


def irrigation_loop() -> None:
    """Simulate soil drying and irrigation. Water is spent only if the wallet allows it."""
    while True:
        time.sleep(TICK_SECONDS)
        sensors["soil_moisture"] = max(0.0, sensors["soil_moisture"] - random.uniform(0.5, 1.5))
        day = wallet.current_day()
        wants_water = day and sensors["irrigation_enabled"] and (
            sensors["soil_moisture"] < MOISTURE_ON or (sensors["valve_open"] and sensors["soil_moisture"] < MOISTURE_OFF)
        )
        if not wants_water:
            sensors.update(valve_open=False, flow_l_per_tick=0)
            continue
        try:
            wallet.consume(day, FLOW_L_PER_TICK)
            sensors.update(valve_open=True, flow_l_per_tick=FLOW_L_PER_TICK, quota_exhausted=False)
            sensors["soil_moisture"] = min(100.0, sensors["soil_moisture"] + random.uniform(2.0, 3.0))
        except InsufficientWater:
            if not sensors["quota_exhausted"]:
                log.warning("Quota exhausted for %s: valve closed", day)
            sensors.update(valve_open=False, flow_l_per_tick=0, quota_exhausted=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    log.info("Farm %s started; ledger day %s; gateway %s", FARM_ID, wallet.current_day(), GATEWAY_URL)
    sync.start()
    threading.Thread(target=irrigation_loop, name="irrigation", daemon=True).start()
    yield


app = FastAPI(title=f"AquaShare farm node {FARM_ID}", lifespan=lifespan)


class Amount(BaseModel):
    amount: int


class TransferRequest(BaseModel):
    recipient: str
    amount_l: int

def current_day() -> str:
    day = wallet.current_day()
    if day is None:
        raise HTTPException(409, "no allocation from the ledger yet")
    return day


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "farm_id": FARM_ID}


@app.get("/wallet")
def get_wallet() -> dict:
    return wallet.balance(current_day())


@app.get("/sensors")
def get_sensors() -> dict:
    return {"farm_id": FARM_ID, **sensors}


@app.post("/consume")
def consume(body: Amount) -> dict:
    """Manual consumption, used by tests and demos."""
    try:
        return wallet.consume(current_day(), body.amount)
    except InsufficientWater as exc:
        raise HTTPException(409, str(exc))
    except ValueError as exc:
        raise HTTPException(422, str(exc))


@app.post("/irrigation/{state}")
def set_irrigation(state: str) -> dict:
    if state not in ("on", "off"):
        raise HTTPException(422, "state must be on or off")
    sensors["irrigation_enabled"] = state == "on"
    return {"irrigation_enabled": sensors["irrigation_enabled"]}

@app.post("/transfers")
def send_transfer(body: TransferRequest) -> dict:
    """Send water to another farm: reserve locally, commit on the ledger, then confirm locally."""
    day = current_day()
    transfer_id = str(uuid.uuid4())
    try:
        wallet.reserve(day, transfer_id, body.amount_l, recipient=body.recipient)   # step 1: reserve
    except InsufficientWater as exc:
        raise HTTPException(409, str(exc))
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    outcome = sync.submit_transfer(transfer_id)                                    # steps 2 and 3
    return {"transfer_id": transfer_id, **outcome, "wallet": wallet.balance(day)}


@app.get("/transfers/open")
def open_transfers() -> dict:
    """Transfers reserved locally whose outcome on the ledger is not known yet."""
    return wallet.open_reservations()

@app.get("/key")
def public_key() -> dict:
    """This farm's public key (never the private one), so the operator can register it again."""
    return {"farm_id": FARM_ID, "public_key": signing.public_key_text(key) if key else None,
            "registered": sync.key_registered}


@app.get("/sync")
def sync_status() -> dict:
    return sync.status()


@app.post("/link/{state}")
def set_link(state: str) -> dict:
    """Simulate losing (down) or recovering (up) the farm's connection to the gateway."""
    if state not in ("up", "down"):
        raise HTTPException(422, "state must be up or down")
    sync.link_up = state == "up"
    log.warning("uplink %s", state.upper())
    return {"link": state}


@app.get("/journal")
def journal(limit: int = 20) -> list[dict]:
    return wallet.events(limit)



