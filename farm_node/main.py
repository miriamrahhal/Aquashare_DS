"""Farm node: simulated sensors and irrigation, spending only from the local escrow wallet."""
import logging
import os
import random
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from farm_node.journal import Journal
from farm_node.wallet import InsufficientWater, Wallet

FARM_ID = os.getenv("FARM_ID", "farm-a")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")) / FARM_ID
INITIAL_QUOTA_L = int(os.getenv("INITIAL_QUOTA_L", "10000"))   # temporary: replaced by ledger allocations
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "2"))
FLOW_L_PER_TICK = int(os.getenv("FLOW_L_PER_TICK", "20"))
MOISTURE_ON, MOISTURE_OFF = 35.0, 60.0   # start irrigating below 35%, stop above 60%

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(FARM_ID)

wallet = Wallet(FARM_ID, Journal(DATA_DIR / "journal.jsonl"))
sensors = {"soil_moisture": 50.0, "flow_l_per_tick": 0, "valve_open": False, "irrigation_enabled": True,
           "quota_exhausted": False}


def today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def irrigation_loop() -> None:
    """Simulate soil drying and irrigation. Water is spent only if the wallet allows it."""
    while True:
        time.sleep(TICK_SECONDS)
        sensors["soil_moisture"] = max(0.0, sensors["soil_moisture"] - random.uniform(0.5, 1.5))
        wants_water = sensors["irrigation_enabled"] and (
            sensors["soil_moisture"] < MOISTURE_ON or (sensors["valve_open"] and sensors["soil_moisture"] < MOISTURE_OFF)
        )
        if not wants_water:
            sensors.update(valve_open=False, flow_l_per_tick=0)
            continue
        try:
            wallet.consume(today(), FLOW_L_PER_TICK)
            sensors.update(valve_open=True, flow_l_per_tick=FLOW_L_PER_TICK, quota_exhausted=False)
            sensors["soil_moisture"] = min(100.0, sensors["soil_moisture"] + random.uniform(2.0, 3.0))
        except InsufficientWater:
            if not sensors["quota_exhausted"]:
                log.warning("Quota exhausted for %s: valve closed", today())
            sensors.update(valve_open=False, flow_l_per_tick=0, quota_exhausted=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not wallet.has_allocation(today()):
        wallet.set_allocation(today(), INITIAL_QUOTA_L)
    log.info("Farm %s started: %s", FARM_ID, wallet.balance(today()))
    threading.Thread(target=irrigation_loop, name="irrigation", daemon=True).start()
    yield


app = FastAPI(title=f"AquaShare farm node {FARM_ID}", lifespan=lifespan)


class Amount(BaseModel):
    amount: int


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "farm_id": FARM_ID}


@app.get("/wallet")
def get_wallet() -> dict:
    return wallet.balance(today())


@app.get("/sensors")
def get_sensors() -> dict:
    return {"farm_id": FARM_ID, **sensors}


@app.post("/consume")
def consume(body: Amount) -> dict:
    """Manual consumption, used by tests and demos."""
    try:
        return wallet.consume(today(), body.amount)
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


@app.get("/journal")
def journal(limit: int = 20) -> list[dict]:
    return wallet.events(limit)