"""Analytics service: consumes ledger events from RabbitMQ and keeps running totals.

It is completely decoupled from the ledger: it never calls the ledger, and the ledger never
waits for it. If it is stopped, events wait in its durable RabbitMQ queue; when it starts
again it processes them in order. A message is acknowledged only after its event is stored,
so a crash in between makes RabbitMQ deliver it again, and the store ignores the duplicate.
"""
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from analytics.bus import QUEUE, connect
from analytics.store import Store

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://aquashare:aquashare_dev@localhost:5673/%2F")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data/analytics"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger("analytics")

store = Store(DATA_DIR / "analytics.db")
status = {"connected": False, "received": 0}


def consume_forever() -> None:
    while True:
        try:
            connection, channel = connect(RABBITMQ_URL)
            channel.basic_qos(prefetch_count=50)
            status["connected"] = True
            log.info("connected to RabbitMQ; consuming queue %s", QUEUE)
            for method, _props, body in channel.consume(QUEUE, inactivity_timeout=1):
                if method is None:
                    continue
                event = json.loads(body)
                new = store.apply(event)                      # stored first ...
                channel.basic_ack(method.delivery_tag)        # ... then acknowledged
                status["received"] += 1
                if not new:
                    log.info("ignored duplicate event at log index %d", event["log_index"])
        except Exception as exc:                              # broker down or connection lost
            status["connected"] = False
            log.warning("RabbitMQ unavailable (%s); retrying in 3 s", type(exc).__name__)
            time.sleep(3)


@asynccontextmanager
async def lifespan(_: FastAPI):
    threading.Thread(target=consume_forever, name="consumer", daemon=True).start()
    yield


app = FastAPI(title="AquaShare analytics", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", **status}


@app.get("/summary")
def summary(day: str | None = None) -> dict:
    return {**status, **store.summary(day)}
