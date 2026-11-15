"""Event relay: publishes every change committed on the ledger to RabbitMQ.

The relay reads applied commands from any ledger node, in log order, and publishes each
one as a persistent message with publisher confirms. It saves its position (the last log
index it published) after each message. If it crashes between publishing and saving, the
same event is published again after the restart; every event carries its log index, and
the consumers ignore an index they have already processed. Delivery is therefore
at-least-once, and processing is effectively exactly-once.

The ledger itself never waits for RabbitMQ: if the broker is down, commands are still
committed, and the relay catches up when the broker returns.
"""
import json
import logging
import os
import time
from pathlib import Path

import httpx
import pika

from analytics.bus import EXCHANGE, connect
from analytics.events import to_event

log = logging.getLogger("relay")


class RabbitPublisher:
    def __init__(self, url: str):
        self.url, self.connection, self.channel = url, None, None

    def publish(self, event: dict) -> None:
        if self.connection is None or self.connection.is_closed:
            self.connection, self.channel = connect(self.url)
            self.channel.confirm_delivery()                     # basic_publish waits for the broker's ack
        self.channel.basic_publish(
            EXCHANGE, routing_key=event["type"], body=json.dumps(event),
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json",
                                            message_id=str(event["log_index"])))

    def reset(self) -> None:
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass
        self.connection = None


class Relay:
    def __init__(self, nodes: list[str], publisher, position_file: Path, transport=None):
        self.nodes, self.publisher, self.position_file = nodes, publisher, Path(position_file)
        self.http = httpx.Client(timeout=5.0, transport=transport)
        self.position = int(self.position_file.read_text()) if self.position_file.exists() else 0
        self.published = 0

    def _save(self) -> None:
        tmp = self.position_file.with_suffix(".tmp")
        tmp.write_text(str(self.position))
        os.replace(tmp, self.position_file)                     # atomic: never a half-written file

    def fetch(self) -> list[dict] | None:
        for url in self.nodes:                                  # every node holds the same applied state
            try:
                reply = self.http.get(f"{url}/events", params={"after": self.position, "limit": 200})
                if reply.status_code == 200:
                    return reply.json()
            except httpx.HTTPError:
                continue
        return None

    def run_once(self) -> dict:
        items = self.fetch()
        if items is None:
            return {"ledger": "unreachable", "published": 0, "position": self.position}
        published = 0
        for item in items:
            event = to_event(item)
            if event is not None:
                self.publisher.publish(event)                   # raises if the broker is unreachable
                published += 1
            self.position = item["log_index"]
            self._save()
        self.published += published
        return {"ledger": "ok", "published": published, "position": self.position}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("pika").setLevel(logging.WARNING)
    nodes = [u.strip() for u in os.getenv("LEDGER_NODES", "http://localhost:8701").split(",") if u.strip()]
    data = Path(os.getenv("DATA_DIR", "./data/relay"))
    data.mkdir(parents=True, exist_ok=True)
    publisher = RabbitPublisher(os.getenv("RABBITMQ_URL", "amqp://aquashare:aquashare_dev@localhost:5673/%2F"))
    relay = Relay(nodes, publisher, data / "position.txt")
    log.info("relay started at log index %d; ledger nodes %s", relay.position, nodes)
    while True:
        try:
            result = relay.run_once()
            if result["published"]:
                log.info("published %d events (up to log index %d)", result["published"], result["position"])
        except (pika.exceptions.AMQPError, OSError) as exc:
            log.warning("RabbitMQ unavailable (%s); will retry from log index %d", type(exc).__name__, relay.position)
            publisher.reset()
            time.sleep(3)
        time.sleep(1)


if __name__ == "__main__":
    main()
