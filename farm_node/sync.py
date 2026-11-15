"""Keeps a farm's wallet in step with the ledger, through the gateway.

Every few seconds the farm:
  1. asks the ledger for the current day and its allocation, and records it in the wallet;
  2. sends the consumption events the ledger has not confirmed yet, oldest first.

If the gateway cannot be reached, the farm is offline. Irrigation does not stop: the wallet
still holds the water the ledger granted, and every litre used is written to the journal.
When the link comes back, the pending events are sent. Each event has its own event_id,
so an event sent twice (for example if the answer was lost) is counted only once.
"""
import logging
import threading
import time
import uuid

import httpx

log = logging.getLogger("sync")


class LedgerSync:
    def __init__(self, wallet, farm_id: str, gateway_url: str, interval: float = 5.0, transport=None):
        self.wallet, self.farm_id, self.gateway = wallet, farm_id, gateway_url.rstrip("/")
        self.interval = interval
        self.http = httpx.Client(timeout=15.0, transport=transport)
        self.link_up = True            # set to False to simulate losing the farm's uplink
        self.online = False
        self.last_sync = None
        self.last_error = None
        self.ledger_balance = None

    def _get(self, path):
        if not self.link_up:
            raise httpx.ConnectError("uplink is down")
        return self.http.get(f"{self.gateway}{path}")

    def _post(self, path, body):
        if not self.link_up:
            raise httpx.ConnectError("uplink is down")
        return self.http.post(f"{self.gateway}{path}", json=body)

    def sync_once(self) -> dict:
        """One round of synchronisation. Returns what happened, for logs and tests."""
        sent = 0
        try:
            reply = self._get(f"/farms/{self.farm_id}/balance")
            if reply.status_code == 200:
                ledger = reply.json()
                if self.wallet.set_ledger_allocation(ledger["day"], ledger["allocated"]):
                    log.info("ledger opened day %s: allocation %d L", ledger["day"], ledger["allocated"])
            while True:
                day, batch = self.wallet.pending_consumption()
                if not batch:
                    break
                events = [{"event_id": e["event_id"], "seq": e["seq"], "amount_l": e["amount"]} for e in batch]
                reply = self._post(f"/farms/{self.farm_id}/consumption",
                                   {"day": day, "events": events, "cmd_id": str(uuid.uuid4())})
                if reply.status_code != 200:
                    raise RuntimeError(f"ledger answered {reply.status_code}: {reply.text[:200]}")
                self.wallet.mark_synced(day, batch[-1]["seq"])
                sent += len(batch)
            reply = self._get(f"/farms/{self.farm_id}/balance")
            self.ledger_balance = reply.json() if reply.status_code == 200 else None
            if not self.online:
                log.info("online: connected to the ledger through %s", self.gateway)
            self.online, self.last_sync, self.last_error = True, time.time(), None
            if sent:
                log.info("synced %d consumption events to the ledger", sent)
        except (httpx.HTTPError, RuntimeError) as exc:
            if self.online or self.last_error is None:
                log.warning("offline: %s (%d events waiting)", exc, self.wallet.pending_count())
            self.online, self.last_error = False, str(exc)
        return {"online": self.online, "sent": sent, "pending": self.wallet.pending_count()}

    def run_forever(self):
        while True:
            self.sync_once()
            time.sleep(self.interval)

    def start(self):
        threading.Thread(target=self.run_forever, name="ledger-sync", daemon=True).start()

    def status(self) -> dict:
        day = self.wallet.current_day()
        local = self.wallet.balance(day) if day else None
        return {
            "farm_id": self.farm_id,
            "link": "up" if self.link_up else "down",
            "online": self.online,
            "current_day": day,
            "pending_events": self.wallet.pending_count(),
            "local_consumed": local["consumed"] if local else None,
            "ledger_consumed": self.ledger_balance.get("consumed") if self.ledger_balance else None,
            "seconds_since_last_sync": round(time.time() - self.last_sync, 1) if self.last_sync else None,
            "last_error": self.last_error,
        }