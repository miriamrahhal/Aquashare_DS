"""Keeps a farm's wallet in step with the ledger, through the gateway.

Every few seconds the farm:
  1. asks the ledger for the current day and its allocation, and records it in the wallet;
  2. sends the consumption events the ledger has not confirmed yet, oldest first;
  3. settles its outgoing transfers whose outcome is not known yet;
  4. adds to its wallet the transfers other farms sent to it.

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

from farm_node.wallet import UnknownReservation
from shared import signing, tls

log = logging.getLogger("sync")


class LedgerSync:
    def __init__(self, wallet, farm_id: str, gateway_url: str, interval: float = 5.0, transport=None, key=None):
        self.wallet, self.farm_id, self.gateway = wallet, farm_id, gateway_url.rstrip("/")
        self.interval = interval
        self.http = tls.Client(timeout=15.0, transport=transport)
        self.link_up = True            # set to False to simulate losing the farm's uplink
        self.online = False
        self.last_sync = None
        self.last_error = None
        self.ledger_balance = None
        self.transfer_lock = threading.Lock()   # one submission at a time per transfer
        self.key = key                          # the farm's private key (None: reports are not signed)
        self.key_registered = key is None

    def _sign(self, command: dict, body: dict) -> dict:
        if self.key is not None:
            body["signature"] = signing.sign(self.key, command)
        return body

    def _register_key(self):
        """Send the farm's public key to the ledger once, before any signed report."""
        public = signing.public_key_text(self.key)
        reply = self._post(f"/farms/{self.farm_id}/key", {"public_key": public, "cmd_id": f"key-{self.farm_id}-{public[:16]}"})
        if reply.status_code == 200:
            self.key_registered = True
            log.info("public key registered in the ledger")
        else:
            raise RuntimeError(f"the ledger refused this farm's key ({reply.status_code}): {reply.text[:200]}")

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
            if not self.key_registered:
                self._register_key()
            while True:
                day, batch = self.wallet.pending_consumption()
                if not batch:
                    break
                events = [{"event_id": e["event_id"], "seq": e["seq"], "amount_l": e["amount"]} for e in batch]
                command = {"type": "record_consumption", "farm_id": self.farm_id, "day": day, "events": events}
                reply = self._post(f"/farms/{self.farm_id}/consumption",
                                   self._sign(command, {"day": day, "events": events, "cmd_id": str(uuid.uuid4())}))
                if reply.status_code != 200:
                    raise RuntimeError(f"ledger answered {reply.status_code}: {reply.text[:200]}")
                self.wallet.mark_synced(day, batch[-1]["seq"])
                sent += len(batch)
            for transfer_id in self.wallet.open_reservations():
                if self.submit_transfer(transfer_id)["status"] == "pending":
                    raise RuntimeError("ledger did not settle a transfer")
            self._credit_incoming()
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

    # ------------------------------------------------------------ transfers
    def submit_transfer(self, transfer_id: str) -> dict:
        """Step 2 of a transfer: ask the ledger to commit a transfer already reserved in the wallet.

        committed -> the reservation becomes a confirmed send (step 3)
        rejected  -> the ledger refused it, so the reserved water is released
        pending   -> the outcome is unknown (no answer); the water stays reserved and the
                     same transfer is sent again later. It is never released on a guess,
                     because the ledger may have committed it.
        """
        with self.transfer_lock:
            info = self.wallet.open_reservations().get(transfer_id)
            if info is None:
                return {"status": "settled"}
            body = {"transfer_id": transfer_id, "day": info["day"], "sender": self.farm_id,
                    "recipient": info["recipient"], "amount_l": info["amount"],
                    "cmd_id": f"transfer-{transfer_id}"}          # the same cmd_id on every attempt
            self._sign({"type": "transfer", **body}, body)
            try:
                reply = self._post("/transfers", body)
            except httpx.HTTPError as exc:
                return {"status": "pending", "reason": str(exc)}
            try:
                if reply.status_code == 200:
                    self.wallet.confirm_sent(info["day"], transfer_id)
                    log.info("transfer %s: %d L to %s committed", transfer_id[:8], info["amount"], info["recipient"])
                    return {"status": "committed", "committed_index": reply.json()["committed_index"]}
                if reply.status_code == 409:
                    reason = reply.json().get("detail", {}).get("error")
                    self.wallet.release(info["day"], transfer_id)
                    log.warning("transfer %s rejected by the ledger (%s): reservation released", transfer_id[:8], reason)
                    return {"status": "rejected", "reason": reason}
            except UnknownReservation:
                return {"status": "settled"}
            return {"status": "pending", "reason": f"ledger answered {reply.status_code}"}

    def _credit_incoming(self):
        """Add to the wallet every committed transfer sent to this farm that is not credited yet."""
        reply = self._get(f"/farms/{self.farm_id}/transfers")
        if reply.status_code != 200:
            return
        for t in reply.json():
            if t["recipient"] == self.farm_id and t["status"] == "committed":
                if self.wallet.credit(t["day"], t["transfer_id"], t["amount_l"]):
                    log.info("received %d L from %s (transfer %s)", t["amount_l"], t["sender"], t["transfer_id"][:8])

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