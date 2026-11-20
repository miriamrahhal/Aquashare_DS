"""Signed farm reports: a farm's reports are accepted only with its own signature.

The first tests check the signatures alone; the others run the ledger's state machine on
the test database (like test_state_machine.py) to check the ledger's rules."""
import uuid

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from shared import signing
from tests.test_state_machine import DAY, Log, balances, events, setup_day
from tests.test_storage import dsn  # noqa: F401  (pytest fixture)
from ledger_node.state_machine import StateMachine


def consumption(farm_id, evs):
    return {"type": "record_consumption", "farm_id": farm_id, "day": DAY, "events": evs}


# ------------------------------------------------------------ the signatures themselves
def test_a_signed_report_verifies_and_a_changed_one_does_not():
    key = Ed25519PrivateKey.generate()
    report = consumption("farm-a", events(50))
    sig = signing.sign(key, report)
    assert signing.verify(signing.public_key_text(key), report, sig)
    report["events"][0]["amount_l"] = 500                       # changed on the way
    assert not signing.verify(signing.public_key_text(key), report, sig)


def test_another_key_or_no_signature_does_not_verify():
    farm, attacker = Ed25519PrivateKey.generate(), Ed25519PrivateKey.generate()
    report = consumption("farm-a", events(50))
    assert not signing.verify(signing.public_key_text(farm), report, signing.sign(attacker, report))
    assert not signing.verify(signing.public_key_text(farm), report, None)
    assert not signing.verify(signing.public_key_text(farm), report, "not-base64!")


def test_cmd_id_is_not_signed_so_a_retry_keeps_its_signature():
    key = Ed25519PrivateKey.generate()
    report = consumption("farm-a", events(50))
    sig = signing.sign(key, report)
    assert signing.verify(signing.public_key_text(key), {**report, "cmd_id": str(uuid.uuid4())}, sig)


def test_the_key_is_saved_and_reloaded(tmp_path):
    first = signing.load_or_create_key(tmp_path / "farm_key.pem")
    again = signing.load_or_create_key(tmp_path / "farm_key.pem")
    assert signing.public_key_text(first) == signing.public_key_text(again)


# ------------------------------------------------------------ the ledger's rules
def setup_signed(dsn):  # noqa: F811
    log = Log(StateMachine(dsn))
    setup_day(log)
    key = Ed25519PrivateKey.generate()
    assert log.run(type="register_key", farm_id="farm-a", public_key=signing.public_key_text(key))["ok"]
    return log, key


def test_ledger_accepts_a_correctly_signed_report(dsn):  # noqa: F811
    log, key = setup_signed(dsn)
    report = consumption("farm-a", events(50))
    result = log.run(**report, signature=signing.sign(key, report))
    assert result["ok"] and balances(log.sm)["farm-a"]["consumed"] == 50


def test_ledger_rejects_unsigned_and_forged_reports(dsn):  # noqa: F811
    log, key = setup_signed(dsn)
    report = consumption("farm-a", events(50))
    unsigned = log.run(**report)
    forged = log.run(**consumption("farm-a", events(50)),
                     signature=signing.sign(Ed25519PrivateKey.generate(), report))
    assert not unsigned["ok"] and "signature missing" in unsigned["error"]
    assert not forged["ok"] and "invalid signature" in forged["error"]
    assert balances(log.sm)["farm-a"]["consumed"] == 0


def test_ledger_rejects_a_transfer_not_signed_by_the_sender(dsn):  # noqa: F811
    log, key = setup_signed(dsn)
    transfer = {"type": "transfer", "transfer_id": str(uuid.uuid4()), "day": DAY,
                "sender": "farm-a", "recipient": "farm-b", "amount_l": 500}
    stolen = log.run(**transfer, signature=signing.sign(Ed25519PrivateKey.generate(), transfer))
    assert not stolen["ok"]
    signed = log.run(**{**transfer, "transfer_id": str(uuid.uuid4())}, signature=signing.sign(key, transfer | {"transfer_id": "x"}))
    assert not signed["ok"]                                   # signature made for another transfer
    t2 = {**transfer, "transfer_id": str(uuid.uuid4())}
    assert log.run(**t2, signature=signing.sign(key, t2))["ok"]
    assert balances(log.sm)["farm-b"]["received"] == 500


def test_a_key_cannot_be_replaced_and_farms_without_key_still_work(dsn):  # noqa: F811
    log, key = setup_signed(dsn)
    other = signing.public_key_text(Ed25519PrivateKey.generate())
    again = log.run(type="register_key", farm_id="farm-a", public_key=signing.public_key_text(key))
    replaced = log.run(type="register_key", farm_id="farm-a", public_key=other)
    assert again["ok"] and again.get("already_registered")
    assert not replaced["ok"] and "different key" in replaced["error"]
    assert log.run(**consumption("farm-b", events(30)))["ok"]  # farm-b has no key yet: accepted as before


# ------------------------------------------------------------ the farm side
def test_farm_registers_its_key_then_signs_every_report(tmp_path):
    import json

    import httpx

    from farm_node.journal import Journal
    from farm_node.sync import LedgerSync
    from farm_node.wallet import Wallet

    key = signing.load_or_create_key(tmp_path / "farm_key.pem")
    seen = []

    def gateway(request: httpx.Request):
        if request.method == "GET" and request.url.path.endswith("/transfers"):
            return httpx.Response(200, json=[])
        if request.method == "GET":
            return httpx.Response(200, json={"day": DAY, "allocated": 1000, "consumed": 0})
        seen.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"result": {"ok": True}})

    wallet = Wallet("farm-a", Journal(tmp_path / "journal.jsonl"))
    sync = LedgerSync(wallet, "farm-a", "http://gw", transport=httpx.MockTransport(gateway), key=key)
    sync.sync_once()
    wallet.consume(DAY, 20)
    sync.sync_once()
    paths = [p for p, _ in seen]
    assert paths[0] == "/farms/farm-a/key"                       # the key is sent first
    report = next(b for p, b in seen if p.endswith("/consumption"))
    command = {"type": "record_consumption", "farm_id": "farm-a", "day": report["day"], "events": report["events"]}
    assert signing.verify(signing.public_key_text(key), command, report["signature"])
