"""Digital signatures for farm reports (Ed25519).

Each farm has its own key pair. The private key never leaves the farm; the public key is
registered once in the ledger. From then on, every consumption report and every transfer
sent by that farm must carry a signature made with its private key, and the ledger checks
it before applying the command. A report sent by someone else, or changed on the way,
does not match the signature and is rejected.

The signature covers only the fields that describe what the farm asked for (who, which
day, which events or which transfer, how much). It does not cover cmd_id, because the
farm and the gateway use cmd_id only to apply a command once.
"""
import base64
import json
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

SIGNED_FIELDS = {
    "record_consumption": ("type", "farm_id", "day", "events"),
    "transfer": ("type", "transfer_id", "day", "sender", "recipient", "amount_l"),
}


def signed_message(command: dict) -> bytes:
    """The exact bytes that are signed: the command's important fields, in a fixed order."""
    body = {k: command[k] for k in SIGNED_FIELDS[command["type"]]}
    if "events" in body:
        body["events"] = [{"event_id": str(e["event_id"]), "seq": int(e["seq"]), "amount_l": int(e["amount_l"])}
                          for e in body["events"]]
    if "amount_l" in body:
        body["amount_l"] = int(body["amount_l"])
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def load_or_create_key(path: Path) -> Ed25519PrivateKey:
    """Load the farm's private key, or create one the first time the farm starts."""
    path = Path(path)
    if path.exists():
        return serialization.load_pem_private_key(path.read_bytes(), password=None)
    key = Ed25519PrivateKey.generate()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                       serialization.NoEncryption()))
    return key


def public_key_text(key: Ed25519PrivateKey) -> str:
    raw = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return base64.b64encode(raw).decode()


def sign(key: Ed25519PrivateKey, command: dict) -> str:
    return base64.b64encode(key.sign(signed_message(command))).decode()


def verify(public_key: str, command: dict, signature: str | None) -> bool:
    """True only if the signature was made by the owner of public_key over this exact command."""
    if not signature:
        return False
    try:
        pub = Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key))
        pub.verify(base64.b64decode(signature), signed_message(command))
        return True
    except (InvalidSignature, ValueError, KeyError, TypeError):
        return False
