"""Mutual TLS between AquaShare components.

Off by default, so the system and the tests behave exactly as before. It is turned on by
the docker-compose.tls.yml override, which sets for every container:

  TLS_ENABLED=1
  TLS_CA=/certs/ca.crt            the AquaShare certificate authority everyone trusts
  TLS_CERT=/certs/<name>.crt      this component's own certificate (its identity)
  TLS_KEY=/certs/<name>.key       and its private key

With TLS on, every HTTP call and every Raft (gRPC) message is encrypted, and both sides
must show a certificate signed by the AquaShare CA: a machine without one cannot connect.

Scripts run from your own computer call for_host() first. It reads TLS_ENABLED from the
project's .env file and uses certs/operator.crt as their identity.
"""
import os
import ssl
from pathlib import Path

import httpx

ENABLED = os.getenv("TLS_ENABLED") == "1"
CA = os.getenv("TLS_CA", "/certs/ca.crt")
CERT = os.getenv("TLS_CERT")
KEY = os.getenv("TLS_KEY")
ROOT = Path(__file__).resolve().parent.parent


def for_host() -> bool:
    """Used by the scripts in scripts/: follow the project's .env and act as the operator."""
    global ENABLED, CA, CERT, KEY
    env = {}
    dotenv = ROOT / ".env"
    if dotenv.exists():
        for line in dotenv.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    ENABLED = os.getenv("TLS_ENABLED", env.get("TLS_ENABLED", "0")) == "1"
    certs = ROOT / "certs"
    CA, CERT, KEY = str(certs / "ca.crt"), str(certs / "operator.crt"), str(certs / "operator.key")
    return ENABLED


def fix(url: str) -> str:
    """http:// becomes https:// when TLS is on, so the same URLs work in both modes."""
    if ENABLED and url.startswith("http://"):
        return "https://" + url[len("http://"):]
    return url


def ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context(cafile=CA)          # trust only the AquaShare CA
    ctx.load_cert_chain(CERT, KEY)                       # and show our own certificate
    return ctx


def httpx_kwargs() -> dict:
    return {"verify": ssl_context()} if ENABLED else {}


class Client(httpx.Client):
    """An httpx.Client that uses mutual TLS when it is on (and is a plain client otherwise)."""

    def __init__(self, **kwargs):
        if ENABLED and "transport" not in kwargs:
            kwargs = {**httpx_kwargs(), **kwargs}
        super().__init__(**kwargs)

    def request(self, method, url, **kwargs):
        return super().request(method, fix(str(url)), **kwargs)


_shared = None


def _client() -> Client:
    global _shared
    if _shared is None:
        _shared = Client()
    return _shared


def get(url, **kwargs):
    return _client().get(url, **kwargs)


def post(url, **kwargs):
    return _client().post(url, **kwargs)


# ------------------------------------------------------------------ gRPC (Raft messages)
def grpc_server_credentials():
    import grpc
    return grpc.ssl_server_credentials(
        [(Path(KEY).read_bytes(), Path(CERT).read_bytes())],
        root_certificates=Path(CA).read_bytes(),
        require_client_auth=True)                        # the "mutual" part: peers must show a certificate


def grpc_channel(target: str, options=None):
    import grpc
    if not ENABLED:
        return grpc.insecure_channel(target, options=options)
    creds = grpc.ssl_channel_credentials(root_certificates=Path(CA).read_bytes(),
                                         private_key=Path(KEY).read_bytes(),
                                         certificate_chain=Path(CERT).read_bytes())
    return grpc.secure_channel(target, creds, options=options)
