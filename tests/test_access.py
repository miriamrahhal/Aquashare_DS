"""Authorization with mutual TLS: each certificate may only do what its role allows.

A real gateway is started over HTTPS with shared.serve. No ledger is running, so a request
that passes the access rules ends with 503 (no ledger reachable), and a request that does
not pass is stopped earlier with 403 Forbidden."""
import os
import socket
import ssl
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from scripts.make_certs import create

ROOT = Path(__file__).resolve().parent.parent
NAMES = ["gateway-1", "gateway-lb", "operator", "dashboard", "farm-a", "farm-b"]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def gateway(tmp_path_factory):
    certs = tmp_path_factory.mktemp("certs")
    create(certs, {n: [] for n in NAMES})
    port = free_port()
    env = {**os.environ, "PORT": str(port), "PYTHONPATH": str(ROOT),
           "TLS_ENABLED": "1", "TLS_CA": str(certs / "ca.crt"),
           "TLS_CERT": str(certs / "gateway-1.crt"), "TLS_KEY": str(certs / "gateway-1.key"),
           "UVICORN_SSL_CERTFILE": str(certs / "gateway-1.crt"), "UVICORN_SSL_KEYFILE": str(certs / "gateway-1.key"),
           "UVICORN_SSL_CA_CERTS": str(certs / "ca.crt"), "UVICORN_SSL_CERT_REQS": "2",
           "LEDGER_NODES": "ledger-1=http://127.0.0.1:9", "RETRY_SECONDS": "0.3"}
    proc = subprocess.Popen([sys.executable, "-m", "shared.serve", "gateway.main:app"], cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = f"https://localhost:{port}"
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.1)
    yield url, certs
    proc.terminate()
    proc.wait(5)


def as_(certs, name):
    ctx = ssl.create_default_context(cafile=str(certs / "ca.crt"))
    ctx.load_cert_chain(certs / f"{name}.crt", certs / f"{name}.key")
    return httpx.Client(verify=ctx, timeout=10)


DAY = {"day": "2026-12-01", "limit_l": 100, "allocations": {"farm-a": 100}}
USE = {"day": "2026-12-01", "events": [{"event_id": "e1", "seq": 1, "amount_l": 5}]}
TRANSFER = {"day": "2026-12-01", "sender": "farm-a", "recipient": "farm-b", "amount_l": 5}


@pytest.mark.parametrize("who,path,body,expected", [
    ("farm-a", "/days", DAY, 403),                           # a farm cannot open a day
    ("operator", "/days", DAY, 503),                         # the operator can (then: no ledger running)
    ("dashboard", "/days", DAY, 503),
    ("farm-b", "/farms/farm-a/consumption", USE, 403),       # farm-b cannot report for farm-a
    ("farm-a", "/farms/farm-a/consumption", USE, 503),       # farm-a can report for itself
    ("farm-b", "/transfers", TRANSFER, 403),                 # farm-b cannot send farm-a's water
    ("farm-a", "/transfers", TRANSFER, 503),
    ("farm-a", "/farms/farm-a/key/replace", {"public_key": "x"}, 403),   # a farm cannot swap its own key
    ("operator", "/farms/farm-a/key/replace", {"public_key": "x"}, 503),
])
def test_each_certificate_can_only_do_its_own_job(gateway, who, path, body, expected):
    url, certs = gateway
    with as_(certs, who) as c:
        assert c.post(url + path, json=body).status_code == expected


def test_reading_is_open_to_every_certificate(gateway):
    url, certs = gateway
    with as_(certs, "farm-a") as c:
        assert c.get(url + "/health").status_code == 200


def test_a_farm_cannot_fake_the_load_balancer_header(gateway):
    url, certs = gateway
    with as_(certs, "farm-a") as c:
        r = c.post(url + "/days", json=DAY, headers={"X-Client-CN": "operator"})
    assert r.status_code == 403                              # the header is only trusted from gateway-lb


def test_the_load_balancer_passes_on_the_real_caller(gateway):
    url, certs = gateway
    with as_(certs, "gateway-lb") as c:
        assert c.post(url + "/days", json=DAY, headers={"X-Client-CN": "farm-a"}).status_code == 403
        assert c.post(url + "/days", json=DAY, headers={"X-Client-CN": "operator"}).status_code == 503
