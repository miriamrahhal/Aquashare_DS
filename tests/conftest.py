"""Every test starts with TLS off.

Some tests import scripts such as verify_proof.py, and the scripts read the project's .env
when they are loaded. If .env turns TLS on (for the running Docker system), that must not
leak into the unit tests. The TLS tests turn it on themselves, with their own certificates."""
import pytest

from shared import tls


@pytest.fixture(autouse=True)
def tls_off(monkeypatch):
    monkeypatch.setattr(tls, "ENABLED", False)
    monkeypatch.setattr(tls, "_shared", None)
