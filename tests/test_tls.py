"""Mutual TLS: only components with a certificate signed by the AquaShare CA can connect.

Each test creates fresh certificates in a temporary folder, starts a small HTTPS server
(or a Raft gRPC server) that requires a client certificate, and connects to it."""
import http.server
import ssl
import threading
from concurrent import futures

import grpc
import httpx
import pytest

from scripts.make_certs import create
from shared import tls


@pytest.fixture
def certs(tmp_path, monkeypatch):
    create(tmp_path / "certs", {"server": [], "client": []})
    create(tmp_path / "other", {"intruder": []})          # signed by a different CA
    return tmp_path


def use(monkeypatch, folder, name):
    monkeypatch.setattr(tls, "ENABLED", True)
    monkeypatch.setattr(tls, "CA", str(folder / "ca.crt"))
    monkeypatch.setattr(tls, "CERT", str(folder / f"{name}.crt"))
    monkeypatch.setattr(tls, "KEY", str(folder / f"{name}.key"))


@pytest.fixture
def https_server(certs):
    class Hello(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200); self.end_headers(); self.wfile.write(b"hello")

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Hello)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certs / "certs/server.crt", certs / "certs/server.key")
    ctx.load_verify_locations(certs / "certs/ca.crt")
    ctx.verify_mode = ssl.CERT_REQUIRED                    # mutual: the client must show a certificate
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://localhost:{srv.server_address[1]}"      # tls.fix() turns this into https://
    srv.shutdown()


def test_a_component_with_a_certificate_can_connect(https_server, certs, monkeypatch):
    use(monkeypatch, certs / "certs", "client")
    with tls.Client(timeout=5) as c:
        assert c.get(https_server).text == "hello"


def test_a_client_without_a_certificate_is_refused(https_server, certs):
    with httpx.Client(verify=ssl.create_default_context(cafile=str(certs / "certs/ca.crt")), timeout=5) as c:
        with pytest.raises(httpx.HTTPError):
            c.get(https_server.replace("http://", "https://"))


def test_a_certificate_from_another_authority_is_refused(https_server, certs, monkeypatch):
    use(monkeypatch, certs / "other", "intruder")
    monkeypatch.setattr(tls, "CA", str(certs / "certs/ca.crt"))     # trusts the server, but is not trusted back
    with tls.Client(timeout=5) as c:
        with pytest.raises(httpx.HTTPError):
            c.get(https_server)


def test_tls_off_leaves_urls_and_clients_unchanged():
    assert tls.fix("http://ledger-1:8000") == ("https://ledger-1:8000" if tls.ENABLED else "http://ledger-1:8000")
    assert not tls.ENABLED                                   # tests run with TLS off, as before


def test_raft_messages_use_mutual_tls(certs, monkeypatch):
    from ledger_node.proto import raft_pb2, raft_pb2_grpc

    class Peer(raft_pb2_grpc.RaftServicer):
        def RequestVote(self, request, context):
            return raft_pb2.VoteReply(term=request.term, vote_granted=True)

    use(monkeypatch, certs / "certs", "server")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    raft_pb2_grpc.add_RaftServicer_to_server(Peer(), server)
    port = server.add_secure_port("localhost:0", tls.grpc_server_credentials())
    server.start()
    try:
        use(monkeypatch, certs / "certs", "client")
        stub = raft_pb2_grpc.RaftStub(tls.grpc_channel(f"localhost:{port}"))
        assert stub.RequestVote(raft_pb2.VoteRequest(term=3, candidate_id="ledger-2"), timeout=5).vote_granted
        plain = raft_pb2_grpc.RaftStub(grpc.insecure_channel(f"localhost:{port}"))
        with pytest.raises(grpc.RpcError):                   # a node without TLS cannot join
            plain.RequestVote(raft_pb2.VoteRequest(term=3, candidate_id="intruder"), timeout=3)
    finally:
        server.stop(0)


def test_transport_none_still_uses_tls(https_server, certs, monkeypatch):
    use(monkeypatch, certs / "certs", "client")              # the gateway and farms pass transport=None
    with tls.Client(timeout=5, transport=None) as c:
        assert c.get(https_server).text == "hello"
