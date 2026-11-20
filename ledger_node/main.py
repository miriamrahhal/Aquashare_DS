"""Ledger node: runs Raft over gRPC and exposes an HTTP API for status and commands."""
import json
import logging
import os
import time
from concurrent import futures
from contextlib import asynccontextmanager

import grpc
import httpx
import psycopg
from ledger_node.storage import MemoryStorage, PostgresStorage
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ledger_node.proto import raft_pb2_grpc
from ledger_node.raft import CommitTimeout, NotLeader, RaftNode
from ledger_node.storage import MemoryStorage
from ledger_node.state_machine import StateMachine
from ledger_node import merkle, metrics
from shared import tls

NODE_ID = os.getenv("NODE_ID", "ledger-1")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))


def parse_peers(text: str) -> dict:
    """'ledger-2=ledger-2:50051,ledger-3=ledger-3:50051' -> {'ledger-2': 'ledger-2:50051', ...}"""
    return dict(item.split("=", 1) for item in text.split(",") if item.strip())


PEERS = parse_peers(os.getenv("PEERS", ""))               # Raft (gRPC) addresses of the other nodes
HTTP_PEERS = parse_peers(os.getenv("HTTP_PEERS", ""))     # HTTP addresses, used to forward commands

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(NODE_ID)

def log_only_apply(entry):
    """Used when there is no database: only records what was committed."""
    log.info("applied entry %d (term %d): %s", entry.index, entry.term, entry.command)
    return {"ok": True}


def open_storage():
    """Use the node's own PostgreSQL database when DATABASE_URL is set; otherwise keep the log in memory."""
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        return MemoryStorage()
    for attempt in range(1, 31):
        try:
            storage = PostgresStorage(dsn)
            log.info("loaded from PostgreSQL: term %d, %d log entries, last hash %s",
                     storage.term, storage.last_index(), storage.last_hash()[:16])
            return storage
        except psycopg.OperationalError:
            log.warning("database not ready (attempt %d), retrying in 1 s", attempt)
            time.sleep(1)
    raise RuntimeError("could not connect to the ledger database")


storage = open_storage()
state = StateMachine(os.environ["DATABASE_URL"]) if os.getenv("DATABASE_URL") else None


def apply(entry):
    result = state.apply(entry) if state else log_only_apply(entry)
    log.info("applied entry %d (term %d) %s -> %s", entry.index, entry.term, entry.command, result)
    return result


applied = state.last_applied() if state else 0
log.info("state machine resumes after entry %d", applied)
node = RaftNode(NODE_ID, PEERS, storage, apply_fn=apply, applied_index=applied)
grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
raft_pb2_grpc.add_RaftServicer_to_server(node, grpc_server)
if tls.ENABLED:                                      # Raft messages between nodes: mutual TLS
    grpc_server.add_secure_port(f"[::]:{GRPC_PORT}", tls.grpc_server_credentials())
else:
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")


@asynccontextmanager
async def lifespan(_: FastAPI):
    grpc_server.start()
    node.start()
    log.info("%s started; peers: %s", NODE_ID, PEERS)
    yield
    node.stop()
    grpc_server.stop(1)


app = FastAPI(title=f"AquaShare ledger node {NODE_ID}", lifespan=lifespan)
metrics.install(app, node, NODE_ID)


class Command(BaseModel):
    command: dict


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "node_id": NODE_ID}


@app.get("/status")
def status() -> dict:
    return node.status()


@app.get("/log")
def read_log(start: int = 1, limit: int = 50) -> list[dict]:
    return [{"index": e.index, "term": e.term, "command": json.loads(e.command)}
            for e in node.storage.entries_from(start, limit)]


@app.post("/command")
def submit(body: Command) -> dict:
    """Commit a command. A follower forwards it to the current leader."""
    try:
        started = time.perf_counter()
        index, result = node.submit(body.command)
        metrics.COMMIT_SECONDS.observe(time.perf_counter() - started)
        metrics.COMMANDS.labels("committed" if result and result.get("ok") else "rejected").inc()
        return {"committed_index": index, "leader_id": NODE_ID, "result": result}
    except NotLeader as exc:
        metrics.COMMANDS.labels("forwarded").inc()
        leader = exc.leader_id
        if not leader or leader not in HTTP_PEERS:
            raise HTTPException(503, "no leader is currently known; try again shortly")
        try:
            reply = tls.post(f"{HTTP_PEERS[leader]}/command", json=body.model_dump(), timeout=6.0)
        except httpx.HTTPError:
            raise HTTPException(503, f"leader {leader} is unreachable; try again shortly")
        if reply.status_code != 200:
            raise HTTPException(reply.status_code, reply.json().get("detail", "leader refused the command"))
        return {**reply.json(), "forwarded_by": NODE_ID}
    except CommitTimeout:
        metrics.COMMANDS.labels("no_majority").inc()
        raise HTTPException(503, "command not committed: no majority of ledger nodes is reachable")

@app.get("/state")
def read_state(day: str | None = None) -> dict:
    """Balances for a day (default: the current day) as stored on this node."""
    if state is None:
        raise HTTPException(404, "this node has no database")
    return {"node_id": NODE_ID, "last_applied": node.status()["last_applied"], **state.snapshot(day)}


@app.get("/transfers")
def read_transfers(farm: str, day: str | None = None) -> list[dict]:
    """Committed transfers sent or received by a farm, as stored on this node."""
    if state is None:
        raise HTTPException(404, "this node has no database")
    return state.transfers(farm, day)


@app.get("/events")
def read_events(after: int = 0, limit: int = 200) -> list[dict]:
    """Applied commands after a log index, for the event relay."""
    if state is None:
        raise HTTPException(404, "this node has no database")
    return state.events(after, limit)


# ---------------------------------------------------------------- Merkle proofs
def committed_leaves(size: int | None):
    commit = node.status()["commit_index"]
    size = commit if size is None else size
    if not 1 <= size <= commit:
        raise HTTPException(400, f"size must be between 1 and the commit index {commit}")
    entries = node.storage.entries_from(1, size)
    return size, entries, [merkle.leaf_hash(e.entry_hash) for e in entries]


@app.get("/merkle/root")
def merkle_root(size: int | None = None) -> dict:
    """Root hash of the Merkle tree over the first `size` committed entries (default: all of them)."""
    size, _, leaves = committed_leaves(size)
    return {"node_id": NODE_ID, "size": size, "root": merkle.root(leaves).hex()}


@app.get("/merkle/proof")
def merkle_proof(index: int | None = None, transfer_id: str | None = None, size: int | None = None) -> dict:
    """Proof that one committed entry is in the tree. Find it by log index or by transfer_id."""
    size, entries, leaves = committed_leaves(size)
    if index is None and transfer_id:
        index = next((e.index for e in entries if transfer_id in e.command
                      and json.loads(e.command).get("transfer_id") == transfer_id), None)
    if index is None or not 1 <= index <= size:
        raise HTTPException(404, "no such committed entry")
    e = entries[index - 1]
    return {"node_id": NODE_ID, "size": size, "root": merkle.root(leaves).hex(),
            "entry": {"index": e.index, "term": e.term, "command": json.loads(e.command),
                      "prev_hash": e.prev_hash, "entry_hash": e.entry_hash},
            "path": [h.hex() for h in merkle.proof(leaves, index - 1)]}
