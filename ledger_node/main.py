"""Ledger node: runs Raft over gRPC and exposes an HTTP API for status and commands."""
import json
import logging
import os
from concurrent import futures
from contextlib import asynccontextmanager

import grpc
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ledger_node.proto import raft_pb2_grpc
from ledger_node.raft import CommitTimeout, NotLeader, RaftNode
from ledger_node.storage import MemoryStorage

NODE_ID = os.getenv("NODE_ID", "ledger-1")
GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))


def parse_peers(text: str) -> dict:
    """'ledger-2=ledger-2:50051,ledger-3=ledger-3:50051' -> {'ledger-2': 'ledger-2:50051', ...}"""
    return dict(item.split("=", 1) for item in text.split(",") if item.strip())


PEERS = parse_peers(os.getenv("PEERS", ""))               # Raft (gRPC) addresses of the other nodes
HTTP_PEERS = parse_peers(os.getenv("HTTP_PEERS", ""))     # HTTP addresses, used to forward commands

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(NODE_ID)


def apply(entry):
    """State machine. For now it only records what was committed; allocation rules come later."""
    log.info("applied entry %d (term %d): %s", entry.index, entry.term, entry.command)
    return {"index": entry.index}


node = RaftNode(NODE_ID, PEERS, MemoryStorage(), apply_fn=apply)
grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
raft_pb2_grpc.add_RaftServicer_to_server(node, grpc_server)
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
        index, result = node.submit(body.command)
        return {"committed_index": index, "leader_id": NODE_ID, "result": result}
    except NotLeader as exc:
        leader = exc.leader_id
        if not leader or leader not in HTTP_PEERS:
            raise HTTPException(503, "no leader is currently known; try again shortly")
        try:
            reply = httpx.post(f"{HTTP_PEERS[leader]}/command", json=body.model_dump(), timeout=6.0)
        except httpx.HTTPError:
            raise HTTPException(503, f"leader {leader} is unreachable; try again shortly")
        if reply.status_code != 200:
            raise HTTPException(reply.status_code, reply.json().get("detail", "leader refused the command"))
        return {**reply.json(), "forwarded_by": NODE_ID}
    except CommitTimeout:
        raise HTTPException(503, "command not committed: no majority of ledger nodes is reachable")