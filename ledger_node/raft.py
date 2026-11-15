"""Raft consensus for the AquaShare ledger (leader election and log replication).

One lock protects all Raft state. Network calls are always made outside the lock,
so a slow or failed peer can never block the node.
"""
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

from ledger_node.proto import raft_pb2, raft_pb2_grpc
from ledger_node.storage import Entry

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"

log = logging.getLogger("raft")


class NotLeader(Exception):
    def __init__(self, leader_id):
        super().__init__(f"not the leader; current leader is {leader_id}")
        self.leader_id = leader_id


class CommitTimeout(Exception):
    pass


class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, peers, storage, apply_fn=None, applied_index=0,
                 election_timeout=(1.5, 3.0), heartbeat_interval=0.3, rpc_timeout=0.5):
        self.id = node_id
        self.peers = dict(peers)                      # peer id -> "host:port"
        self.storage = storage
        self.apply_fn = apply_fn or (lambda entry: None)
        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval
        self.rpc_timeout = rpc_timeout

        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)  # signalled when commit/role changes
        self.term, self.voted_for = storage.load_term_vote()
        self.role = FOLLOWER
        self.leader_id = None
        self.commit_index = applied_index     # entries already applied before a restart are committed
        self.last_applied = applied_index
        self.next_index = {}
        self.match_index = {}
        self.results = {}
        self.election_deadline = self._new_deadline()
        self.last_heartbeat_sent = 0.0
        self.running = False

        # Short reconnect backoff, so a peer that restarts is reached again within about a second.
        options = [("grpc.initial_reconnect_backoff_ms", 100), ("grpc.min_reconnect_backoff_ms", 100),
                   ("grpc.max_reconnect_backoff_ms", 1000)]
        self.stubs = {pid: raft_pb2_grpc.RaftStub(grpc.insecure_channel(addr, options=options))
                      for pid, addr in self.peers.items()}
        self.pool = ThreadPoolExecutor(max_workers=max(4, 2 * len(self.peers)))
        self.sending = {pid: False for pid in self.peers}

    # ------------------------------------------------------------------ helpers
    def _new_deadline(self):
        return time.monotonic() + random.uniform(*self.election_timeout)

    def _majority(self):
        return (len(self.peers) + 1) // 2 + 1

    def _become_follower(self, term, leader_id=None):
        """Must hold the lock."""
        if term > self.term:
            self.term, self.voted_for = term, None
            self.storage.save_term_vote(self.term, self.voted_for)
        if self.role != FOLLOWER:
            log.info("%s: became follower in term %d", self.id, self.term)
        self.role = FOLLOWER
        self.leader_id = leader_id
        self.election_deadline = self._new_deadline()
        self.changed.notify_all()

    def status(self):
        with self.lock:
            return {
                "node_id": self.id, "role": self.role, "term": self.term, "leader_id": self.leader_id,
                "voted_for": self.voted_for, "commit_index": self.commit_index,
                "last_applied": self.last_applied, "last_log_index": self.storage.last_index(),
                "last_log_term": self.storage.last_term(),
                "last_log_hash": self.storage.last_hash()[:16],
            }

    # ------------------------------------------------------------------ lifecycle
    def start(self):
        self.running = True
        threading.Thread(target=self._ticker, name=f"raft-{self.id}", daemon=True).start()

    def stop(self):
        self.running = False

    def _ticker(self):
        while self.running:
            time.sleep(0.05)
            with self.lock:
                role, now = self.role, time.monotonic()
                heartbeat_due = role == LEADER and now - self.last_heartbeat_sent >= self.heartbeat_interval
                election_due = role != LEADER and now >= self.election_deadline
            if heartbeat_due:
                self._replicate_to_all()
            elif election_due:
                self._start_election()

    # ------------------------------------------------------------------ elections
    def _start_election(self):
        with self.lock:
            self.role = CANDIDATE
            self.term += 1
            self.voted_for = self.id
            self.storage.save_term_vote(self.term, self.voted_for)   # persist before asking for votes
            self.leader_id = None
            self.election_deadline = self._new_deadline()
            term = self.term
            request = raft_pb2.VoteRequest(term=term, candidate_id=self.id,
                                           last_log_index=self.storage.last_index(),
                                           last_log_term=self.storage.last_term())
        log.info("%s: starting election for term %d", self.id, term)
        votes = 1
        futures = [self.pool.submit(self._call, pid, "RequestVote", request) for pid in self.peers]
        for future in futures:
            reply = future.result()
            if reply is None:
                continue
            with self.lock:
                if reply.term > self.term:
                    self._become_follower(reply.term)
                    return
                if self.role != CANDIDATE or self.term != term:
                    return
                if reply.vote_granted:
                    votes += 1
                    if votes >= self._majority():
                        self._become_leader()
                        break
        if self.role == LEADER:
            self._replicate_to_all()

    def _become_leader(self):
        """Must hold the lock."""
        self.role = LEADER
        self.leader_id = self.id
        last = self.storage.last_index()
        self.next_index = {pid: last + 1 for pid in self.peers}
        self.match_index = {pid: 0 for pid in self.peers}
        log.info("%s: became LEADER for term %d", self.id, self.term)
        # A no-op entry from the new term lets entries from earlier terms be committed safely.
        self._append_local(json.dumps({"type": "noop"}))
        self.changed.notify_all()

    def RequestVote(self, request, context):
        with self.lock:
            if request.term < self.term:
                return raft_pb2.VoteReply(term=self.term, vote_granted=False)
            if request.term > self.term:
                self._become_follower(request.term)
            log_ok = (request.last_log_term, request.last_log_index) >= (self.storage.last_term(),
                                                                          self.storage.last_index())
            granted = self.voted_for in (None, request.candidate_id) and log_ok
            if granted:
                self.voted_for = request.candidate_id
                self.storage.save_term_vote(self.term, self.voted_for)   # persist before replying
                self.election_deadline = self._new_deadline()
            return raft_pb2.VoteReply(term=self.term, vote_granted=granted)

    # ------------------------------------------------------------------ replication
    def _append_local(self, command):
        """Append a new entry to the leader's own log. Must hold the lock."""
        entry = Entry(index=self.storage.last_index() + 1, term=self.term, command=command)
        self.storage.append([entry])
        return entry.index

    def _replicate_to_all(self):
        with self.lock:
            if self.role != LEADER:
                return
            self.last_heartbeat_sent = time.monotonic()
        for pid in self.peers:
            with self.lock:
                if self.sending[pid]:
                    continue        # previous request to this peer is still in flight
                self.sending[pid] = True
            self.pool.submit(self._replicate_to, pid)

    def _replicate_to(self, pid):
        try:
            with self.lock:
                if self.role != LEADER:
                    return
                term = self.term
                prev_index = self.next_index[pid] - 1
                entries = self.storage.entries_from(self.next_index[pid])
                request = raft_pb2.AppendRequest(
                    term=term, leader_id=self.id, prev_log_index=prev_index,
                    prev_log_term=self.storage.term_at(prev_index), leader_commit=self.commit_index,
                    entries=[raft_pb2.LogEntry(index=e.index, term=e.term, command=e.command,
                                               prev_hash=e.prev_hash, entry_hash=e.entry_hash) for e in entries])
            reply = self._call(pid, "AppendEntries", request)
            if reply is None:
                return
            with self.lock:
                if reply.term > self.term:
                    self._become_follower(reply.term)
                    return
                if self.role != LEADER or self.term != term:
                    return
                if reply.success:
                    self.match_index[pid] = max(self.match_index[pid], reply.match_index)
                    self.next_index[pid] = self.match_index[pid] + 1
                    self._advance_commit()
                else:
                    self.next_index[pid] = max(1, reply.conflict_index or self.next_index[pid] - 1)
        finally:
            with self.lock:
                self.sending[pid] = False

    def _advance_commit(self):
        """Commit the highest index stored on a majority from the current term. Must hold the lock."""
        for n in range(self.storage.last_index(), self.commit_index, -1):
            if self.storage.term_at(n) != self.term:
                break
            replicas = 1 + sum(1 for m in self.match_index.values() if m >= n)
            if replicas >= self._majority():
                self.commit_index = n
                self._apply_committed()
                break

    def AppendEntries(self, request, context):
        with self.lock:
            if request.term < self.term:
                return raft_pb2.AppendReply(term=self.term, success=False)
            if request.term > self.term or self.role != FOLLOWER or self.leader_id != request.leader_id:
                self._become_follower(request.term, request.leader_id)
            self.election_deadline = self._new_deadline()

            last = self.storage.last_index()
            if request.prev_log_index > last:
                return raft_pb2.AppendReply(term=self.term, success=False, conflict_index=last + 1)
            if request.prev_log_index > 0 and self.storage.term_at(request.prev_log_index) != request.prev_log_term:
                bad_term, first = self.storage.term_at(request.prev_log_index), request.prev_log_index
                while first > 1 and self.storage.term_at(first - 1) == bad_term:
                    first -= 1
                return raft_pb2.AppendReply(term=self.term, success=False, conflict_index=first)

            new = []
            for e in request.entries:
                existing = self.storage.entry(e.index)
                if existing is not None and existing.term == e.term:
                    continue
                if existing is not None:
                    if e.index <= self.commit_index:
                        raise RuntimeError("leader tried to overwrite a committed entry")
                    self.storage.truncate_from(e.index)      # conflicting entries are discarded
                new.append(Entry(e.index, e.term, e.command, e.prev_hash, e.entry_hash))
            if new:
                self.storage.append(new)

            match = request.prev_log_index + len(request.entries)
            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, match)
                self._apply_committed()
            return raft_pb2.AppendReply(term=self.term, success=True, match_index=match)

    def _apply_committed(self):
        """Apply committed entries to the state machine in log order. Must hold the lock."""
        while self.last_applied < self.commit_index:
            entry = self.storage.entry(self.last_applied + 1)
            self.results[entry.index] = self.apply_fn(entry)
            self.last_applied += 1               # only after the entry was applied successfully
        self.changed.notify_all()

    # ------------------------------------------------------------------ client commands
    def submit(self, command: dict, timeout=5.0):
        """Replicate a command and wait until it is committed. Only the leader accepts commands."""
        with self.lock:
            if self.role != LEADER:
                raise NotLeader(self.leader_id)
            term = self.term
            index = self._append_local(json.dumps(command, sort_keys=True))
        self._replicate_to_all()
        deadline = time.monotonic() + timeout
        with self.lock:
            while self.last_applied < index:
                if self.role != LEADER or self.term != term:
                    raise NotLeader(self.leader_id)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise CommitTimeout(f"entry {index} not committed within {timeout}s")
                self.changed.wait(remaining)
            if self.storage.term_at(index) != term:
                raise NotLeader(self.leader_id)      # our entry was replaced by a new leader
            return index, self.results.pop(index, None)

    # ------------------------------------------------------------------ network
    def _call(self, pid, method, request):
        try:
            return getattr(self.stubs[pid], method)(request, timeout=self.rpc_timeout)
        except grpc.RpcError:
            return None