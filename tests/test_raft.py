"""Raft tests: three real nodes talking over gRPC on this machine."""
import socket
import time
from concurrent import futures

import grpc
import pytest

from ledger_node.proto import raft_pb2_grpc
from ledger_node.raft import LEADER, CommitTimeout, NotLeader, RaftNode
from ledger_node.storage import MemoryStorage


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class PartitionableNode(RaftNode):
    """A Raft node whose messages can be dropped, to simulate a network partition.
    Every Raft message is sent through _call, so dropping it there cuts both directions."""
    cluster = None

    def _call(self, pid, method, request):
        if self.cluster and self.cluster.is_cut(self.id, pid):
            return None
        return super()._call(pid, method, request)


class Cluster:
    def __init__(self, size=3):
        ids = [f"ledger-{i}" for i in range(1, size + 1)]
        self.addresses = {i: f"127.0.0.1:{free_port()}" for i in ids}
        self.nodes, self.servers = {}, {}
        self.cut = set()                    # pairs of nodes that cannot reach each other
        for node_id in ids:
            self.start(node_id, MemoryStorage())

    def start(self, node_id, storage):
        peers = {p: a for p, a in self.addresses.items() if p != node_id}
        node = PartitionableNode(node_id, peers, storage, election_timeout=(0.3, 0.6), heartbeat_interval=0.05,
                                 rpc_timeout=0.2)
        node.cluster = self
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        raft_pb2_grpc.add_RaftServicer_to_server(node, server)
        server.add_insecure_port(self.addresses[node_id])
        server.start()
        node.start()
        self.nodes[node_id], self.servers[node_id] = node, server

    def is_cut(self, a, b):
        return frozenset((a, b)) in self.cut

    def isolate(self, node_id):
        self.cut = {frozenset((node_id, other)) for other in self.addresses if other != node_id}

    def heal(self):
        self.cut = set()

    def crash(self, node_id):
        self.nodes[node_id].stop()
        self.servers[node_id].stop(0)
        return self.nodes.pop(node_id).storage

    def leader(self, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            leaders = [n for n in self.nodes.values() if n.role == LEADER]
            # wait until every running node also knows who the leader is
            if len(leaders) == 1 and all(n.leader_id == leaders[0].id for n in self.nodes.values()):
                return leaders[0]
            time.sleep(0.05)
        raise AssertionError("no single leader elected")

    def shutdown(self):
        for node_id in list(self.nodes):
            self.crash(node_id)


@pytest.fixture
def cluster():
    c = Cluster()
    yield c
    c.shutdown()


def test_one_leader_is_elected(cluster):
    leader = cluster.leader()
    time.sleep(0.5)
    terms = {n.term for n in cluster.nodes.values()}
    assert len(terms) == 1
    assert all(n.leader_id == leader.id for n in cluster.nodes.values())


def test_followers_reject_commands(cluster):
    leader = cluster.leader()
    follower = next(n for n in cluster.nodes.values() if n is not leader)
    with pytest.raises(NotLeader) as error:
        follower.submit({"type": "test"})
    assert error.value.leader_id == leader.id


def test_command_is_replicated_to_all_nodes(cluster):
    leader = cluster.leader()
    index, _ = leader.submit({"type": "test", "value": 1})
    time.sleep(0.3)
    for node in cluster.nodes.values():
        assert node.storage.entry(index).command == '{"type": "test", "value": 1}'
        assert node.commit_index >= index


def test_new_leader_after_leader_crash_keeps_committed_entries(cluster):
    old = cluster.leader()
    index, _ = old.submit({"type": "test", "value": "before crash"})
    cluster.crash(old.id)
    new = cluster.leader()
    assert new.id != old.id and new.term > old.term
    assert "before crash" in new.storage.entry(index).command
    new.submit({"type": "test", "value": "after crash"})


def test_no_commit_without_majority(cluster):
    leader = cluster.leader()
    for node_id in [n for n in list(cluster.nodes) if n != leader.id]:
        cluster.crash(node_id)
    with pytest.raises(CommitTimeout):
        leader.submit({"type": "test"}, timeout=1.0)


def test_restarted_follower_catches_up(cluster):
    leader = cluster.leader()
    follower_id = next(n for n in cluster.nodes if n != leader.id)
    storage = cluster.crash(follower_id)
    for i in range(5):
        leader.submit({"type": "test", "value": i})
    cluster.start(follower_id, storage)
    deadline = time.time() + 5
    while time.time() < deadline and cluster.nodes[follower_id].storage.last_index() < leader.storage.last_index():
        time.sleep(0.1)
    assert cluster.nodes[follower_id].storage.last_index() == leader.storage.last_index()



def wait_for(condition, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


def test_network_partition_isolated_leader_cannot_commit_and_steps_down(cluster):
    old = cluster.leader()
    cluster.nodes[old.id].submit({"type": "test", "value": "before the partition"})
    cluster.isolate(old.id)                                       # the leader is cut off but keeps running

    with pytest.raises(CommitTimeout):                            # 1 of 3 nodes is not a majority
        old.submit({"type": "test", "value": "written by the isolated leader"}, timeout=1.0)
    lost_index = old.storage.last_index()
    assert old.role == LEADER and old.commit_index < lost_index   # still believes it leads, commits nothing

    others = [n for n in cluster.nodes.values() if n.id != old.id]
    assert wait_for(lambda: any(n.role == LEADER and n.term > old.term for n in others))
    new = next(n for n in others if n.role == LEADER)
    index, _ = new.submit({"type": "test", "value": "committed by the majority"})

    cluster.heal()
    assert wait_for(lambda: old.role != LEADER and old.term == new.term and old.commit_index >= index)
    # the isolated leader's uncommitted entry was discarded and replaced by the majority's log
    assert "isolated leader" not in old.storage.entry(lost_index).command
    assert wait_for(lambda: len({n.storage.last_hash() for n in cluster.nodes.values()}) == 1)
