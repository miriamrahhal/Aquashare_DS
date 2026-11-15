"""Checks that the Raft messages are generated correctly and survive a round trip."""
from ledger_node.proto import raft_pb2, raft_pb2_grpc


def test_vote_request_round_trip():
    original = raft_pb2.VoteRequest(term=3, candidate_id="ledger-1", last_log_index=10, last_log_term=2)
    copy = raft_pb2.VoteRequest.FromString(original.SerializeToString())
    assert copy == original
    assert copy.candidate_id == "ledger-1"


def test_append_request_carries_entries():
    entry = raft_pb2.LogEntry(index=11, term=3, command='{"type": "noop"}', prev_hash="a", entry_hash="b")
    request = raft_pb2.AppendRequest(term=3, leader_id="ledger-1", prev_log_index=10, prev_log_term=2,
                                     entries=[entry], leader_commit=10)
    copy = raft_pb2.AppendRequest.FromString(request.SerializeToString())
    assert len(copy.entries) == 1 and copy.entries[0].command == '{"type": "noop"}'


def test_heartbeat_is_an_empty_append():
    heartbeat = raft_pb2.AppendRequest(term=3, leader_id="ledger-1", leader_commit=10)
    assert len(heartbeat.entries) == 0


def test_service_defines_both_calls():
    assert hasattr(raft_pb2_grpc.RaftStub, "__init__")
    assert hasattr(raft_pb2_grpc.RaftServicer, "RequestVote")
    assert hasattr(raft_pb2_grpc.RaftServicer, "AppendEntries")