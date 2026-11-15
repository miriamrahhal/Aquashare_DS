"""Prometheus metrics for a ledger node.

Raft state (term, role, commit index, ...) is read from the node at scrape time, so the
Raft code itself is not changed. Command outcomes and commit latency are recorded by the
HTTP API."""
from fastapi import Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, Counter, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily

COMMANDS = Counter("aquashare_commands_total", "Commands handled by this ledger node", ["outcome"])
COMMIT_SECONDS = Histogram("aquashare_commit_seconds", "Time for the leader to commit and apply a command",
                           buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))


class RaftCollector:
    def __init__(self, node, node_id):
        self.node, self.node_id = node, node_id

    def collect(self):
        s = self.node.status()
        for name, help_text, value in [
            ("aquashare_raft_term", "Current Raft term", s["term"]),
            ("aquashare_raft_is_leader", "1 if this node is the leader", 1 if s["role"] == "leader" else 0),
            ("aquashare_raft_commit_index", "Highest committed log index", s["commit_index"]),
            ("aquashare_raft_last_applied", "Highest log index applied to the database", s["last_applied"]),
            ("aquashare_raft_log_length", "Number of entries in the log", s["last_log_index"]),
        ]:
            gauge = GaugeMetricFamily(name, help_text, labels=["node"])
            gauge.add_metric([self.node_id], value)
            yield gauge


def install(app, node, node_id):
    REGISTRY.register(RaftCollector(node, node_id))

    @app.get("/metrics", include_in_schema=False)
    def metrics_endpoint():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
