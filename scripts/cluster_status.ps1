foreach ($p in 8701, 8702, 8703) {
    try {
        $s = Invoke-RestMethod "http://localhost:$p/status" -TimeoutSec 2
        "{0}  {1,-9}  term={2}  leader={3}  commit_index={4}  last_hash={5}" -f $s.node_id, $s.role, $s.term, $s.leader_id, $s.commit_index, $s.last_log_hash    } catch { "port $p   DOWN" }
}