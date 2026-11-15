foreach ($p in 8701, 8702, 8703) {
    try {
        $s = Invoke-RestMethod "http://localhost:$p/status" -TimeoutSec 2
        "{0}  {1,-9}  term={2}  leader={3}  commit_index={4}" -f $s.node_id, $s.role, $s.term, $s.leader_id, $s.commit_index
    } catch { "port $p   DOWN" }
}