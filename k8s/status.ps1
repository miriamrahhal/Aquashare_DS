# Show the Raft role of each ledger pod:  powershell -ExecutionPolicy Bypass -File k8s\status.ps1
foreach ($i in 0, 1, 2) {
  $s = kubectl exec "ledger-$i" -c ledger -- python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8000/status',timeout=2).read().decode())" 2>$null
  if ($s) { $j = $s | ConvertFrom-Json; "{0}  {1,-9} term={2} commit={3}" -f $j.node_id, $j.role, $j.term, $j.commit_index }
  else { "ledger-$i  DOWN" }
}
