8801..8804 | ForEach-Object { Invoke-RestMethod "http://localhost:$_/sync" } |
    Format-Table farm_id, link, online, current_day, pending_events, local_consumed, ledger_consumed