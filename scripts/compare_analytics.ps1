# The analytics service's totals next to the ledger's balances, for the current day.
$ledger = Invoke-RestMethod http://localhost:8600/state
$an = Invoke-RestMethod "http://localhost:8901/summary?day=$($ledger.day)"
$ledger.farms | ForEach-Object {
    $f = $_; $a = $an.farms | Where-Object farm_id -eq $f.farm_id
    [pscustomobject]@{ farm = $f.farm_id
        ledger_consumed = $f.consumed; analytics_consumed = $a.consumed
        ledger_sent = $f.sent; analytics_sent = $a.sent
        ledger_received = $f.received; analytics_received = $a.received }
} | Format-Table
"day $($ledger.day): ledger last applied entry $($ledger.last_applied); analytics processed $($an.events_processed) events up to log index $($an.last_log_index), ignored $($an.duplicates_ignored) duplicates"
