# Each farm's own wallet next to the ledger's record of the same farm, for the current day.
$ledger = (Invoke-RestMethod http://localhost:8600/state).farms
8801..8804 | ForEach-Object {
    $w = Invoke-RestMethod "http://localhost:$_/wallet"
    $l = $ledger | Where-Object farm_id -eq $w.farm_id
    [pscustomobject]@{ farm = $w.farm_id; day = $w.day
        wallet_sent = $w.sent; ledger_sent = $l.sent
        wallet_received = $w.received; ledger_received = $l.received
        wallet_reserved = $w.reserved }
} | Format-Table