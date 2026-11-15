param([int]$FromPort = 8801, [string]$To = "farm-b", [int]$Amount = 500)
$body = @{ recipient = $To; amount_l = $Amount } | ConvertTo-Json
try {
    Invoke-RestMethod -Method Post -Uri "http://localhost:$FromPort/transfers" -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 3
} catch { "REFUSED BY THE WALLET: " + $_.ErrorDetails.Message }