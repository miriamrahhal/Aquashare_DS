param([int]$Port = 8701, [string]$Value = "hello")
$body = @{ command = @{ type = "test"; value = $Value } } | ConvertTo-Json
try {
    Invoke-RestMethod -Method Post -Uri "http://localhost:$Port/command" -ContentType "application/json" -Body $body -TimeoutSec 15 | ConvertTo-Json
} catch { "REFUSED: " + $_.ErrorDetails.Message }