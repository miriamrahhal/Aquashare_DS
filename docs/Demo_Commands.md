# AquaShare demo from the terminal (PowerShell)

Open PowerShell in the project folder, activate the virtual environment, and paste the two helpers once:

```powershell
cd C:\Users\User\Desktop\Aquashare_DS
.venv\Scripts\activate
function Post($url, $body) { try { Invoke-RestMethod -Method Post -Uri $url -ContentType 'application/json' -Body ($body | ConvertTo-Json -Depth 5) | ConvertTo-Json -Depth 5 } catch { "REFUSED: " + $_.ErrorDetails.Message } }
function Status { foreach ($p in 8701, 8702, 8703) { try { $s = Invoke-RestMethod "http://localhost:$p/status" -TimeoutSec 2; "{0}  {1,-9} term={2} commit={3}" -f $s.node_id, $s.role, $s.term, $s.commit_index } catch { "port $p  DOWN" } } }
```

## 1. Start a new day

```powershell
$next = ([datetime](Invoke-RestMethod http://localhost:8600/state).day).AddDays(1).ToString('yyyy-MM-dd')
Post http://localhost:8600/days @{ day = $next; limit_l = 20000; allocations = @{ 'farm-a' = 6000; 'farm-b' = 5000; 'farm-c' = 4500; 'farm-d' = 4000 } }
Start-Sleep 5
(Invoke-RestMethod http://localhost:8600/state).farms | Format-Table farm_id, allocated, consumed, available
```
You see: the new day, and each farm's share.

## 2. A farm goes offline

```powershell
Post http://localhost:8804/link/down @{}
Start-Sleep 10
Invoke-RestMethod http://localhost:8804/sync | Format-List farm_id, link, online, pending_events, local_consumed, ledger_consumed
Post http://localhost:8804/link/up @{}
Start-Sleep 8
Invoke-RestMethod http://localhost:8804/sync | Format-List farm_id, link, online, pending_events, local_consumed, ledger_consumed
```
You see: while offline, `pending_events` grows and `local_consumed` is ahead of `ledger_consumed`. After reconnecting, `pending_events` is 0 and both numbers are equal.

## 3. Same transfer twice

```powershell
Post http://localhost:8801/transfers @{ recipient = 'farm-b'; amount_l = 50 }
$day = (Invoke-RestMethod http://localhost:8600/state).day
$t = (Invoke-RestMethod "http://localhost:8600/farms/farm-a/transfers?day=$day")[-1]
$again = @{ transfer_id = $t.transfer_id; day = $t.day; sender = $t.sender; recipient = $t.recipient; amount_l = $t.amount_l; cmd_id = "transfer-$($t.transfer_id)" }
Post http://localhost:8600/transfers $again
Post http://localhost:8600/transfers $again
```
You see: both resends answer with `"duplicate": true`, and the transfer is counted once.

## 4. Spend too much

```powershell
Post http://localhost:8801/transfers @{ recipient = 'farm-b'; amount_l = 999999 }
```
You see: `REFUSED`, because farm-a's wallet doesn't hold that much water.

## 5. Crash the leader

```powershell
Status
$leader = foreach ($p in 8701, 8702, 8703) { try { $s = Invoke-RestMethod "http://localhost:$p/status" -TimeoutSec 2; if ($s.role -eq 'leader') { $s.node_id } } catch {} }
docker stop "aq-$leader"
Start-Sleep 4
Status
docker start "aq-$leader"
Start-Sleep 5
Status
```
You see: the old leader `DOWN` and another node `leader` with a higher term. After the restart it comes back as a `follower` with the same commit.

## 6. Lose the majority

```powershell
docker stop aq-ledger-2 aq-ledger-3
Status
Post http://localhost:8801/transfers @{ recipient = 'farm-b'; amount_l = 100 }
docker start aq-ledger-2 aq-ledger-3
Start-Sleep 8
Status
```
You see: with one node left, the transfer comes back `pending` (not saved, water on hold). After the restart, the nodes elect a leader and the farm sends the transfer again by itself.

## 7. Prove a record is real (Merkle proof)

```powershell
$day = (Invoke-RestMethod http://localhost:8600/state).day
$t = (Invoke-RestMethod "http://localhost:8600/farms/farm-a/transfers?day=$day")[-1]
python scripts/verify_proof.py --transfer-id $t.transfer_id
python scripts/verify_proof.py --transfer-id $t.transfer_id --tamper
```
You see: `VERIFIED` for the real record, and a failure for the falsified copy.

## 8. A fake farm

```powershell
(Invoke-RestMethod http://localhost:8600/state).farms | Format-Table farm_id, signs_reports, consumed
python scripts/fake_farm.py --farm farm-a --amount 500
(Invoke-RestMethod http://localhost:8600/state).farms | Format-Table farm_id, signs_reports, consumed
```
You see: `HTTP 409 REFUSED … signature missing` and `HTTP 409 REFUSED … invalid signature`, and farm-a's `consumed` doesn't jump by 500.

## 9. Check the 7 rules

```powershell
python scripts/check_invariants.py
```
You see: `PASS` on all 7 rules (I1–I7) across the 3 databases.

## Bonus: watch a service live

```powershell
docker logs -f aq-farm-a
```
Stop with Ctrl + C.
