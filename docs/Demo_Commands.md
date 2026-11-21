# AquaShare terminal demo (works with mutual TLS on)

Open PowerShell in the project folder and activate the environment once:

```powershell
cd C:\Users\User\Desktop\Aquashare_DS
.venv\Scripts\activate
```

Every command below uses the operator certificate by itself (it reads `.env`).

| # | Scenario | Command(s) | What you show |
|---|---|---|---|
| 0 | Fresh start (optional, before class) | `docker compose down -v` → `docker compose up -d --build` → wait 30 s → `python scripts/setup_today.py` | Empty ledger, today's date |
| 1 | Start a new day | `python scripts/demo.py day` | Each farm gets its share |
| 2 | A farm goes offline | `python scripts/demo.py offline` | `pending` grows while offline, back to 0 after the link returns; local and ledger usage become equal |
| 3 | Same transfer twice | `python scripts/demo.py duplicate` | Both resends answer `"duplicate": true`, counted once |
| 4 | Spend too much | `python scripts/demo.py overspend` | `HTTP 409`, not enough water |
| 5 | Crash the leader | `python scripts/demo.py status` → `docker stop aq-ledger-X` (the leader) → wait 4 s → `python scripts/demo.py status` → `docker start aq-ledger-X` → wait 6 s → `python scripts/demo.py status` | New leader with a higher term; the old one comes back as follower, same commit |
| 6 | Lose the majority | `docker stop aq-ledger-2 aq-ledger-3` → `python scripts/demo.py transfer` → `docker start aq-ledger-2 aq-ledger-3` → wait 8 s → `python scripts/demo.py status` | Transfer stays `pending` (water on hold), completes by itself after the restart |
| 7 | Prove a record is real | `python scripts/demo.py proof` | `VERIFIED`, then a failure for the falsified copy |
| 8 | A fake farm | `python scripts/fake_farm.py --farm farm-a --amount 500` | Two `HTTP 409 REFUSED` (signature missing / invalid) |
| 9 | Check the 7 rules | `python scripts/check_invariants.py` | `PASS` on I1–I7 |
| 10 | No certificate, no access | `python scripts/demo.py nocert` | Refused at the TLS handshake |
| 11 | Wrong role | `python scripts/demo.py forbidden` | `HTTP 403 certificate 'farm-a' is not allowed to open a day` |

| 12 | A lying ledger node | `python scripts/demo.py lie` → `python scripts/check_invariants.py` → `python scripts/demo.py honest` | The gateway still shows farm-a's true water, with `OUTVOTED: ledger-3`; the checker flags ledger-3 (I5, I7); `honest` puts it back |

`python scripts/demo.py state` shows each farm's water at any moment.
Wait with `Start-Sleep 4` in PowerShell.

## Kubernetes (optional, separate)
See `k8s/README.md`. Stop the Docker version first (`docker compose down`), and bring it back with `docker compose up -d`.

## A farm loses its data (recovery)

```powershell
docker compose stop farm-a
docker compose rm -f farm-a
docker volume rm aquashare_ds_farm-a-data
docker compose up -d farm-a
Start-Sleep 10
docker logs aq-farm-a --tail 8
python scripts/replace_farm_key.py --farm farm-a
Start-Sleep 8
docker logs aq-farm-a --tail 5
python scripts/demo.py state
```
You see: after the restart the farm rebuilds its balance from the ledger ("rebuilt this farm's balance from the ledger"), so it does not believe it has its whole share again. Its new key is refused ("already has a different key"). The operator replaces the key, the farm registers again and its reports are accepted. A farm's own certificate cannot do this (403).
