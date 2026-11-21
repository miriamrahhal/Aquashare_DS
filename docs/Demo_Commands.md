# AquaShare terminal demo

Open PowerShell in the project folder and activate the environment once:

```powershell
cd C:\Users\User\Desktop\Aquashare_DS
.venv\Scripts\activate
```

Every command uses the operator certificate by itself (it reads `.env`), so it works with mutual TLS on.
Run `python scripts/demo.py` alone to list every scenario.

## The whole live demo in one command

```powershell
python scripts/demo.py all
```

It runs scenarios 1 to 12 below in order, and pauses after each one so you can explain it (press Enter to go on). It takes about 3 to 4 minutes, plus the time you talk.

## Before class (optional): start fresh on today's date

```powershell
docker compose down -v
docker compose up -d --build
Start-Sleep 30
python scripts/setup_today.py
```

This empties the ledger, registers the four farms and opens today's day.

## The live scenarios (included in `all`)

| # | Scenario | Command | What you show |
|---|---|---|---|
| 1 | Start a new day | `python scripts/demo.py day` | Each farm gets its share, agreed by all 3 ledger nodes |
| 2 | A farm goes offline | `python scripts/demo.py offline` | `pending` grows while offline and drops back after the link returns |
| 3 | Same transfer twice | `python scripts/demo.py duplicate` | Both resends answer `"duplicate": true`: counted once |
| 4 | Spend too much | `python scripts/demo.py overspend` | `HTTP 409`, not enough water |
| 5 | Crash the leader | `python scripts/demo.py crash` | A new leader in a higher term; the old one comes back as a follower with the same commit |
| 6 | Lose the majority | `python scripts/demo.py majority` | With one node left a transfer stays `pending` (water on hold); after the restart it is `committed` by itself |
| 7 | Prove a record is real | `python scripts/demo.py proof` | `VERIFIED`, then `VERIFICATION FAILED` for the falsified copy |
| 8 | A fake farm | `python scripts/demo.py fake` | Two `HTTP 409 REFUSED`: signature missing, then invalid signature |
| 9 | No certificate | `python scripts/demo.py nocert` | `400 No required SSL certificate was sent` |
| 10 | Wrong role | `python scripts/demo.py forbidden` | `HTTP 403 certificate 'farm-a' is not allowed to open a day` |
| 11 | A lying ledger node | `python scripts/demo.py lie` then `python scripts/demo.py check` | The gateway still shows the true water with `OUTVOTED: ledger-3`; the checker fails I5 and I7 on ledger-3 only |
| 12 | Undo the lie | `python scripts/demo.py honest` then `python scripts/demo.py check` | `ALL INVARIANTS HOLD` |

Always finish a lying-node demo with `honest`.

## Longer scenarios (run separately)

| Scenario | Command | Time | What you show |
|---|---|---|---|
| A farm loses all its data | `python scripts/demo.py recover` | ~30 s | The farm rebuilds its balance from the ledger; its new key is refused until the operator replaces it; then its reports are accepted |
| Network partition | `python scripts/demo.py partition` | ~1 min | The isolated leader cannot commit; the majority elects a new leader; after healing all nodes hold the same log |
| Chaos test | `python scripts/demo.py chaos` | ~3 min | Random crashes while clients write: nothing lost, nothing counted twice |
| Benchmark | `python scripts/demo.py benchmark` | several min | Throughput, latency and failover time |
| Spark season report | `python scripts/demo.py spark` | ~30 s | Who runs out of water, who helps whom, suggested shares for next season |
| Automated tests | `python scripts/demo.py tests` | ~1 min | `108 passed` |
| Kubernetes | `python scripts/demo.py kubernetes` | ~3 min | The leader pod is deleted; Raft elects a new leader and Kubernetes recreates the pod. Stops the Docker version first and starts it again at the end (Docker Desktop's Kubernetes must be on) |

## Useful at any moment

| Command | Shows |
|---|---|
| `python scripts/demo.py state` | Each farm's water today, and which ledger nodes agreed on it |
| `python scripts/demo.py status` | The 3 ledger nodes: who leads, term, commit |
| `python scripts/demo.py transfer` | The farm with the most water sends 100 L to another farm |
| `docker logs -f aq-farm-a` | A farm's log, live (stop with Ctrl + C, the keys) |

## Without mutual TLS

Rename `.env` to `env.off` and run `docker compose up -d`. The scenarios still work, except `nocert` and `forbidden`: do not run them without TLS. Without certificates there are no roles, so `forbidden` would really open a day in 2099.
