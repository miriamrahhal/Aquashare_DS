# AquaShare on Kubernetes (optional)

A second way to run the core of AquaShare: the three Raft ledger nodes (each with its own
PostgreSQL database) and the gateways. The normal `docker compose` version is not changed.

## Start
1. Docker Desktop → Settings → Kubernetes → Enable.
2. From the project folder:
   ```powershell
   powershell -ExecutionPolicy Bypass -File k8s\deploy.ps1
   ```
3. Check:
   ```powershell
   kubectl get pods
   powershell -ExecutionPolicy Bypass -File k8s\status.ps1
   Invoke-RestMethod http://localhost:30600/state
   ```

## Demo 1: Kubernetes heals a crashed ledger node
```powershell
powershell -ExecutionPolicy Bypass -File k8s\status.ps1      # find the leader, e.g. ledger-1
kubectl delete pod ledger-1                                   # kill it
powershell -ExecutionPolicy Bypass -File k8s\status.ps1      # another node is leader, ledger-1 DOWN
kubectl get pods -w                                           # Kubernetes recreates ledger-1 (Ctrl+C)
powershell -ExecutionPolicy Bypass -File k8s\status.ps1      # ledger-1 back as follower, same commit
```
The new pod gets the same name and the same disk, so it keeps its database and Raft
brings it up to date.

## Demo 2: scale the gateways
```powershell
kubectl scale deployment gateway --replicas=4
kubectl get pods -l app=gateway
```

## Stop and clean up
```powershell
kubectl delete -f k8s/gateway.yaml -f k8s/ledger.yaml
kubectl delete pvc -l app=ledger          # also delete the databases (optional)
```
If you run both versions at once, they do not conflict (different ports), but your laptop
needs the memory for both. Stopping the compose version first is safer.
