# Deploy the AquaShare ledger and gateways on Docker Desktop's Kubernetes.
# Run from the project folder:  powershell -ExecutionPolicy Bypass -File k8s\deploy.ps1
# This does not touch the docker-compose version.
$ErrorActionPreference = "Stop"
kubectl config use-context docker-desktop
docker build -t aquashare-ledger:k8s -f ledger_node/Dockerfile .
docker build -t aquashare-gateway:k8s -f gateway/Dockerfile .
kubectl create configmap ledger-schema --from-file=schema.sql=ledger_node/schema.sql --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/ledger.yaml
kubectl apply -f k8s/gateway.yaml
Write-Host "Waiting for the pods to start..."
kubectl rollout status statefulset/ledger --timeout=180s
kubectl rollout status deployment/gateway --timeout=120s
kubectl get pods -o wide
