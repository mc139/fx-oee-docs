# Deploy FX-OEE to local Minikube

Step-by-step. Run from repo root (`/Users/Shared/gitrepos/fx-oee`).
Docker Compose stays usable — this is independent.

---

## Step 0 — Prerequisites (one time)

```bash
# macOS via Homebrew
brew install minikube kubectl

minikube version
kubectl version --client
docker info >/dev/null && echo "docker OK"
```

---

## Step 1 — Start the cluster

Kafka + two JVMs are heavy. Give it room.

```bash
minikube start --cpus=4 --memory=8192 --driver=docker
minikube status
```

---

## Step 2 — Enable the ingress controller

```bash
minikube addons enable ingress
kubectl -n ingress-nginx get pods        # wait until controller is Running
```

---

## Step 3 — Build images INTO Minikube's docker daemon

Critical: `eval` and both `docker build` run in the **same shell**. No registry
is used, so the cluster can only see images built here.

```bash
eval $(minikube docker-env)

docker build -t fx-oee-backend:dev  -f Dockerfile.backend .
docker build -t fx-oee-frontend:dev frontend/

docker images | grep fx-oee             # confirm both tags exist
```

---

## Step 4 — Apply manifests

Namespace first, then everything (recursive).

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -R -f k8s/
```

---

## Step 5 — Watch the rollout

```bash
kubectl -n fx-oee get pods -w
```

Expected order: `postgres-0` and `zookeeper-0` Ready → `kafka-0` Ready →
`backend` Ready (slow: Maven compiles on first boot, several minutes) →
`frontend`, `prometheus`, `grafana` Ready. Ctrl-C when all show `1/1 Running`.

Stuck? See `docs/kubernetes/TROUBLESHOOTING.md` §3.

---

## Step 6 — Map hostnames

```bash
echo "$(minikube ip) fx-oee.local grafana.fx-oee.local prometheus.fx-oee.local" | sudo tee -a /etc/hosts
```

---

## Step 7 — Open the app

macOS + docker driver: the Minikube IP is not directly routable. Pick one.

**Option A — ingress via tunnel** (separate terminal, keep it open, needs sudo):
```bash
minikube tunnel
```
Then:
```bash
open http://fx-oee.local
open http://grafana.fx-oee.local       # admin / admin
open http://prometheus.fx-oee.local
```
If the frontend shows *"This host is not allowed"*, see TROUBLESHOOTING §3.2.

**Option B — port-forward** (no tunnel, host becomes localhost):
```bash
kubectl -n fx-oee port-forward svc/frontend   5173:5173 &
kubectl -n fx-oee port-forward svc/grafana    3000:3000 &
kubectl -n fx-oee port-forward svc/prometheus 9090:9090 &
open http://localhost:5173
```

---

## Step 8 — Verify

```bash
kubectl -n fx-oee get pods,svc,ingress
kubectl -n fx-oee exec deploy/backend -- wget -qO- http://localhost:8080/actuator/health
```

---

## Update an image after code changes

```bash
eval $(minikube docker-env)
docker build -t fx-oee-backend:dev -f Dockerfile.backend .
kubectl -n fx-oee rollout restart deploy/backend
kubectl -n fx-oee rollout status  deploy/backend
```

(Replace `backend` with `frontend` for frontend changes.)

---

## Tear down

```bash
kubectl delete namespace fx-oee          # removes pods, svcs, PVCs (DATA LOST)
# full cluster wipe:
minikube delete
# detach shell from Minikube docker:
eval $(minikube docker-env -u)
```
