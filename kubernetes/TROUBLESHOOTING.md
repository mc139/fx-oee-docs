# FX-OEE Kubernetes — Troubleshooting & Change Log

This document records **every change made to add Kubernetes support**, the
reasoning behind non-obvious decisions, and a troubleshooting playbook for the
problems you are most likely to hit on Minikube.

Docker Compose is **unchanged** — `docker-compose.yml`, `Dockerfile.backend`,
and `.env` all still work exactly as before. The Kubernetes setup lives entirely
under `k8s/` and is purely additive.

---

## 1. What was added

All new files, nothing existing was modified:

```
k8s/
├── namespace.yaml                 # namespace: fx-oee
├── postgres/
│   ├── secret.yaml                # DB name/user/password
│   ├── service.yaml               # headless svc "postgres"
│   └── statefulset.yaml           # 1 replica + PVC (volumeClaimTemplates)
├── kafka/
│   ├── zookeeper.yaml             # headless svc + StatefulSet
│   └── kafka.yaml                 # headless svc + StatefulSet
├── backend/
│   ├── configmap.yaml             # non-secret env (DB host, kafka, risk limits)
│   ├── secret.yaml                # DB_USER / DB_PASSWORD
│   ├── service.yaml               # ClusterIP "backend"
│   └── deployment.yaml            # Spring Boot, slow-start probes
├── observability/
│   ├── prometheus.yaml            # ConfigMap + Deployment + Service
│   └── grafana.yaml               # Secret + Deployment + Service
└── ingress.yaml                   # fx-oee.local + grafana/prometheus hosts
```

### Quick start

```bash
# 1. Cluster with enough RAM for Kafka + JVM
minikube start --cpus=4 --memory=8192 --driver=docker

# 2. Ingress controller
minikube addons enable ingress

# 3. Build image INTO Minikube's docker daemon (no registry)
eval $(minikube docker-env)
docker build -t fx-oee-backend:dev -f Dockerfile.backend .

# 4. Apply everything (namespace first)
kubectl apply -f k8s/namespace.yaml
kubectl apply -R -f k8s/

# 5. Watch rollout
kubectl -n fx-oee get pods -w

# 6. Map hostnames -> minikube IP
echo "$(minikube ip) fx-oee.local grafana.fx-oee.local prometheus.fx-oee.local" | sudo tee -a /etc/hosts

# 7. Open
open http://fx-oee.local
```

---

## 2. Key design decisions (and why)

### 2.1 Frontend is served from the backend, not a separate pod

The React/Vite app in `frontend/` is built by `npm run build` (output to
`src/main/resources/static/`). `Dockerfile.backend` includes a Node build stage
that runs this automatically. Spring Boot serves the static files at port 8080
alongside the API and WebSocket endpoints. There is no separate frontend
Deployment, Service, or Ingress route — **all browser traffic enters through
`backend:8080`**.

### 2.2 PostgreSQL uses `volumeClaimTemplates`, not a standalone PVC

The README sketched a separate `pvc.yaml`. A `StatefulSet` with
`volumeClaimTemplates` is the idiomatic choice — it binds a stable PVC to the
pod identity and survives pod restarts. No separate PVC file exists by design.

`PGDATA` is set to a **subdirectory** (`/var/lib/postgresql/data/pgdata`)
because some storage classes put a `lost+found` directory at the volume root,
which makes Postgres `initdb` refuse to run ("directory not empty").

### 2.3 Kafka advertised listener

Single broker. The broker advertises
`PLAINTEXT://kafka.fx-oee.svc.cluster.local:9092`. This **must** be a name every
client can resolve. Using the bare `kafka` would also work *within* the
namespace, but the FQDN works from anywhere and is unambiguous. The backend
bootstraps on `kafka:9092`; the broker then hands back the FQDN, which resolves
to the same pod. Both Kafka and Zookeeper are `StatefulSet`s so the pod name
(`kafka-0`) is stable across restarts — a plain Deployment would assign random
pod names and break broker discovery.

### 2.4 Backend probes are deliberately slow

`Dockerfile.backend` runs `mvn -B spring-boot:run`. On first boot it resolves
dependencies and **compiles the project inside the container**, which can take
several minutes. The `startupProbe` allows up to **10 minutes**
(`failureThreshold: 60 × periodSeconds: 10`) before the `livenessProbe` is
allowed to kill the pod. If you see the backend pod `CrashLoopBackOff` early,
this is almost always the cause — see §3.4.

### 2.5 Config split: ConfigMap vs Secret

- `backend-config` ConfigMap: non-secret env (`DB_HOST`, `KAFKA_BOOTSTRAP_SERVERS`,
  risk limits, circuit-breaker threshold). Mirrors `.env` non-secret values.
- `backend-secret` / `postgres-secret` / `grafana-secret`: credentials.

Spring Boot consumes them through the **same `${PROPERTY}` placeholders** in
`application.yml` — no application code changed.

> ⚠️ Secrets here use `stringData` with default dev passwords (`fxoee` /
> `admin`) to match `.env`. **Do not use these manifests as-is in production.**
> Replace with sealed-secrets / External Secrets / Vault.

---

## 3. Troubleshooting playbook

### 3.0 First commands to run for ANY problem

```bash
kubectl -n fx-oee get pods
kubectl -n fx-oee describe pod <pod>
kubectl -n fx-oee logs <pod> [-c <container>] [--previous]
kubectl -n fx-oee get events --sort-by=.lastTimestamp | tail -30
```

---

### 3.1 `ErrImageNeverPull` / `ImagePullBackOff` on backend

**Cause:** image not present in Minikube's docker daemon. The manifests use
`imagePullPolicy: IfNotPresent` and a local tag (`fx-oee-backend:dev`) — there
is no registry to pull from.

**Fix:**
```bash
eval $(minikube docker-env)          # point docker at Minikube's daemon
docker images | grep fx-oee          # confirm images exist HERE
docker build -t fx-oee-backend:dev -f Dockerfile.backend .
kubectl -n fx-oee rollout restart deploy/backend
```
The classic mistake: building in your normal shell, then `kubectl` can't see
the image. The `eval $(minikube docker-env)` must be run in the **same shell**
as `docker build`. Undo with `eval $(minikube docker-env -u)`.

---

### 3.2 Ingress host returns 404 / connection refused

Checklist:
```bash
minikube addons list | grep ingress          # must be "enabled"
minikube addons enable ingress
kubectl -n ingress-nginx get pods             # controller Running?
kubectl -n fx-oee get ingress                 # ADDRESS populated?
grep fx-oee.local /etc/hosts                  # mapped to `minikube ip`?
```
On macOS with the **docker** driver, the Minikube IP is **not** directly
routable. Either run `minikube tunnel` (separate terminal, needs sudo) **or**
use `kubectl port-forward svc/backend 8080:8080`. The `/etc/hosts` entry must
point at the value of `minikube ip`.

---

### 3.3 Backend pod `CrashLoopBackOff` or stuck `0/1 Running`

**Most common cause:** slow Maven build/startup exceeding probe windows, or DB
/ Kafka not ready yet.

```bash
kubectl -n fx-oee logs deploy/backend --tail=100
kubectl -n fx-oee logs deploy/backend --previous   # last crashed instance
```

- `Connection refused: postgres/5432` or `kafka/9092` → backend started before
  its dependencies. It will retry; confirm `postgres-0` and `kafka-0` are
  `Running` and `READY 1/1`. Spring Boot + Flyway will reconnect once they are.
- Pod killed at ~exactly the liveness window → build slower than the
  `startupProbe` budget. Increase `failureThreshold` in
  `k8s/backend/deployment.yaml` (`startupProbe`) and re-apply.
- `OOMKilled` (check `kubectl describe pod`) → raise the backend memory
  `limits` (default `1536Mi`); the Maven JVM + app JVM together are heavy.

---

### 3.4 Postgres pod won't start — "data directory not empty" / Pending

```bash
kubectl -n fx-oee describe pod postgres-0
kubectl -n fx-oee get pvc
```
- PVC stuck `Pending` → no default StorageClass.
  `minikube addons enable storage-provisioner` (usually on by default) and
  check `kubectl get storageclass` shows `standard (default)`.
- `initdb: directory "/var/lib/postgresql/data" exists but is not empty` →
  handled already via `PGDATA=/var/lib/postgresql/data/pgdata`. If you removed
  that env var, restore it.
- Wiping Postgres state: `kubectl -n fx-oee delete pvc postgres-data-postgres-0`
  then delete the pod. **Destroys all data.**

---

### 3.5 Kafka `CrashLoopBackOff` / clients can't connect

```bash
kubectl -n fx-oee logs kafka-0
kubectl -n fx-oee logs zookeeper-0
```
- Kafka starts before Zookeeper is up → it retries; wait for `zookeeper-0` to be
  `1/1`. If it never recovers, restart: `kubectl -n fx-oee delete pod kafka-0`.
- Backend logs show Kafka reachable then **timeout on produce/consume** →
  advertised-listener mismatch. Confirm
  `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka.fx-oee.svc.cluster.local:9092`
  in `k8s/kafka/kafka.yaml` and that the backend uses `kafka:9092` as bootstrap
  (`backend-config` ConfigMap). The bootstrap name and the advertised name must
  both resolve to the broker pod.
- Kafka `OOMKilled` → raise memory `limits` (default `2Gi`) or give Minikube
  more RAM (`minikube start --memory=8192`).

---

### 3.6 Prometheus shows no `fx-oee` target / target DOWN

```bash
kubectl -n fx-oee port-forward svc/prometheus 9090:9090
# open http://localhost:9090/targets
```
- Target DOWN → backend not `READY`, or actuator path wrong. Verify:
  `kubectl -n fx-oee exec deploy/prometheus -- wget -qO- http://backend:8080/actuator/prometheus | head`
- Scrape config is **static** (`targets: ['backend:8080']`) in the
  `prometheus-config` ConfigMap. After editing the ConfigMap you must restart:
  `kubectl -n fx-oee rollout restart deploy/prometheus`.
- The backend pod carries `prometheus.io/scrape` annotations for future
  Kubernetes service-discovery, but the current config does **not** use them —
  editing annotations alone changes nothing until the scrape config does.

---

### 3.7 Grafana — can't log in / no data

- Default creds `admin` / `admin` (`grafana-secret`). Changed the Secret but
  still locked out? Grafana persists the admin password in its DB after first
  boot. This deployment uses an `emptyDir` (non-persistent) volume, so deleting
  the pod resets Grafana entirely, including the password.
- "No data" panels → add Prometheus as a datasource with URL
  `http://prometheus:9090` (in-cluster Service DNS), not `localhost`.

---

### 3.8 Everything `Pending` / scheduler can't place pods

```bash
kubectl -n fx-oee describe pod <pod> | grep -A5 Events
kubectl top nodes      # needs metrics-server: minikube addons enable metrics-server
```
`Insufficient cpu/memory` → the sum of resource **requests** exceeds the
Minikube node. Either start Minikube bigger
(`minikube start --cpus=4 --memory=8192`) or lower the `requests` in the
deployments/statefulsets. Kafka (1Gi) + backend (768Mi) + Postgres (256Mi) are
the heavy hitters.

---

### 3.9 Clean reset

```bash
kubectl delete namespace fx-oee          # removes everything incl. PVCs
# or nuke the whole cluster:
minikube delete && minikube start --cpus=4 --memory=8192 --driver=docker
```

---

## 4. Docker Compose vs Kubernetes — equivalence map

| Concern        | Docker Compose                | Kubernetes                                   |
|----------------|-------------------------------|----------------------------------------------|
| Backend env    | `.env` + compose `environment`| `backend-config` ConfigMap + `backend-secret`|
| DB persistence | `postgres_data` volume        | `volumeClaimTemplates` PVC on StatefulSet    |
| Kafka identity | container name `kafka`        | StatefulSet pod `kafka-0` + headless svc     |
| Service DNS    | compose network aliases       | `*.fx-oee.svc.cluster.local`                 |
| Browser access | `localhost:8080`              | `fx-oee.local` Ingress / port-forward        |
| Prometheus cfg | `docker/prometheus.yml` mount | `prometheus-config` ConfigMap                |
| Grafana creds  | `.env` `GRAFANA_ADMIN_*`      | `grafana-secret`                             |

Both paths are independent. Changing one does not affect the other unless you
edit a shared file (`Dockerfile.backend`, `application.yml`, or files under
`frontend/`).

---

## 5. Validation note

`kubectl apply --dry-run=client` and `--validate` both require a reachable
cluster (they download the OpenAPI schema). To lint these manifests **without**
a cluster, use a schema validator:

```bash
# kubeconform (recommended)
brew install kubeconform
kubeconform -strict -summary -ignore-missing-schemas $(find k8s -name '*.yaml')

# or against a live cluster
kubectl apply -R -f k8s/ --dry-run=server
```
