# 09 - Deployment & operations

_Last updated: 2026-06-09._

The app ships as a single Docker image (`Dockerfile.backend`): Node build → Maven build → JRE. The
**frontend is compiled into the backend JAR** (Spring Boot static resources), so there is no separate
frontend deployment.

Two deployment targets are supported:

| Target | Entry point | Ingress class | Image |
|--------|-------------|---------------|-------|
| **Minikube** (local dev) | `./scripts/bootstrap-cluster.sh` | nginx (minikube addon) | `localhost/fx-oee-backend:dev` |
| **Hetzner k3s** (production) | GitHub Actions `deploy-hetzner.yml` | Traefik (k3s built-in) | `ghcr.io/mc139/fx-oee-backend:latest` |

---

## Kustomize overlay structure

Environment-specific settings live in `k8s/overlays/`:

```
k8s/
├── base/                       ← prod values; do not apply directly
│   ├── kustomization.yaml
│   ├── ingress.yaml            ← Traefik, host: fxoee.mcieslik.me
│   ├── backend/deployment.yaml ← ghcr.io image, Always pull, imagePullSecrets
│   └── …
└── overlays/
    ├── local/                  ← Minikube patches
    │   ├── kustomization.yaml
    │   └── patches/
    │       ├── ingress.yaml    ← ingressClassName: nginx
    │       └── ingress-host.json ← host: fx-oee.local (app)
    │       └── deployment.json ← localhost image, IfNotPresent, no imagePullSecrets
    └── prod/                   ← Hetzner k3s (references base; no patches)
        └── kustomization.yaml
```

**Always apply via an overlay**, never from `k8s/` directly:

```bash
# local
kubectl apply -k k8s/overlays/local/

# prod (CI does this automatically; manual override)
kubectl apply -k k8s/overlays/prod/
```

---

## Minikube (local development)

```mermaid
flowchart TB
    subgraph mk["Minikube cluster - namespace: fx-oee"]
        ing["ingress-nginx\nlocalhost"]
        be["Deployment: backend\nlocalhost/fx-oee-backend:dev  :8080"]
        subgraph stateful["StatefulSets"]
            pg[("postgres-0\nPVC: postgres-data")]
            kf["kafka-0 (emptyDir)"]
            zk["zookeeper-0 (emptyDir)"]
        end
        subgraph obs["Observability"]
            prom["prometheus :9090"]
            graf["grafana :3000"]
            exp["postgres-exporter :9187"]
            kui["kafka-ui"]
        end
    end
    ing --> be & graf & prom
    be --> pg & kf
    kf --> zk
    prom -->|scrape /actuator/prometheus| be
    prom -->|scrape| exp --> pg
    graf --> prom
```

### Prerequisites

`minikube`, `kubectl`, `docker` or `podman` (auto-detected; podman preferred if both present).

### One-time bootstrap

```bash
minikube start
./scripts/bootstrap-cluster.sh
```

[bootstrap-cluster.sh](../scripts/bootstrap-cluster.sh) enables the `ingress` and `metrics-server`
addons, then runs `kubectl apply -k k8s/overlays/local/` (retrying until the nginx admission webhook
is serving). Idempotent, so it's safe to re-run.

### Build & deploy the backend

```bash
# Full redeploy: resets Postgres, rebuilds image, rolls out backend + observability
./scripts/deploy-all.sh

# Add --wipe to also recycle Kafka/Zookeeper (all topics + offsets lost)
./scripts/deploy-all.sh --wipe

# Backend only: rebuild image + rollout, no DB or observability touch
./scripts/deploy-minikube.sh
```

[deploy-minikube.sh](../scripts/deploy-minikube.sh) builds `localhost/fx-oee-backend:dev` directly
into Minikube's container runtime (`eval $(minikube docker-env)` for docker; podman builds locally
then `minikube image load`). After applying the base backend manifests it patches `imagePullPolicy`
to `IfNotPresent` and removes `imagePullSecrets` so no registry auth is attempted.

> **Data lifecycle.** `deploy-all.sh` **deletes the Postgres PVC** on every run (fresh DB).
> `--wipe` additionally scales Kafka/Zookeeper to zero (emptyDir destroyed, all topics lost).

### Accessing services (port-forward)

`deploy-all.sh` sets up port-forwards automatically at the end. To do it manually:

```bash
kubectl port-forward -n fx-oee svc/backend    8080:8080 &
kubectl port-forward -n fx-oee svc/grafana    3000:3000 &
kubectl port-forward -n fx-oee svc/prometheus 9091:9090 &
kubectl port-forward -n fx-oee svc/kafka-ui   9090:9090 &
```

Or use the ingress (requires `/etc/hosts` entry):

```bash
echo "$(minikube ip) fx-oee.local grafana.fx-oee.local prometheus.fx-oee.local" | sudo tee -a /etc/hosts
```

| URL | Serves |
|-----|--------|
| `http://fx-oee.local` | app (frontend + REST + WebSocket) |
| `http://grafana.fx-oee.local` | Grafana (admin/admin) |
| `http://prometheus.fx-oee.local` | Prometheus |

The remote debugger (JDWP, `suspend=n`) listens on container port **5005**.

### Hybrid dev: local backend + frontend, Minikube infra

For fast iteration, keep Postgres, Kafka, Zookeeper, and observability in Minikube and run
**Spring Boot + Vite** on the host. Frontend changes hot-reload instantly on
`http://localhost:5173`; Vite proxies `/api` and `/ws` to the local backend on `:8080`.

```mermaid
flowchart LR
    subgraph host["Host"]
        FE["Vite :5173\nHMR"]
        BE["Spring Boot\n:8080"]
        PF["kubectl port-forward"]
    end
    subgraph mk["Minikube fx-oee"]
        PG[("postgres-0")]
        KF["kafka-0"]
        OBS["grafana / prometheus / kafka-ui"]
    end
    FE -->|/api /ws| BE
    BE --> PF --> PG & KF
```

**Prerequisites:** cluster bootstrapped (`./scripts/bootstrap-cluster.sh`). Observability is
optional. Deploy once with `./scripts/deploy-all.sh` if you want Grafana/Prometheus in-cluster
(remember `deploy-all` resets the Postgres PVC every run).

**Kafka from the host:** the broker exposes a second **EXTERNAL** listener on port **9093** that
advertises `localhost:9093` (see `k8s/base/kafka/kafka.yaml`). `dev-local-backend.sh` port-forwards
that port and sets `KAFKA_BOOTSTRAP_SERVERS=localhost:9093`. No `/etc/hosts` entry needed. On
first run (or after upgrading manifests) the script patches and restarts Kafka if the EXTERNAL
listener is missing.

**Daily workflow:**

```bash
# Port-forward postgres + kafka, scale k8s backend to 0, write .env.local
./scripts/dev-local-backend.sh

# Full dev stack: backend + Vite (open http://localhost:5173 for UI work)
./scripts/dev-local-backend.sh --run

# Backend only, for attaching an IDE debugger (--be-only skips Vite)
./scripts/dev-local-backend.sh --run --be-only

# Also forward grafana (3000), kafka-ui (9090), prometheus (9091)
./scripts/dev-local-backend.sh --obs --run

# Restore cluster backend and kill port-forwards
./scripts/dev-local-backend.sh --stop
```

| Mode | UI | Backend | Postgres / Kafka |
|------|-----|---------|------------------|
| Full Minikube (`deploy-minikube.sh`) | Embedded in JAR | Pod | In-cluster |
| Hybrid `--run` | Vite `:5173` HMR | Host `mvn` | Port-forward |
| Hybrid `--run --be-only` | Embedded `:8080` (stale) | Host `mvn` + IDE debugger | Port-forward |
| docker-compose | Embedded or separate Vite | Container | Compose network |

The script writes `.env.local` (gitignored) with the same variables as the k8s ConfigMap/Secret.
`--run` waits for `/actuator/health` before starting Vite so the first page load succeeds.
`npm install` in `frontend/` runs automatically on first use if `node_modules` is missing.

> **Note.** In-cluster Prometheus scrapes `backend:8080` only. Metrics from a host-run backend
> are not collected unless you add a scrape target manually.

---

## Production: Hetzner k3s

```mermaid
flowchart LR
    gh["GitHub master branch"]
    ci["CI workflow\n(tests)"]
    build["deploy-hetzner.yml\nbuild + push to ghcr.io"]
    reg["ghcr.io/mc139/fx-oee-backend:latest"]
    srv["Hetzner VPS\n167.233.48.58"]
    cf["Cloudflare proxy\nfxoee.mcieslik.me"]
    user["Browser"]

    gh -->|push| ci -->|success| build --> reg
    build -->|SSH kubectl apply -k overlays/prod/| srv
    user -->|HTTPS| cf -->|HTTP :80| srv
```

### Infrastructure

| Component | Details |
|-----------|---------|
| VPS | Hetzner Ubuntu, 1 node, k3s |
| Ingress | Traefik (k3s built-in), port 80 |
| DNS / SSL | Cloudflare proxy ON, SSL mode **Flexible** |
| Domain | `fxoee.mcieslik.me` → `167.233.48.58` |
| Image registry | `ghcr.io/mc139/fx-oee-backend` (private) |
| Manifests on server | `/opt/fx-oee/` (git clone of this repo) |

### CI/CD pipeline

Push to `master` → CI passes → `deploy-hetzner.yml` runs:

1. **Build & push**: multi-stage Docker build, pushes `:latest` to ghcr.io
2. **SSH deploy**: connects to Hetzner, runs:
   ```bash
   git pull --ff-only                              # sync manifests
   kubectl apply -f k8s/base/namespace.yaml        # ensure namespace
   kubectl create secret docker-registry ghcr-secret ...   # registry auth
   kubectl apply -k k8s/overlays/prod/             # all resources (Kustomize)
   kubectl rollout restart deployment/backend      # force new image
   kubectl rollout status deployment/backend --timeout=300s
   ```

### Required GitHub secrets

| Secret | Value |
|--------|-------|
| `HETZNER_HOST` | `167.233.48.58` |
| `HETZNER_SSH_KEY` | Private key for `root@<server>` (ed25519, generated by `hetzner-init.sh`) |
| `GHCR_TOKEN` | GitHub PAT with `read:packages` scope (server pulls private image) |

### One-time server setup

```bash
# On a fresh Hetzner Ubuntu server (as root):
bash <(curl -fsSL https://raw.githubusercontent.com/mc139/fx-oee/master/scripts/hetzner-init.sh)
```

[hetzner-init.sh](../scripts/hetzner-init.sh) installs k3s, generates SSH keys, and prints the
values to paste into GitHub secrets. Then clone the repo:

```bash
GIT_SSH_COMMAND='ssh -i /root/.ssh/fx-oee-deploy' \
  git clone git@github.com:mc139/fx-oee.git /opt/fx-oee
```

Bootstrap the cluster manually once (CI handles all subsequent deploys):

```bash
cd /opt/fx-oee
kubectl apply -f k8s/base/namespace.yaml
kubectl apply -k k8s/overlays/prod/
```

---

## Pod resources & probes

[deployment.yaml](../k8s/base/backend/deployment.yaml) requests `500m` CPU / `1Gi`, limits `2` CPU /
`1.5Gi`. JVM flags: `-Xms512m -Xmx1200m -XX:+UseG1GC -XX:MaxGCPauseMillis=100`.

| Probe | Config |
|-------|--------|
| startupProbe | `/actuator/health`, 60 × 10s (up to 10 min; cold boot is slow) |
| readinessProbe | `/actuator/health`, every 10s |
| livenessProbe | `/actuator/health`, every 15s, 4 failures → restart |

---

## Observability

Prometheus scrapes `/actuator/prometheus` (Micrometer) every 15s. Two Grafana dashboards are
provisioned automatically by `deploy-all.sh`, no manual import needed:

| Dashboard | Source |
|-----------|--------|
| **FX-OEE Trading Engine** (home) | `docker/grafana/dashboards/fxoee.json` (in repo) |
| **PostgreSQL Database** (ID 9628) | fetched from grafana.com at deploy time |

Custom metrics:

| Metric | Type | Tags |
|--------|------|------|
| `orders.placed.total` | Counter | `pair`, `side` |
| `matching.latency` | Timer + histogram | `pair` |
| `orderbook.depth` | Gauge | `pair` |
| `trades.volume.total` | Counter | `pair` |

`matching.latency` publishes Prometheus histogram buckets, enabling `histogram_quantile` for
p50/p99/p999 in Grafana. `postgres-exporter` (`:9187`) feeds the PG dashboard. Kafka UI is
deployed for topic inspection.

---

## docker-compose (lightweight local alternative)

```bash
docker compose up --build
```

[docker-compose.yml](../docker-compose.yml) starts nginx, backend, postgres, zookeeper, kafka,
postgres-exporter, prometheus, and grafana with health-gated ordering. nginx fronts the app on
port 80, and the backend is also reachable directly on 8080.

| Service | Host port |
|---------|-----------|
| nginx (app entry) | 80 |
| backend (app) | 8080 |
| postgres | 5432 |
| kafka | 9092 |
| postgres-exporter | 9187 |
| prometheus | 9090 |
| grafana | 3000 |

---

## Configuration reference

All runtime knobs are environment variables consumed by
[application.yml](../src/main/resources/application.yml). See
[Configuration reference](10-configuration.md) for the full list.
