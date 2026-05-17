# Running FX-OEE on Kubernetes (Minikube)

This document explains why running the FX Order Execution Engine on Kubernetes is
valuable, what components are required, and how the moving parts fit together.

---

## 1. Why run this on Kubernetes?

The project currently runs via `docker-compose`, which is fine for a single
developer machine. Moving to Kubernetes (locally via Minikube) buys us several
properties that matter for a trading system:

| Concern | Docker Compose | Kubernetes |
|---|---|---|
| **Production parity** | Different runtime than prod | Same primitives as any cloud cluster (EKS/GKE/AKS) |
| **Self-healing** | Crashed container stays dead until restart | Failed pods are automatically rescheduled |
| **Rolling deploys** | Stop-the-world restart | Zero-downtime rolling updates |
| **Service discovery** | DNS via compose network only | Native DNS, labels, selectors |
| **Config management** | `.env` file | `ConfigMap` + `Secret` with RBAC |
| **Resource isolation** | Best-effort | Hard CPU/memory requests & limits per pod |
| **Horizontal scale** | Manual `--scale` | `HorizontalPodAutoscaler` based on metrics |
| **Stateful workloads** | Volumes only | `StatefulSet` with stable identity + ordered start |

For an FX execution engine specifically, the wins are:
- **Latency-sensitive components** (the Disruptor-backed matching engine) get
  guaranteed CPU and memory via pod resource requests.
- **Kafka + Zookeeper** get stable network identities via `StatefulSet`, which
  is what brokers require to advertise listeners correctly.
- **PostgreSQL** gets a `PersistentVolumeClaim` so data survives pod restarts.
- **Prometheus** can use the native Kubernetes service discovery to find every
  pod that exposes `/actuator/prometheus` — no manual scrape config edits when
  pods change.

Minikube specifically gives us a single-node cluster on the developer laptop, so
the manifests we write here are the same ones we would later apply to a real
cluster — only the storage class and ingress class differ.

---

## 2. What needs to be built

### 2.1 Container images

| Image | Source | Notes |
|---|---|---|
| `fx-oee-backend` | `Dockerfile.backend` (already exists) | Spring Boot fat jar, port 8080 |
| `fx-oee-frontend` | New `frontend/Dockerfile` | Vite build → nginx static serve, port 80 |
| `postgres:16` | Docker Hub | No changes |
| `confluentinc/cp-kafka:7.6.0` | Docker Hub | No changes |
| `confluentinc/cp-zookeeper:7.6.0` | Docker Hub | No changes |
| `prom/prometheus` | Docker Hub | Custom scrape config via `ConfigMap` |
| `grafana/grafana` | Docker Hub | Dashboards mounted via `ConfigMap` |

Images must be built **inside Minikube's Docker daemon** so the cluster can pull
them without a registry:

```bash
eval $(minikube docker-env)
docker build -t fx-oee-backend:dev -f Dockerfile.backend .
docker build -t fx-oee-frontend:dev frontend/
```

### 2.2 Kubernetes manifests

Suggested layout under `k8s/`:

```
k8s/
├── namespace.yaml
├── postgres/
│   ├── statefulset.yaml
│   ├── service.yaml
│   ├── pvc.yaml
│   └── secret.yaml
├── kafka/
│   ├── zookeeper-statefulset.yaml
│   ├── kafka-statefulset.yaml
│   └── services.yaml
├── backend/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   └── secret.yaml
├── frontend/
│   ├── deployment.yaml
│   └── service.yaml
├── observability/
│   ├── prometheus-configmap.yaml
│   ├── prometheus-deployment.yaml
│   ├── grafana-deployment.yaml
│   └── services.yaml
└── ingress.yaml
```

### 2.3 Configuration

Anything currently in `.env` or `application.yml` that varies by environment
moves into one of two places:

- **`ConfigMap`** — non-secret values (Kafka bootstrap server, Prometheus URL,
  feature flags, log levels).
- **`Secret`** — credentials (Postgres password, JWT signing key, FIX session
  passwords).

These are projected into pods as environment variables, so Spring Boot picks
them up via the same `${PROPERTY}` placeholders it already uses.

---

## 3. How it works at runtime

### 3.1 Cluster topology

```
                ┌─────────────────────────────────────────────┐
                │              Minikube Node                  │
                │                                             │
   browser ───► │  Ingress ──► frontend (nginx) ─┐            │
                │                                ▼            │
                │                          backend (Spring)   │
                │                          │  │  │  │         │
                │                          ▼  ▼  ▼  ▼         │
                │                     postgres kafka          │
                │                             │               │
                │                          zookeeper          │
                │                                             │
                │   prometheus  ◄── scrape ── all /metrics    │
                │   grafana     ◄── query ── prometheus       │
                └─────────────────────────────────────────────┘
```

### 3.2 Networking

- Every pod gets a cluster-internal DNS name like
  `kafka-0.kafka.fx-oee.svc.cluster.local`.
- Backend talks to Postgres via `postgres.fx-oee.svc.cluster.local:5432`.
- Backend talks to Kafka via `kafka.fx-oee.svc.cluster.local:9092`.
- Browser reaches frontend and backend via an `Ingress` resource, exposed on the
  host with `minikube tunnel`.

### 3.3 Stateful services

- **Postgres** runs as a single-replica `StatefulSet` with a `PersistentVolumeClaim`.
  Restarting the pod re-attaches the same volume, so data persists.
- **Kafka + Zookeeper** run as `StatefulSet`s because brokers advertise their
  hostnames to clients. A stable name like `kafka-0` is required; a plain
  `Deployment` would assign random pod names that change on every restart and
  break broker discovery.

### 3.4 Stateless services

- **Backend** and **frontend** are `Deployment`s. Pods are interchangeable, so
  the scheduler can recreate them anywhere and rolling updates work cleanly.
- Backend pods are stateless because all state lives in Postgres and Kafka.

### 3.5 Observability

- Prometheus runs in-cluster and uses the Kubernetes API to discover every pod
  with the annotation `prometheus.io/scrape: "true"`.
- Backend already exposes `/actuator/prometheus`, so adding the annotation is
  the only change needed.
- Grafana is configured with Prometheus as a datasource and dashboards mounted
  from a `ConfigMap`.

### 3.6 Local access

Two options for hitting the app from the browser:

1. **`minikube tunnel`** — assigns a real IP to `LoadBalancer` services. Most
   production-like.
2. **`kubectl port-forward svc/frontend 5173:80`** — quick and dirty, no
   tunnel needed.

---

## 4. Quick start (once manifests exist)

```bash
# 1. Start Minikube with enough resources for Kafka
minikube start --cpus=4 --memory=8192 --driver=docker

# 2. Build images inside the Minikube Docker daemon
eval $(minikube docker-env)
docker build -t fx-oee-backend:dev -f Dockerfile.backend .
docker build -t fx-oee-frontend:dev frontend/

# 3. Apply manifests
kubectl apply -f k8s/namespace.yaml
kubectl apply -R -f k8s/

# 4. Wait for everything to be ready
kubectl -n fx-oee get pods -w

# 5. Open the app
minikube tunnel        # in a separate terminal
open http://fx-oee.local
```

---

## 5. What this does NOT cover (yet)

- **Helm chart** — manifests above are raw YAML. A Helm chart would parameterize
  image tags, replica counts, and resource limits.
- **CI/CD** — no GitHub Actions workflow to build images and apply manifests.
- **Production cluster** — storage class, ingress controller, and secret
  management (e.g. External Secrets / Vault) differ on a real cluster.
- **Network policies** — currently every pod can talk to every other pod. A
  production setup should restrict this.
- **mTLS / FIX session security** — out of scope for this doc; needs cert-manager
  or sidecar-managed certificates.

---

## 6. References

- Minikube docs: https://minikube.sigs.k8s.io/docs/
- Kubernetes StatefulSet: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/
- Strimzi (production-grade Kafka on k8s): https://strimzi.io/
- Spring Boot on Kubernetes: https://spring.io/guides/topicals/spring-on-kubernetes/
