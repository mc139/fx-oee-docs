# Useful Commands

Quick command list for local development.

## Saved commands

- `chmod +x orders.sh` — Adds execute permission to `orders.sh` (fixes "permission denied" when trying to run it).
- `kubectl apply -f k8s/backend/deployment.yaml` — Applies the backend Deployment manifest to the current Kubernetes context.
- `kubectl rollout restart statefulset/kafka -n fx-oee` — Performs rolling restart of Kafka StatefulSet in fx-oee namespace.
- `cd frontend && npm run build` — Rebuilds the React/Vite frontend bundle into `src/main/resources/static/` (required before rebuilding the backend image).

## Podman on macOS — Prerequisites

When running Minikube with Podman on macOS, two things must be in place or services won't resolve:

1. `/etc/hosts` must map `127.0.0.1` to your service hostnames.
2. `minikube tunnel` must be running in a separate terminal — this exposes LoadBalancer services to localhost.

## Minikube

- `minikube start --cpus=4 --memory=6144 --driver=podman` — Starts Minikube with 4 CPUs and 6 GB RAM. The full stack (Kafka + Zookeeper + Postgres + backend + Prometheus + Grafana + postgres-exporter) needs ~3.5–4 GB of requests; 6 GB leaves headroom. Below ~5 GB, `postgres-0` and other pods land in `Pending` with `Insufficient memory`, which cascades into backend `CrashLoopBackOff` (`UnknownHostException: postgres`).
- `minikube stop` — Stops the Minikube cluster (preserves state).
- `minikube image ls` — Lists all container images available inside Minikube's Docker daemon.

### Podman VM memory (the real cap)

With the **podman** driver, `minikube --memory` cannot exceed the podman machine's RAM, and you **cannot resize an existing minikube** — you must `minikube delete` and recreate. So size the podman VM first:

```bash
podman machine stop
podman machine set --memory 8192   # VM gets 8 GB so minikube can claim 6 GB + leave VM headroom
podman machine start
minikube delete
minikube start --cpus=4 --memory=6144 --driver=podman
```

Check the VM's current memory with `podman machine inspect | grep Memory`. This is host state, **not** in the repo — if you ever `podman machine rm`, redo the `set --memory` step.

## Deploy scripts

- `./scripts/bootstrap-cluster.sh` — **Run once on a fresh/recreated cluster.** Enables the `ingress` + `metrics-server` addons, applies every k8s manifest in dependency order (namespace → postgres/kafka/backend/observability/exporter → ingress, with an ingress-webhook retry), and waits for the Zookeeper/Kafka/Postgres StatefulSets to be Ready. Does **not** build the backend image — run `deploy-all.sh` after.
- `./scripts/deploy-all.sh [--wipe]` — Resets the DB (optionally wipes Kafka), rebuilds + rolls out the backend, wires observability (postgres-exporter + Prometheus scrape + Grafana with dashboard 9628 pre-loaded), and opens port-forwards (backend 8080, kafka-ui 9090, grafana 3000, prometheus 9091). Assumes the cluster is already bootstrapped.
- `./scripts/deploy-minikube.sh` — Backend-only redeploy: build image (includes Node stage that bundles the React app) → load into Minikube → restart Deployment → wait for rollout. Override via env vars: `APP_NAME`, `DEPLOYMENT_NAME`, `DOCKERFILE`, `BUILD_CONTEXT`, `TAG`, `NAMESPACE`.

## Cluster resource tracking (k9s / kubectl top)

- The `metrics-server` addon (enabled by `bootstrap-cluster.sh`) powers `kubectl top` and the CPU/MEM columns in **k9s**. Without it, k9s shows no usage and `kubectl top` errors.
- `kubectl top nodes` — node CPU/MEM usage and % of allocatable.
- `kubectl top pods -n fx-oee --sort-by=memory` — per-pod usage, heaviest first.
- In k9s: `:nodes` (node usage), `:pods` (per-pod), `:pulses` (cluster gauges). Node metrics take ~60 s after enabling the addon to populate.
- `minikube addons enable metrics-server` — enable manually if you skipped the bootstrap script.

## Kafka
- **All topics and lags for a specific group**:
  ```bash
  kubectl exec -it kafka-0 -n fx-oee -- kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe \
    --group fx-oee-group
  ```
- **Describe all consumer groups**:
  ```bash
  kubectl exec -it kafka-0 -n fx-oee -- kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe \
    --all-groups
  ```
- **Live watch during data seeding**:
  ```bash
  watch -n 2 'kubectl exec -it kafka-0 -n fx-oee -- kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe \
    --group fx-oee-group'
  ```
- **Reset offsets for a specific topic (backend must be scaled to 0)**:
  ```bash
  kubectl exec -it kafka-0 -n fx-oee -- kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group fx-oee-group \
    --topic order-events \
    --reset-offsets \
    --to-latest \
    --execute
  ```
- **Reset all topics at once**:
  ```bash
  kubectl exec -it kafka-0 -n fx-oee -- kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group fx-oee-group \
    --all-topics \
    --reset-offsets \
    --to-latest \
    --execute
  ```
- **List all topics**:
  ```bash
  kubectl exec -it kafka-0 -n fx-oee -- kafka-topics \
    --bootstrap-server localhost:9092 \
    --list
  ```
- **Details of a specific topic (partitions, replicas)**:
  ```bash
  kubectl exec -it kafka-0 -n fx-oee -- kafka-topics \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic orders.placed
  ```