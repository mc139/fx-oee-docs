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

- `minikube start --cpus=4 --memory=8192 --driver=docker` — Starts Minikube with 4 CPUs and 8 GB RAM (minimum recommended for Kafka + full stack).
- `minikube stop` — Stops the Minikube cluster (preserves state).
- `minikube image ls` — Lists all container images available inside Minikube's Docker daemon.

## Deploy scripts

- `./scripts/deploy-minikube.sh` — Full backend redeploy: build image (includes Node stage that bundles the React app) → load into Minikube → restart Deployment → wait for rollout. Override via env vars: `APP_NAME`, `DEPLOYMENT_NAME`, `DOCKERFILE`, `BUILD_CONTEXT`, `TAG`, `NAMESPACE`.