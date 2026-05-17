# Useful Commands

Quick command list for local development.

## Saved commands

- `lsof -nP -iTCP:5173 -sTCP:LISTEN` — Shows which process is listening on frontend port `5173` on macOS.
- `kill -9 $(lsof -tiTCP:5173 -sTCP:LISTEN)` — Hard-kills the process currently listening on frontend port `5173`.
- `chmod +x orders.sh` — Adds execute permission to `orders.sh` (fixes "permission denied" when trying to run it).
- `kubectl apply -f k8s/backend/deployment.yaml` — Applies the backend Deployment manifest to the current Kubernetes context.
- `kubectl rollout restart statefulset/kafka -n fx-oee` — Performs rolling restart of Kafka StatefulSet in fx-oee namespace.
- `kubectl apply -f k8s/frontend/deployment.yaml` — Applies the frontend Deployment manifest to the current Kubernetes context.

## Minikube

- `minikube start --cpus=4 --memory=8192 --driver=docker` — Starts Minikube with 4 CPUs and 8 GB RAM (minimum recommended for Kafka + full stack).
- `minikube stop` — Stops the Minikube cluster (preserves state).
- `minikube image ls` — Lists all container images available inside Minikube's Docker daemon.
- `podman build -t localhost/fx-oee-frontend:dev -f frontend/Dockerfile frontend && podman save localhost/fx-oee-frontend:dev | minikube image load -` — Builds frontend image with Podman and loads it directly into Minikube (use when Minikube runs with Docker driver but local build toolchain is Podman).

## Deploy scripts

- `./scripts/deploy-frontend.sh` — Full frontend redeploy: build image → load into Minikube → restart Deployment → wait for rollout. Use after any frontend/Vite config change.
- `./scripts/deploy-minikube.sh` — Same pipeline for backend (defaults). Override via env vars: `APP_NAME`, `DEPLOYMENT_NAME`, `DOCKERFILE`, `BUILD_CONTEXT`, `TAG`, `NAMESPACE`.