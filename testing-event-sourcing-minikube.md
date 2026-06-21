# Runbook: verifying warm-restart event sourcing on minikube / k3s

_Last updated: 2026-06-20 BST._

_How to prove, on a live cluster, that killing the backend pod **does not lose trading state**:
the engine is rebuilt from a durable log on restart._

This is the operational counterpart to the automated coverage in
[08-testing.md](08-testing.md#bootstrap--recovery-warm-restart-three-layers); the mechanism itself is
described in [05-event-sourcing-persistence.md](05-event-sourcing-persistence.md#warm-restart-recovery-engine-replay).

> **Which lane is deployed?** As of this writing the in-cluster config
> ([k8s/base/backend/configmap.yaml](../k8s/base/backend/configmap.yaml)) defaults to the **speed
> engine + Aeron WAL + QuestDB lane** (`FXOEE_ENGINE_MODE=speed`, `KAFKA_ENABLED=false`,
> `FXOEE_WAL_*_ENABLED=true`), with **`FXOEE_RECOVERY_REPLAY_ON_STARTUP=false`** and no persisted
> Archive volume, so each pod boot is a **fresh start**, not a warm restart. Sections 2-6 below
> verify the **Lane 1** (default engine, `trade_events` Kafka event-sourcing) warm restart and require
> flipping the config back to that lane first (see [Section 2](#section-2-verify-warm-restart-is-enabled)).
> The speed-engine WAL durable-restart story is in
> [Section 7](#section-7-lane-2-speed-engine--aeron-wal-durable-restart).

## What you are verifying

```mermaid
sequenceDiagram
    autonumber
    participant U as You (curl)
    participant B as backend pod
    participant E as engine (in-JVM)
    participant L as trade_events (Postgres)
    U->>B: POST /api/orders (open a position)
    B->>E: match + apply fill
    B->>L: append TradeExecuted (durable, then published=true)
    U->>B: GET /api/engine/accounts/{id}  (record cash + positions)
    Note over B: kubectl delete pod (crash / rolling update)
    B-->>B: pod restarts, engine memory is empty
    B->>L: recoverFromLog (read log in seq order)
    L-->>E: seedForReplay + replayFill + reconcileReserved
    U->>B: GET /api/engine/accounts/{id}  (identical cash + positions)
```

---

## Section 1: Prerequisites

- A running **minikube** cluster (local) **or** SSH/kubeconfig access to the **k3s VPS** (prod).
- `kubectl` pointed at the cluster, plus `curl` and `jq`.
- The app deployed in namespace **`fx-oee`** (deployment `backend`, StatefulSet `postgres` → pod
  `postgres-0`).
- Local access to the API on `localhost:8080`. `scripts/deploy-all.sh` already starts
  `kubectl port-forward -n fx-oee svc/backend 8080:8080`. If that process died, re-run it:
  ```bash
  kubectl port-forward -n fx-oee svc/backend 8080:8080 &
  ```

> ⚠️ **Do not run `scripts/deploy-all.sh` mid-test.** It deletes the Postgres PVC
> (`postgres-data-postgres-0`) on every run: a clean slate, which is the opposite of what we are
> testing. To simulate a crash use `kubectl delete pod` (below); that preserves the PVC and the log.

## Section 2: Verify warm restart is enabled

Sections 2-6 cover **Lane 1** (default engine + `trade_events` Kafka event-sourcing). The shipped
configmap now defaults to the **speed + WAL lane**, so first switch the relevant keys in
[k8s/base/backend/configmap.yaml](../k8s/base/backend/configmap.yaml):

```yaml
FXOEE_ENGINE_MODE: "default"               # lock-based engine (was: speed)
KAFKA_ENABLED: "true"                       # producer + consumer for the event-sourcing lane
FXOEE_RECOVERY_REPLAY_ON_STARTUP: "true"   # replay trade_events on boot (was: false)
FXOEE_WAL_AERON_ENABLED: "false"           # turn the Aeron WAL lane off
FXOEE_WAL_QUESTDB_ENABLED: "false"
FXOEE_WAL_POSTGRES_ENABLED: "false"
```

Re-apply (`kubectl apply -k k8s/overlays/local` or `.../prod`) and roll the deployment, then confirm:

```bash
kubectl get configmap backend-config -n fx-oee \
  -o jsonpath='{.data.FXOEE_RECOVERY_REPLAY_ON_STARTUP}'; echo
# Expected: true
```

If this prints empty or `false`, the pod will fresh-start (wipe to 10 M) instead of replaying.

## Section 3: End-to-end scenario

`TRADER_01` is the seeded `trader/trader` account, id
`00000000-0000-0000-0000-000000000001`, starting balance 10 M USD.

### Step 1: Log in

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"trader","password":"trader"}' | jq -r .token)
ACC=00000000-0000-0000-0000-000000000001
```

### Step 2: Open a position

Submit a LIMIT BUY; the `MockMarketMaker` (`MOCK_MARKET_ENABLED=true`) provides resting liquidity, so
it fills:

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"pair":"GBP_USD","side":"BUY","type":"LIMIT","price":"1.2700","quantity":"1000"}' | jq .
```

### Step 3: Record engine state BEFORE the restart

```bash
curl -s http://localhost:8080/api/engine/accounts/$ACC | jq '{cash, reserved, positions}'
# Save this output - it is what must survive the restart.
```

`/api/engine/accounts/{id}` reads the **in-JVM engine** (`cash`, `reserved`, open `positions`): the
exact state warm restart has to reconstruct.

> ℹ️ **Resting orders are recovered 1:1.** Warm restart rebuilds both positions/cash (`trade_events`)
> **and** the live order books (`resting_orders`). An open, unfilled LIMIT order placed before the
> restart comes back with the same id, price, remaining quantity, and time priority, and its margin is
> re-locked, so `reserved` after the restart equals `positions + resting` margin, exactly as before.
> To see it directly, place a **non-crossing** LIMIT (one that rests) in Step 2 and verify it is still
> on the book after Step 6.

### Step 4: Kill the pod (simulate crash / rolling update)

```bash
kubectl delete pod -n fx-oee -l app=backend
kubectl rollout status deployment/backend -n fx-oee --timeout=5m
```

The Deployment controller starts a fresh pod with an empty engine. The PVC (and the `trade_events`
log) are untouched.

### Step 5: Watch the warm-restart logs

```bash
kubectl logs -n fx-oee -l app=backend | grep -iE "bootstrap|warm|replay|relayed"
# Expected:
#   Bootstrap: warm restart - replaying trade_events into the engine
#   Bootstrap: warm restart complete - replayed N trades, restored M resting orders across K accounts
```

If you instead see `Bootstrap: Initializing accounts with 10M balance (fresh start)`, the flag from
Section 2 is off.

### Step 6: Verify state AFTER the restart

```bash
# port-forward may have dropped with the old pod - restart it if needed:
kubectl port-forward -n fx-oee svc/backend 8080:8080 >/dev/null 2>&1 &
sleep 2
curl -s http://localhost:8080/api/engine/accounts/$ACC | jq '{cash, reserved, positions}'
# Expected: identical cash + positions to Step 3.
```

## Section 4: Verify DB ↔ engine consistency

In a stable state every log row should be published (the relay has nothing to do):

```bash
kubectl exec -n fx-oee postgres-0 -- \
  psql -U fxoee -d fxoee -c \
  "SELECT count(*) AS total, count(*) FILTER (WHERE published) AS published FROM trade_events;"
# Expected: total == published
```

## Section 5: Test the recovery relay (crash between DB append and Kafka publish)

Simulate the narrow window where a trade was durably logged but the Kafka confirm never landed, by
flipping the newest row back to `published=false`:

```bash
kubectl exec -n fx-oee postgres-0 -- \
  psql -U fxoee -d fxoee -c \
  "UPDATE trade_events SET published=false WHERE seq=(SELECT max(seq) FROM trade_events);"

kubectl delete pod -n fx-oee -l app=backend
kubectl rollout status deployment/backend -n fx-oee --timeout=5m

kubectl logs -n fx-oee -l app=backend | grep "relayed"
# Expected: Bootstrap: relayed 1 unpublished trade_events to Kafka
```

`FillConsumer` de-duplicates the replayed event by `event_id`, so the projection self-heals without
double-counting.

## Section 5b: Verify resting orders survived 1:1

Before the restart, place a **non-crossing** LIMIT (well away from the market so it rests instead of
filling), then confirm it returns after the pod is killed:

```bash
# place a resting order (far-from-market BUY) and note its id
curl -s -X POST http://localhost:8080/api/orders \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"pair":"GBP_USD","side":"BUY","type":"LIMIT","price":"1.0000","quantity":"1000"}' | jq '{id, status}'

# it should be durably mirrored
kubectl exec -n fx-oee postgres-0 -- psql -U fxoee -d fxoee -c \
  "SELECT id, remaining_quantity FROM resting_orders WHERE account_id='$ACC';"

kubectl delete pod -n fx-oee -l app=backend
kubectl rollout status deployment/backend -n fx-oee --timeout=5m
kubectl logs -n fx-oee -l app=backend | grep "restored"
# Expected: Bootstrap: warm restart complete - replayed N trades, restored M resting orders across K accounts

# after restart the order is back on the book with the same remaining quantity
curl -s http://localhost:8080/api/orderbook/GBP_USD | jq '.bids'
```

## Section 6: On the k3s VPS (production)

Same scenario; the API base is `https://fxoee.mcieslik.me` (or port-forward as above). Every push to
`master` triggers a CI rolling restart, so warm restart fires on **every deploy**. Confirm it landed:

```bash
kubectl logs -n fx-oee -l app=backend | grep "warm restart complete"
```

A first-ever deploy onto an empty database logs `replayed 0 trades` and behaves like a fresh start:
warm restart on an empty log is a graceful no-op.

## Section 7: Lane 2 (speed engine + Aeron WAL) durable restart

This is the lane the cluster **actually ships** (`FXOEE_ENGINE_MODE=speed`, `KAFKA_ENABLED=false`,
`FXOEE_WAL_*_ENABLED=true`). Here durability is the **embedded Aeron Archive**, not `trade_events`,
and the two downstream projectors keep Postgres and QuestDB in step (ADR 0007). The mechanism and its
automated coverage are in [08-testing.md](08-testing.md#speed-engine-and-aeron-wal-coverage); the
`SpeedWalCrossRestartRecoveryTest` is the real-kill proof.

### 7a: Confirm the WAL lane is live and projecting

```bash
# account state served by the WAL → Postgres projector (WalDbProjector), not the frozen deposit
curl -s http://localhost:8080/api/debug/state | jq '.accounts[0]'

# WAL pipeline telemetry: projected fill-seq + lag (bytes) for the Postgres and QuestDB projectors
curl -s http://localhost:8080/api/debug/pipeline-stats | jq '{postgres, questdb}'

# the SQL-queryable trade tape in QuestDB (console at http://questdb.fx-oee.local, or PG wire :8812)
kubectl port-forward -n fx-oee svc/questdb 8812:8812 >/dev/null 2>&1 &
psql 'host=localhost port=8812 user=admin password=quest dbname=qdb' \
  -c "SELECT count(*) FROM trades;"
```

`lagBytes` is `recordingPosition − projectedPosition`: it should hover near zero and drain to zero
when trading stops. A persistently growing lag means a projector has stalled (the stall reason
surfaces as `pipeline-stats.postgres.error`, with `idleMs` since the last clean catch-up).

### 7b: Restart behaviour as shipped (fresh start)

The in-cluster configmap sets neither `FXOEE_WAL_PERSIST_ARCHIVE` nor `FXOEE_WAL_SNAPSHOT_ENABLED` and
mounts **no persistent Archive volume**, so the Aeron dirs live in the pod's ephemeral storage. Killing
the pod therefore **wipes the WAL and starts fresh** (by design for a dev cluster):

```bash
kubectl delete pod -n fx-oee -l app=backend
kubectl rollout status deployment/backend -n fx-oee --timeout=5m
curl -s http://localhost:8080/api/debug/state | jq '.accounts[0]'   # back to the seeded balance
```

This is the expected behaviour for the shipped config, not a bug: the Aeron lane only survives a
restart when it is told to persist.

### 7c: Make it a true durable restart (no DB)

For a warm restart over the WAL you need a **stable, persisted** Archive plus engine snapshots. Set on
the backend (configmap or `dev-local-backend.sh --wal-durable` locally):

```yaml
FXOEE_WAL_PERSIST_ARCHIVE: "true"     # keep the Archive recordings across boots
FXOEE_WAL_SNAPSHOT_ENABLED: "true"    # periodic engine snapshots (bounded restart, Phase E)
FXOEE_WAL_AERON_DIR:   "/data/wal/aeron"      # on a PVC, not ephemeral
FXOEE_WAL_ARCHIVE_DIR: "/data/wal/archive"    # on the same PVC
FXOEE_WAL_SNAPSHOT_FILE: "/data/wal/engine.snapshot.json"
```

On reboot `Snapshotter.recover()` restores the latest snapshot and replays only the Archive tail past
it (`SpeedEngineConfig.walRecoverOnBoot`), so state comes back with no database in the loop. Locally
this is one command:

```bash
./scripts/dev-local-backend.sh --wal-durable   # persisted Archive + snapshots under ./.dev-local/wal
```

> **Caveat (in-cluster).** The shipped manifests do not yet attach a PVC for the Aeron dirs, so 7c
> needs a volume added to [the backend deployment](../k8s/base/backend/deployment.yaml) before it
> survives a pod reschedule. The automated `SpeedWalCrossRestartRecoveryTest` proves the mechanism
> end-to-end against persisted dirs today.
