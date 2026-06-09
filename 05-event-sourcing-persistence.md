# 05 - Event sourcing & persistence

_Last updated: 2026-06-09 BST._

The engine is authoritative and in-memory. Durability and the read-model come from an **append-only
event log** plus Kafka projections. This doc traces a fill from the matching thread to PostgreSQL and
back through a warm restart.

All of this is gated on `kafka.enabled=true`. With it off, `OrderEventProducer`, `FillQueue`, and
`PersistenceWorker` beans don't exist and the engine runs as a pure in-memory system.

## The hot path doesn't block on Kafka

When the engine finishes a fill it has the authoritative effects in hand. Instead of publishing to
Kafka inline (a network round-trip on the matching thread), it packages them into a
[PendingFill](../src/main/java/com/fxoee/engine/PendingFill.java) and offers it to the
[FillQueue](../src/main/java/com/fxoee/engine/FillQueue.java) in ~1µs.

```mermaid
flowchart LR
    MS["MatchingService.submit<br/>(matching thread)"] -->|"enqueue PendingFill (~1µs)"| FQ["FillQueue<br/>(ConcurrentLinkedQueue)"]
    FQ -->|"drain ≤512"| PW["PersistenceWorker<br/>(single thread)"]
    PW -->|"1. append"| LOG[("trade_events<br/>append-only")]
    PW -->|"2. publish"| K{{"Kafka:<br/>trades.executed<br/>orders.matched"}}
    PW -->|"3. mark published"| LOG
```

### Backpressure / load shedding

`FillQueue` is unbounded, but `isOverloaded()` returns true at the `HIGH_WATER` mark (50,000 pending
fills). `MatchingService.submit` checks this **before mutating any engine state** and, if the worker
is falling behind, rejects the order with reason `OVERLOADED`: no book lock, no fill, no reservation.
This bounds heap instead of growing the queue until OOM. Worker re-enqueues (after a failed batch) go
through `enqueue` directly and are exempt from shedding.

### PersistenceWorker ordering guarantee

The worker ([PersistenceWorker.java](../src/main/java/com/fxoee/engine/PersistenceWorker.java)) drains
batches of up to 512 and, per item: **(1)** appends each `TradeExecuted` to `trade_events`, **(2)**
publishes to Kafka, **(3)** marks the row published, **(4)** then publishes the terminal
`OrderMatched`. Append happens **before** publish, so the log is the single committed source both
projections derive from. Failed batches are re-enqueued, never dropped.

## The durable log: `trade_events`

[V9__create_trade_events.sql](../src/main/resources/db/migration/V9__create_trade_events.sql):

```sql
CREATE TABLE trade_events (
    seq         BIGSERIAL  PRIMARY KEY,   -- replay order
    event_id    UUID       NOT NULL UNIQUE,
    pair        VARCHAR(10) NOT NULL,
    payload     JSONB      NOT NULL,      -- the serialized TradeExecuted
    published   BOOLEAN    NOT NULL DEFAULT FALSE,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

This is the crux of the no-divergence guarantee:

```mermaid
flowchart TD
    crash{"crash happens..."} --> before["BEFORE the INSERT"]
    crash --> after["AFTER the INSERT"]
    before --> b2["order was never durably committed<br/>→ engine AND db agree it never happened"]
    after --> a2["recovery relay re-publishes rows<br/>where published = FALSE<br/>→ consumer dedup makes replay idempotent"]
```

Both the **engine** (rebuilt by replaying the log) and the **DB projection** (written by `FillConsumer`
off the Kafka stream the log feeds) derive from the same committed rows, so they cannot diverge.

## Kafka topics

[KafkaTopicConfig](../src/main/java/com/fxoee/config/KafkaTopicConfig.java) declares six topics.
Partition count = number of currency pairs (7); messages are keyed by `pair.name()` so all events for
a pair land on one partition (per-pair ordering).

| Topic | Event | Producer | Consumer |
|-------|-------|----------|----------|
| `orders.placed` | `OrderPlaced` | submit (audit) | `OrderAuditConsumer` (writes the `orders` table) |
| `trades.executed` | `TradeExecuted` | PersistenceWorker | `FillConsumer` |
| `orders.matched` | `OrderMatched` | PersistenceWorker | `SnapshotConsumer` + `OrderAuditConsumer` (terminal status) |
| `fills.applied` | `FillApplied` | `FillConsumer` | (downstream / WS) |
| `account.snapshotted` | `AccountSnapshotted` | `SnapshotConsumer`, reset/forceFlat | (downstream / WS) |
| `trading.halted` | none | declared only; nothing produces or consumes it yet | none |

## Projections

```mermaid
flowchart TB
    TE["TradeExecuted<br/>(engine-stamped cash + lot deltas)"] --> FC["FillConsumer<br/>(7 threads, batched)"]
    FC -->|"apply verbatim, no re-derivation"| DB[("PostgreSQL:<br/>customer_account.balance<br/>position_lot")]
    FC --> ASm["AccountState<br/>(in-memory mirror)"]
    FC --> FA["FillApplied + TradeEvent"]

    OM["OrderMatched"] --> SC["SnapshotConsumer"]
    SC --> REG["OrderRegistry<br/>(unregister terminal orders)"]
    SC --> SNAP["AccountSnapshotted +<br/>AccountSnapshotEvent (WS)"]
```

### FillConsumer

[FillConsumer](../src/main/java/com/fxoee/events/kafka/FillConsumer.java) applies the engine-stamped
cash and lot effects to the DB and the `AccountState` mirror **without re-deriving** any open/close or
cash math. It replays exactly what the engine decided, keyed on engine lot ids. It batches DB writes
via `FillBatchRepository` and rolls back the in-memory batch on a DB failure. **Dedup** is in-memory,
keyed `tradeId:side` and `tradeId:FEE`, so a Kafka redelivery is idempotent.

### SnapshotConsumer

[SnapshotConsumer](../src/main/java/com/fxoee/events/kafka/SnapshotConsumer.java) consumes
`OrderMatched`: unregisters terminal orders from `OrderRegistry`, builds an account snapshot (throttled
to one per second), and publishes `AccountSnapshotted` + a Spring `AccountSnapshotEvent` for the
WebSocket layer. Snapshots may briefly lag fills (separate topic, no cross-topic ordering). Dedup by
`eventId`.

## Warm-restart recovery (engine replay)

On restart the books are empty and in-memory state is gone. The engine is rebuilt **1:1** from two
durable sources: `trade_events` (positions + cash) and `resting_orders` (the live order books).

```mermaid
sequenceDiagram
    participant R as Recovery
    participant MS as MatchingService
    participant Books as OrderBooks
    participant Log as trade_events
    participant Rest as resting_orders
    R->>MS: seedForReplay(acct, cash)  // per account: clear positions, seed cash
    R->>Log: read log in seq order
    loop each TradeExecuted
        R->>MS: replayFill(te)  // pure state replay: applyFill both sides, credit P&L, re-apply taker fee
    end
    R->>Rest: findAllOrderByPlacedAt() (oldest-first)
    loop each resting order
        R->>Books: addOrder(rebuilt)  // builder + Order.fill(filled) → exact remaining + priority
    end
    R->>MS: reconcileReserved(acct)  // per touched account: reserved = positions + resting (reads books)
```

`replayFill` does **no** validate/reserve/match. The trade already happened and is durably logged, so
it just applies each non-null side's fill to the `PositionBook`, credits realized P&L, and re-applies
the taker fee to taker/house. The resting orders are then rebuilt onto the books, so
`reconcileReserved` (which reads resting margin **from the books**) re-locks `reserved == Σ position
margin + Σ resting margin`. This round-trip is tested in `MatchingServiceCornerCasesTest.replayRoundTrip`
(positions/cash) and `…warmRestartRecoversRestingOrders` (resting orders).

#### Resting (open, unfilled) orders are recovered 1:1

`trade_events` records **fills only**, so it cannot rebuild a resting LIMIT order that never matched.
The dedicated **`resting_orders`** table is the authoritative mirror of the live books: a row exists
iff the order is currently resting. It is maintained incrementally by `PersistenceWorker` (the same
off-hot-path worker that persists fills), so it has the same durability as `trade_events` and adds
**no** cost to the matching/HTTP threads.

```mermaid
flowchart LR
    A["order rests<br/>(LIMIT, leftover qty)"] -->|upsert| T[("resting_orders")]
    B["resting order partially fills"] -->|upsert (lower remaining)| T
    C["fully fills / cancelled"] -->|delete| T
    T -->|warm restart: findAllOrderByPlacedAt| D["rebuild Order + addOrder"]
    D --> E["reconcile → margin re-locked"]
```

`MatchingService.submit` captures the deltas under the book lock it already holds (`findOrder` per
touched order: present means upsert with current remaining, absent means delete), packages them into
the `PendingFill` already handed to the `FillQueue`, and the worker applies them in the same durable
step as the trade-events append. Cancels enqueue a delete. Net effect of a restart:

| Pre-crash state | After warm restart |
|-----------------|--------------------|
| Filled position (`trade_events`) | restored; margin re-locked |
| Resting LIMIT order (`resting_orders`) | **restored**: same id, price, remaining qty, time priority |
| `reserved` | restored to `Σ position margin + Σ resting margin` (reconcile reads the rebuilt books) |

Scope: **all** resting orders are restored, including house/sim (null-account) liquidity. A
partially-filled order is reconstructed exactly via `Order.fill(originalQty − remaining)`. Covered by
`MatchingServiceCornerCasesTest.warmRestartRecoversRestingOrders` (engine) and
`WarmRestartIntegrationTest` (submit → persist → restart → back on the book).

> The recovery **relay** (re-publishing `published = FALSE` rows after a crash) is indexed by
> `idx_trade_events_unpublished`. A `processed_events` table exists for durable consumer dedup; the
> consumers currently use in-memory dedup.

## Enabling warm restart

Warm restart is gated by a single flag, `fxoee.recovery.replay-on-startup` (env
`FXOEE_RECOVERY_REPLAY_ON_STARTUP`), **default `false`**. When `false`, `AccountBootstrapper` performs a
fresh start: wipe transactional state, reset every account to the 10 M seed. When `true`, it replays
`trade_events` into the engine instead (the sequence above).

| Environment | Setting | Effect |
|-------------|---------|--------|
| **k8s** ([configmap.yaml](../k8s/backend/configmap.yaml)) | `FXOEE_RECOVERY_REPLAY_ON_STARTUP: "true"` | every pod restart (crash, rolling update, OOM kill) preserves open positions + trade history |
| **docker-compose** (`environment:`) | `FXOEE_RECOVERY_REPLAY_ON_STARTUP=true` | same, for a long-lived compose stack |
| **local dev** (default) | _unset, so `false`_ | fresh start on every `mvn spring-boot:run` / `deploy-all.sh` (which also wipes the Postgres PVC, so there would be nothing to replay anyway) |

Why it is safe to default-on for k8s: replay is **idempotent**. The relay's re-publish is de-duplicated
downstream by `FillConsumer` on `event_id`, and an empty log makes warm restart a graceful no-op. So a
first-ever deploy onto an empty database behaves exactly like a fresh start.

Operational verification on a live cluster (minikube / k3s): see the runbook
[testing-event-sourcing-minikube.md](testing-event-sourcing-minikube.md).

## Test coverage

| Layer | Test | What it covers |
|-------|------|----------------|
| Engine unit | `MatchingServiceCornerCasesTest.replayRoundTrip` | seed → replay 2 fills → reconcile → assert positions / cash / margin (engine only, no DB) |
| Engine unit | `MatchingServiceCornerCasesTest.warmRestartRecoversRestingOrders` | filled position **and** resting LIMIT order both restored 1:1; `reserved == positions + resting` margin |
| Bootstrap unit | `AccountBootstrapperTest` (7, Mockito) | fresh vs warm boot branch, resting-order rebuild + reconcile, the Kafka relay of `published=false` rows, bad-JSON tolerance, fallback to fresh start when no `TradeEventRepository` bean |
| Bootstrap IT | `WarmRestartIntegrationTest` (5, Testcontainers + EmbeddedKafka) | DB-to-engine glue: `trade_events` → `recoverFromLog` → `MatchingService.snapshot`; relay re-publish; **resting-order recovery** (incl. a partially-filled row); `RestingOrderRepository` upsert/update/delete; end-to-end submit → `PersistenceWorker` persist → restart → back on the book |

See [doc 08 - Testing](08-testing.md#bootstrap--recovery-warm-restart-three-layers) for the suite map.
