# Clean Architecture: engine → Kafka → DB at 750k+ orders/sec

> **Handoff doc.** Roadmap to make the persistence path keep up with the LMAX speed engine.
> A fresh agent should read the **Status / handoff** section first, then execute from **Phase 1**.

---

## Status / handoff (as of 2026-06-14)

**Already committed** (`feat/lmax-disruptor`, commit `f1209d8`):
- Tier-3 alloc: `PendingFill` → interface; `MaterializedFill` (default engine); `SpeedFill` raw-long
  carrier defers event/JSON materialization to `PersistenceWorker.materialize()` (memoized → stable
  event ids). `TradeExecuted` serialized once (DB row + Kafka reuse). Producer batch/lz4/linger,
  consumer fetch tuning. ~26% request-thread alloc cut. All tests + Kafka E2E green.

**Uncommitted local-dev + Phase-0 config edits this session (NOT committed):**
- `application.yml`: kafka bootstrap default → `localhost:9093`; producer now `batch-size 262144`,
  `compression zstd`, `buffer-memory 64MB`, `linger 20`, `max.in.flight 5`, `delivery.timeout 30s`
  (Phase 0).
- `KafkaTopicConfig.java`: consumer `MAX_POLL_RECORDS 2000`, `FETCH_MIN_BYTES 64KB`,
  `FETCH_MAX_WAIT_MS 10` (Phase 0).
- `DefaultFillQueue.java` / `DisruptorFillQueue.java`: `fxoee.queue.high-water` now configurable.
- `application-local.yml` (new): `local` Spring profile (speed engine + kafka + disruptor queue +
  high-water 100k + localhost:9093/5432) for IntelliJ runs.
- `docker-compose.yml`: kafka now advertises an EXTERNAL listener `localhost:9093` (host BE bypasses
  the kubectl port-forward).
- `scripts/deploy-all.sh` (kafka forward 9092→9093), `scripts/dev-local-backend.sh` (queue env vars).

**`PersistenceWorker.java` is UNCHANGED** — the Phase-1 rewrite below was drafted but not applied.
**Next step = Phase 1.**

### Diagnosis that motivated this (live metrics, real running instance)
382s run, minikube + `kubectl port-forward`: **28.55M orders submitted, 28.45M rejected `OVERLOADED`
(99.6%)**, ~270 orders/sec persisted, ~1,100 Kafka records/sec. Kafka **0 errors**, Postgres pool
**idle** (active=0). Conclusion: the engine is fine; the **durable drain is the wall**, and the
`kubectl port-forward` proxy caps it to ~1k/s (it's an API-server tunnel, not a data pipe). Even on
fast infra the design caps at ~100k/s (single worker + per-batch `acks=all` join). Test high-throughput
over **local docker** (`docker-compose up zookeeper kafka postgres`, EXTERNAL listener already added)
or in-cluster — never over port-forward.

---

## Context / goal

The LMAX speed engine matches at ~660k (crossing) – 1.5M (resting) orders/sec; the persistence tail
caps the system far below that and sheds inside the order path. **Goal:** clean event-sourcing
architecture where ingestion tracks the engine (750k+) and Postgres is an async, lagging, append-only
projection — without breaking correctness.

**Decisions (confirmed with user):**
- Durable boundary = **Kafka-as-WAL** (Postgres leaves the hot path; replay from Kafka + snapshots).
- **Full phased roadmap 0→4.**
- **Re-key the bus by account** — no consumer needs strict per-pair ordering (engine owns the book /
  circuit-breaker / ordering in-process; per-pair trade-tape for UI is reconstructed by timestamp).

## Invariants that MUST hold (every phase)
1. **Engine authoritative.** DB/Kafka are projections; engine rebuilt only from the durable log
   (+ resting orders), never from a stale DB.
2. **Append-only log = single source of truth**; `event_id` **stable & immutable** (already guaranteed
   by `SpeedFill.materialize()` memoization).
3. **Conservation:** `cash == deposits + Σ realized PnL`. Per-account cash applied **in order**;
   parallelism allowed **across** accounts, ordered **within** an account. A `TradeExecuted` touches
   TWO accounts (buy + sell).
4. **Dedup idempotency** on `tradeId:side` / `tradeId:FEE`.
5. **Warm-restart replay** reproduces engine state exactly.

## Target end-state architecture

```
REST / SIMULATE / FIX
        │  submit()                         (authoritative, in-memory, 660k–1.5M/s)
        ▼
  LMAX Speed Engine (single-writer Disruptor)
        │  SpeedFill (raw-long carrier, already account-aware)
        ▼
  Publisher (pipelined; later account-sharded pool)            ← Phase 1 (+1b)
        │  pipelined async sends (idempotent, acks=all, zstd, bounded buffer)
        │  per-ACCOUNT fill-leg records, key = accountId        ← Phase 2
        ▼
  Kafka  trades.byaccount  — 32–64 partitions, THE durable WAL  ← Phase 4
        ├──► DB projector pool (1 consumer/partition, disjoint accounts)   ← Phase 2/3
        │      append-only COPY → account_transaction + lot_event
        │      dedup via UNIQUE + ON CONFLICT DO NOTHING (no global lock)
        │      no FOR UPDATE, no TreeMap (single writer per account)
        ├──► WS / read-model / audit consumers (lag-tolerant)
        └──► EngineSnapshotter → compacted engine.snapshots topic          ← Phase 4

  Restart = load latest snapshot + replay WAL tail from its offset (bounded).
```
Key reframe: **shedding moves out of the order path** — engine→Kafka is fast; the DB consumer lags and
catches up; `OVERLOADED` → ~0 under sustained load.

## Top bottlenecks (confirmed in code, ranked)
1. `PersistenceWorker` single thread + per-batch `acks=all` join (no pipelining) — PersistenceWorker.java:148-164
2. Topic = 7 partitions keyed by `pair` (skew + hard cap) — KafkaTopicConfig.java:49,115
3. `SELECT…FOR UPDATE` on `customer_account` + TreeMap per-account serialization — FillBatchRepository.java:96-108
4. Global synchronized dedup LRU — FillConsumer.java:82-88
5. Per-row INSERT/UPDATE (no COPY) — FillBatchRepository
6. DB `trade_events` write on producer hot path

---

## Phased roadmap

Each phase independently shippable; preserves all five invariants. Ship 1→3 first; Phase 4 last.

### Phase 0 — Config only (~150–200k/s; removes the port-forward cliff). **Mostly applied (uncommitted).**
- Producer (application.yml): `linger 20`, `batch-size 262144`, `compression zstd`, `buffer-memory 64MB`,
  `max.in.flight 5` (safe w/ idempotence). **Done.**
- Consumer (KafkaTopicConfig): `MAX_POLL_RECORDS 2000`, `FETCH_MIN_BYTES 64KB`. **Done.**
- Make topic partition count + `setConcurrency` property-driven (still pair-keyed, so distribution skew
  persists until Phase 2 — low value now; defer the partition bump to Phase 2's new topic). **TODO (optional).**
- Run over local docker / in-cluster, **not** `kubectl port-forward`. **Infra ready (compose EXTERNAL listener).**

### Phase 1 — Pipeline the producer (~400–600k/s). **NEXT. Code only, no schema.**
**Refinement vs the original plan:** do NOT account-shard the appender yet. With a single combined
`trade_events` log, sharding the appender would interleave the BIGSERIAL `seq` across shards and break
warm-restart replay ordering for accounts that span shards (a trade touches two accounts). Correct
account-sharding needs an **engine-stamped monotonic sequence** on each fill + replay `ORDER BY` that —
which is really Phase 2/4 territory. So Phase 1 = keep **one appender** (global `seq` order preserved —
DB append is NOT the bottleneck, the broker join is) and only **pipeline the Kafka publish**:

- In `PersistenceWorker.processBatch`, **remove the per-batch `CompletableFuture.allOf().get(15s)` join.**
  Fire each `sendTradeExecutedJsonAsync(...)` and attach `.whenComplete((r,ex) -> …)`:
  - success → `confirmed.offer(eventId)` (a `ConcurrentLinkedQueue<UUID>`)
  - failure → log + `fillQueue.enqueue(owner)` (re-enqueue the owning `PendingFill`; idempotent via
    `ON CONFLICT (event_id)` + consumer `tradeId:side` dedup)
- Keep `appendBatch` (log-first) + `applyRestingDeltas` synchronous on the single worker thread →
  global `seq` order preserved → **no replay/schema change**.
- Move `markPublished` OFF the per-batch path: a `flushPublished()` step in `runLoop` drains `confirmed`
  and marks them in batched UPDATEs (≤4096/flush). Lagging slightly is harmless (unmarked-but-delivered
  rows are re-published once on restart and deduped).
- Back-pressure now comes from the producer's bounded `buffer-memory`: when the broker falls behind,
  `send()` blocks → FillQueue backs up → engine sheds upstream (correct place). No semaphore needed.
- Capture `eventId`/`owner` as locals in the send loop (NOT the reusable lists, which are cleared next
  batch); `send()` consumes the json synchronously so reusing `payloads`/`owners` is safe.
- Files: `PersistenceWorker.java` (the drafted rewrite — see git stash / this doc), `OrderEventProducer.java`
  (already has `sendTradeExecutedJsonAsync` returning the future), `application.yml` (`buffer-memory` done).
- Invariants: global order preserved (single appender); `SpeedFill.cached` keeps event ids stable;
  crash safety unchanged (append before publish; relay + dedup cover unacked tail).
- Verify: kafka E2E (`BalanceDbE2ETest`, `MatchFillIntegrationTest`, `WarmRestartIntegrationTest`) =
  conservation + replay still hold; then load via SIMULATE over local docker, confirm `OVERLOADED` drops
  and consumer lag stays bounded.

> **Phase 1b (optional, only if DB append becomes the limit):** account-sharded appender pool. Requires
> an **engine-stamped monotonic seq** (the engine is single-writer → a plain counter on each enqueued
> fill), stored on `trade_events`, with replay `ORDER BY engine_seq`. Then shards append concurrently
> while replay stays globally correct.

### Phase 2 — Account-keyed topic + disjoint-account consumers. Kills bottlenecks #2/#3/#4.
- New topic `trades.byaccount`, **32–64 partitions, key = accountId**. Publish **one record per account
  leg** (buy→buyAccount, sell→sellAccount, fee→taker, house→house) so each leg lands on its owner's
  partition. Durable WAL row format otherwise unchanged.
- `FillConsumer`/`FillBatchRepository`: one consumer/partition ⇒ each account touched by exactly one
  thread ⇒ **drop `SELECT…FOR UPDATE` + TreeMap**; **drop the global synchronized dedup LRU** → DB
  UNIQUE + `INSERT … ON CONFLICT DO NOTHING` (reuse `processed_events` table if it fits `(trade_id, leg)`).
- Add `ConsumerRebalanceListener`: on partition assignment, re-seed each owned account's in-memory
  running `balance_after` from the DB tail.
- Files: `FillConsumer.java`, `FillBatchRepository.java`, `OrderEventProducer.java`, `KafkaTopicConfig.java`,
  one migration for the dedup UNIQUE.

### Phase 3 — Fully append-only projection (removes the last UPDATE hotspots).
- **Stop UPDATEing `customer_account.account_balance`** — balance is the running sum already in
  `account_transaction.balance_after` (single owner maintains it in memory, appends each row). Readers
  switch to latest-txn balance (add index `account_transaction(account_id, created_at desc)`) or the
  authoritative engine. **Audit every reader of `account_balance` first.**
- **`position_lot` close → append-only `lot_event` table** (open/partial/full-close rows); current lot
  state is a fold/view. (Or keep the lot UPDATE — uncontended now that each account is single-writer.)
- Writes become **COPY / multi-row INSERT** (jOOQ `loadInto`/COPY).
- Files: `FillBatchRepository.java`, `CustomerAccountRepository.java`, `PositionLotRepository.java`, migrations.

### Phase 4 — Kafka-as-WAL + engine snapshots (engine-rate ingestion, 750k+). Highest risk, last.
- Engine **publishes straight to the Kafka WAL; no synchronous `trade_events` INSERT / `markPublished`
  on the hot path.** Durability = Kafka replication. `trade_events` (if kept) becomes an async
  consumer-written projection. Shedding now guards only the fast engine→Kafka buffer → `OVERLOADED` ≈ 0.
- New **`EngineSnapshotter`**: periodic snapshot (balances + positions + resting orders) to a
  log-compacted `engine.snapshots` topic, tagged with the WAL offset it covers. WAL retention ≥ snapshot
  interval + margin.
- Rewrite `AccountBootstrapper.recoverFromLog`: load latest snapshot → consume WAL from that offset →
  bounded restart time (replaces `findAllPayloadsOrderBySeq`).
- Files: `PersistenceWorker.java` (publish-only), `AccountBootstrapper.java`, new `EngineSnapshotter` +
  snapshot consumer (a `SnapshotConsumer` already exists), `KafkaTopicConfig.java` (compacted topic + retention).

## Reused building blocks
- `SpeedFill` (`engine/speed/SpeedFill.java`) — carries `accountId`, memoizes materialization (stable
  event ids); the natural account-routing + per-leg seam.
- `account_transaction.balance_after` (V1) — append-only running balance already exists → Phase 3 is
  mostly *deleting* the redundant mutable column.
- `trade_events` append-only + `ON CONFLICT (event_id) DO NOTHING` — idempotency pattern to extend.
- `DisruptorFillQueue` — bounded MPSC ring for the engine→publisher buffer.
- `AccountBootstrapper.recoverFromLog` + `WarmRestartIntegrationTest` — recovery + test harness to evolve.

## Verification (end-to-end)
- **Per-phase regression (no Kafka/DB):** `SpeedEngineBench` (events mode) for orders/s;
  `SpeedSubmitAllocProbe` to confirm request-thread allocation didn't regress.
- **Sustained pipeline (over local docker, not port-forward):** drive SIMULATE above the old ceiling;
  measure engine accept (`orders.submitted.total`) vs `OVERLOADED` (target ≈ 0), producer publish rate,
  consumer apply rate + Kafka consumer-group lag. Sustained = accept ≈ apply, bounded non-growing lag;
  then stop input and confirm the consumer drains lag to zero.
- **Conservation gate:** after a high-rate run, per account `latest balance_after == 10M + Σ realized PnL`
  and globally `Σ cash == Σ deposits + Σ realized PnL`; assert `count(distinct trade_id, leg) ==
  count(FILL rows)` (no double-apply / no dropped legs).
- **Warm restart:** `fxoee.recovery.replay-on-startup=true`, restart, re-run conservation asserts.
  Phase 4: kill mid-run, confirm restart replays only the tail since the last snapshot.

## Open risks
- Phase 2 rebalance: account→partition ownership moves → re-seed in-memory `balance_after` from DB on
  assignment (the `ConsumerRebalanceListener`).
- Phase 4 retention sizing: WAL retention must always cover the tail since the last snapshot.
- Phase 3 balance-reader audit: move every reader of `customer_account.account_balance` to the
  derived/engine source before removing the column's UPDATE.

## Measurement helper (live drain/shed rate)
Sample actuator over a window while SIMULATE runs (BE on :8080):
```
python3 - <<'PY'
import urllib.request, time, re
U="http://localhost:8080/actuator/prometheus"
def s():
    t=urllib.request.urlopen(U,timeout=5).read().decode()
    def v(p): return sum(float(l.rsplit(' ',1)[1]) for l in t.splitlines() if not l.startswith('#') and re.match(p,l))
    return {'kafka':v(r'kafka_producer_record_send_total\{'),'sub':v(r'orders_submitted_total(\{| )'),
            'over':v(r'orders_rejected_total\{reason="OVERLOADED"\}')}
a=s(); t0=time.time(); time.sleep(10); b=s(); dt=time.time()-t0
for k in a: print(f"{k:6s} {(b[k]-a[k])/dt:>12,.0f}/s")
PY
```
