# fx-oee — Performance & Bottleneck Analysis

_Branch: `feature/async-fill-queue` · Date: 2026-06-02 · Author: analysis pass_

## TL;DR — where the limit is

The async `FillQueue` (this branch) did its job: Kafka is off the matching hot path, so a
**burst** of orders is now absorbed in heap instead of blocking the HTTP thread. But that moved
the ceiling, it did not remove it. Two hard limits remain, in priority order:

1. **Sustained throughput is capped by `PersistenceWorker`, not by matching.** It is a *single*
   thread that, per batch, sends every `TradeExecuted` to Kafka **synchronously and sequentially**
   (`.get(5s)` per send, in a `for` loop). Drain rate ≈ `1 / kafka_round_trip`. At ~2 ms RTT that
   is **~500 trades/s sustained**; at ~10 ms cloud RTT, **~100/s**. Above this the unbounded
   `FillQueue` grows without bound → **eventual OOM** (the `TODO(backpressure)` is the live risk).

2. **Matching itself is effectively single-threaded across all 7 pairs** because `PositionBook`
   and every `MarginLedger` account method are `synchronized`. The per-pair `OrderBook` lock gives
   the *illusion* of 7-way parallelism, but every fill funnels through one `PositionBook` monitor.
   This is the *burst*/in-memory ceiling: ~thousands–low-tens-of-thousands orders/s, well above
   limit #1, so you hit Kafka first.

**Net: the application can ingest bursts fast, but can only _retire_ them at a few hundred
trades/s. Run sustained load above that and it falls over on memory, not CPU.**

---

## The request path (what one order touches)

`POST /api/engine/orders` → `MatchingService.submit(order)` (Tomcat worker thread):

| Step | Code | Cost | Lock held |
|------|------|------|-----------|
| structural validate | `validator.checkStructural` | µs | none |
| reserve funds | `reserve()` → `MarginLedger` (`synchronized` per account) | µs | **book lock** |
| match | `engine.match()` over `OrderBook` (TreeMap) | µs–ms (depth-dependent) | **book lock** |
| apply fills | `applyFills` → `PositionBook.applyFill` (**`synchronized` whole object**) + `ledger.credit` | µs/trade | **book lock + PositionBook monitor** |
| reconcile | `reconcile()` iterates **all 7 books'** resting orders for the account | **O(pairs × resting orders)** | **book lock + PositionBook monitor** |
| hand-off | `fillQueue.enqueue` | ~1 µs | book lock |
| Kafka sends | done **after** lock release | 0 on hot path (queued) | none |

Good: all Kafka I/O is outside the book lock, and the fill is queued not sent. The hot path is
pure CPU + lock contention.

---

## Bottleneck #1 — PersistenceWorker is a single-threaded synchronous Kafka pump (PRIMARY)

`PersistenceWorker.processBatch` ([PersistenceWorker.java:117](../../src/main/java/com/fxoee/engine/PersistenceWorker.java#L117)):

```java
for (TradeExecuted te : toPublish) producer.sendTradeExecuted(te);  // sendSync → .get(5s) EACH
```

- One dedicated thread (`new Thread(... "persistence-worker")`).
- `sendTradeExecuted` → `sendSync` → `kafkaTemplate.send(...).get(5, SECONDS)` —
  **blocks per event** ([OrderEventProducer.java:80](../../src/main/java/com/fxoee/events/kafka/OrderEventProducer.java#L80)).
- Batch is up to 512, but the sends are a **sequential loop of blocking round-trips**, not
  pipelined. Effective drain = `batch_size / (batch_size × RTT)` = `1 / RTT`.

**Numbers:** local broker RTT ~1–3 ms → **~300–1000 trades/s**. Cloud/replicated `acks=all`
~10–50 ms → **~20–100 trades/s**. The DB append before it (`appendBatch`, one batched insert) is
cheap by comparison.

Because `FillQueue.enqueue` is unbounded and wait-free, the matching side will happily accept
orders far faster than this thread drains them. The gap accumulates in `ConcurrentLinkedQueue` →
heap growth → GC pressure → OOM. `FillQueue.size()` is the metric to watch; sustained nonzero =
you are over the limit.

**Fixes (cheap → structural):**
- Pipeline the sends: fire all `send()` (async, returns `CompletableFuture`), then `join` the
  batch once. Turns N sequential RTTs into ~1 RTT. **Single biggest win — ~Nx on drain rate.**
- Or shard `PersistenceWorker` per pair / per partition (7 threads, matching the 7 partitions).
- Add the bounded-queue backpressure the `TODO` already describes so overload degrades to a 503
  / parked thread instead of OOM.

---

## Bottleneck #2 — global `PositionBook` monitor serializes all pairs (matching ceiling)

`PositionBook` ([PositionBook.java:30](../../src/main/java/com/fxoee/engine/position/PositionBook.java#L30)):
every method (`applyFill`, `netQty`, `lots`, `heldMarginUsd`, `clear`, `seed`) is
**`synchronized`** on the single bean instance. `MarginLedger` is `ConcurrentHashMap` but each
account's mutators are `synchronized(account)`.

Consequence: the 7 per-pair `OrderBook` locks suggest 7-way parallel matching, but **every fill on
every pair contends one `PositionBook` lock**. Real matching concurrency ≈ 1. With 200 Tomcat
threads + 7 Kafka consumer threads all converging here, you get lock convoy, not throughput.

It sits *above* limit #1 (in-memory work is µs), so today it is not the binding constraint — but it
is the next wall once #1 is fixed, and it makes the "7 partitions / one-thread-per-partition" design
in `KafkaTopicConfig` mostly cosmetic on the write side.

**Fix:** shard `PositionBook` state per account (or per pair) and lock per-shard, so the
account/pair touched by one fill never blocks an unrelated one. Same monitor-per-account model as
`MarginLedger` already uses.

### Sub-issue: `reconcile()` cost grows with book depth
`reconcile()` ([MatchingService.java:541](../../src/main/java/com/fxoee/engine/MatchingService.java#L541))
runs **on every fill**, iterates **all 7 books**, pulls `getOrdersForAccount`, builds a list, and
**sorts** it. Cost = O(pairs × restingOrdersForAccount × log). For an account resting many orders
this turns each fill super-linear, all while holding the book lock + PositionBook monitor. Under a
deep book this, not Kafka, becomes the matching limiter. (It also reads other pairs' books without
holding their locks — a latent correctness smell, separate from perf.)

---

## Bottleneck #3 — thread pools & connection pool (config defaults)

No overrides in `application.yml`, so Spring Boot defaults apply:

| Pool | Default | Note |
|------|---------|------|
| Tomcat HTTP threads | **200 max** | fine for ingest; they just queue into the locks above |
| HikariCP DB pool | **10 connections** | **shared by** FillConsumer (7 listener threads) + PersistenceWorker + FundsPersistConsumer + bootstrap + any REST DB reads |
| Kafka `fillBatchListenerContainerFactory` | concurrency = **7** (= pairs) | one thread/partition, good |
| `max.poll.records` | **500** | matches DB batch sizing |
| PersistenceWorker | **1 thread** | see #1 |

**Risk:** the 7 `FillConsumer` threads each open a `@Transactional` DB connection during flush; add
PersistenceWorker's `appendBatch`/`markPublished` and FundsPersist, and you can approach the
**10-connection** Hikari ceiling under load → connection-wait stalls that *look* like Kafka lag.
Raise `spring.datasource.hikari.maximum-pool-size` to ~20–30 and size it against the consumer
thread count.

---

## Bottleneck #4 — allocation / GC churn on the hot path (secondary)

Every order/trade allocates heavily: `UUID.randomUUID()` per event (several per fill), many
`BigDecimal` ops (each a new object, `setScale`/`divide` more so), fresh `ArrayList`/`HashSet`
per submit, `TradeExecuted`/`OrderMatched` records, JSON serialization per event in the worker.
At high rates this is real young-gen pressure and shows up as GC pauses that widen latency tails.
Not the binding limit, but it lowers the ceiling of #2 and adds jitter. Profile before optimizing;
candidate wins are pooling the per-batch lists (worker already reuses some) and avoiding
`UUID.randomUUID` where a cheaper id suffices.

---

## Downstream: FillConsumer (the DB projection) is fine

`FillConsumer.onTradeBatch` batches all DB writes into grouped `dsl.batch(...)` calls
([FillConsumer.java:98](../../src/main/java/com/fxoee/events/kafka/FillConsumer.java#L98)),
runs 7-wide, and is already instrumented (`apply/dbFlush/total µs` logs). With the Hikari pool
sized up it should sustain several thousand fills/s — comfortably above limit #1. It is **not** the
bottleneck; it is gated by how fast PersistenceWorker publishes to it.

---

## Estimated limits (single node, order of magnitude)

| Scenario | Limit | Bound by |
|----------|-------|----------|
| **Burst** (seconds), absorbed in queue | ~5k–20k orders/s | PositionBook monitor (#2), GC (#4) |
| **Sustained** ingest = retire, local Kafka | **~300–1000 trades/s** | PersistenceWorker sync sends (#1) |
| **Sustained**, cloud Kafka `acks=all` | **~20–100 trades/s** | PersistenceWorker sync sends (#1) |
| Failure mode above sustained limit | OOM (minutes) | unbounded FillQueue (#1) |

Numbers are reasoned from the code (RTT × sequential sends, single thread), **not measured**.
Validate with a load test (see below) before quoting them.

---

## Recommended order of work

1. **Pipeline PersistenceWorker sends** (async-fire then batch-join). Biggest, cheapest win. Lifts
   sustained limit ~Nx toward the FillConsumer/DB ceiling.
2. **Bound the FillQueue** + backpressure policy (the existing `TODO`). Converts OOM into a clean
   503 / parked producer.
3. **Raise Hikari pool** to ~20–30; size to consumer threads.
4. **Shard PositionBook** locking per account/pair. Unlocks real multi-pair matching parallelism.
5. **Cap/curtail reconcile()** cost (incremental margin update instead of full rescan per fill).

## How to actually measure (don't trust the estimates)

- Drive `POST /api/engine/orders` with a k6/Gatling ramp; watch `FillQueue.size()` (already exposed)
  via Prometheus (`/actuator/prometheus` is on). Sustained-rate limit = the input rate at which
  queue depth stops returning to ~0.
- The worker and repo already log per-batch µs — turn `com.fxoee` logging up from `off` under test.
- Watch GC logs and Hikari `pending`/`active` gauges for the secondary limits.
