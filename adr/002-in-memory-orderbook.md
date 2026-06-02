# ADR 002 — In-Memory Order Book (TreeMap)

**Status:** Accepted  
**Date:** 2026-06-02

## Context

Price-time priority matching requires:

1. **Best price first** — bids descending, asks ascending.
2. **FIFO within a price level** — earlier orders at the same price fill first.
3. **Fast cancel by order id** — O(1) lookup, then remove from the level queue.
4. **Depth-dependent performance** — book may hold hundreds to thousands of price levels under load.

Alternatives considered: custom skip list, array-backed price levels with fixed tick grid, off-heap structures (Chronicle Map), Redis-backed book.

## Decision

Use **`TreeMap<BigDecimal, Queue<Order>>`** per side in `OrderBook`:

- **Bids:** `TreeMap` with `Comparator.reverseOrder()` — `firstKey()` is best bid.
- **Asks:** natural ascending order — `firstKey()` is best ask.
- **Per level:** `LinkedList<Order>` for FIFO price-time priority.
- **Indexes:** `LinkedHashMap` for order-by-id (cancel) and per-account secondary index (reconcile).

Thread safety: one `ReentrantLock` per book. Simplicity over lock-free for now; JMH benchmarks (`MatchingEngineBenchmark`) guide future optimisation.

### Crash recovery

The order book is **not** the durable source of truth. On restart:

1. **`trade_events` table** — append-only log written *before* Kafka publish (`PersistenceWorker`).
2. **Engine replay** — when `fxoee.recovery.replay-on-startup=true`, `AccountBootstrapper` replays committed fills into `MatchingService.replayFill()`, rebuilding resting orders and positions.
3. **DB projection** — `FillConsumer` derives Postgres state from the same Kafka stream; startup relay re-publishes `published=FALSE` rows.

Invariant: **append-then-publish**. Both engine and DB derive from the same log, so they cannot drift. See [async-fill-flow.md](../architecture/async-fill-flow.md).

## Consequences

**Positive**

- O(log N) insert/remove at a price level; O(1) best bid/ask via `firstKey()`.
- No external store latency on match; typical single-fill cost is microseconds (CPU-bound).
- Recovery model is explicit and testable.

**Negative**

- Full book state is lost on crash before log append; uncommitted orders disappear (by design — never durably accepted).
- `BigDecimal` keys allocate; a fixed-point `long` tick representation would reduce GC pressure if profiling demands it.
- Per-pair lock + global `PositionBook` monitor still serialises cross-pair fills (see [bottleneck-analysis.md](../performance/bottleneck-analysis.md)).

**Follow-ups**

- `OrderBookRecoveryService` from Kafka `order-events` replay (Phase 9) for resting-order reconstruction without full trade replay.
- Evaluate off-heap or object pooling if JMH shows GC as dominant cost at deep books (2000+ levels).
