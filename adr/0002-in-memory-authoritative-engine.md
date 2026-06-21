# ADR 0002 - In-memory matching engine as the source of truth

_Last updated: 2026-06-21 BST._

**Status:** Accepted

## Context

An order book matches at the speed of memory. A naive design makes the database the source of truth
and reads/writes rows on every order, but a SELECT-then-match-then-UPDATE round-trip per order caps
throughput at the DB's transaction rate (thousands/sec at best) and serializes the hot path on the
connection pool. We need order entry to match in microseconds while still surviving a restart with no
lost or invented trades.

There are two state-of-truth options:

1. **DB-authoritative**: every match is a transaction; the DB is correct by construction but slow.
2. **Memory-authoritative**: the engine holds state in RAM and matches without I/O; durability is a
   separate concern.

## Decision

Make the **in-memory engine authoritative**. The `OrderBook` (per pair), `PositionBook`, and
`MarginLedger` live in the JVM heap; `MatchingService.submit` mutates them under fine-grained locks
with **no database I/O on the hot path**.

Durability and the read-model are decoupled and derived:

- After a fill, the engine stamps the exact effects (cash delta, realized P&L, lot opens/closes by
  engine lot id) onto a `TradeExecuted` event and hands it to the async pipeline.
- `PersistenceWorker` appends it to the **append-only `trade_events` log** *before* publishing to
  Kafka, so the log is the single committed source both projections derive from.
- The **DB projection** (`FillConsumer`) and the **in-memory mirror** (`AccountState`) apply those
  stamped effects **verbatim**. They never re-derive open/close or cash math, so they cannot drift
  from the engine.
- On restart the engine is **replayed** from the log (`seedForReplay` → `replayFill` →
  `reconcileReserved`).

Locking is **fine-grained**: one `ReentrantLock` per pair (book) and one per account (positions),
with reconcile run outside book locks to avoid an ABBA deadlock (see [doc 01](../01-architecture.md)
for the locking model). This is the **default engine** (`fxoee.engine.mode=default`). A co-equal
**speed engine** (`fxoee.engine.mode=speed`) keeps the same memory-authoritative model but replaces
the locks with a single-writer LMAX Disruptor command ring, where the lock-free guarantee comes from
having exactly one matching thread (see [ADR 0005](0005-disruptor-adoption.md) and
[speed-engine.md](../speed-engine.md)). The LMAX Disruptor also backs the post-match fill hand-off,
which is orthogonal to the matching model either way.

## Consequences

**Positive**
- Matching is memory-speed with no DB on the critical path. Default-engine baseline: ~218k
  `submit`/sec, ~4.4M `applyFill`/sec (`EnginePerformanceTest`). The speed engine, driven through its
  Disruptor ring with concurrent submitters (`SpeedEngineBench`, no Spring/DB/Kafka, 12-core), runs
  resting LIMITs at ~1.5M/sec and crossing-and-filling orders at ~660k/sec.
- Engine, DB, and mirror cannot diverge: all three derive from one committed log.
- Warm restart with no data loss and no invented trades (crash-before-insert = never happened;
  crash-after-insert = re-published idempotently).
- The conservation invariant (`cash == deposit + Σ realized P&L`) holds in pure memory and is fuzz-
  tested (`EngineConservationFuzzTest`).

**Negative / accepted trade-offs**
- All trading state must fit in heap; capacity is bounded by RAM, not disk.
- A reader querying the DB sees a projection that lags the engine by the async pipeline's latency
  (eventual consistency for the read-model; the engine itself is always current).
- Recovery time scales with log length (replay), so the `trade_events` log needs periodic compaction
  /snapshotting as it grows (future work).
