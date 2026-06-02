# Kafka event flow — async FillQueue + event sourcing

Diagram source: [`async-fill-flow.puml`](./async-fill-flow.puml)
(render with `plantuml async-fill-flow.puml` or any PlantUML viewer).

## Why this exists

The matching engine applies a fill in ~5µs (pure CPU). Publishing the resulting
`TradeExecuted` to Kafka takes a ~1–5ms round-trip. When the hot path did the Kafka send
**synchronously**, throughput was bound by that round-trip, not the CPU — a ceiling of
**~200 orders/pair/sec**. Orders didn't drop; they queued behind the book lock.

This design takes Kafka I/O off the hot path.

## The flow

```
Client ──HTTP──▶ MatchingService ──enqueue(~1µs)──▶ FillQueue
                  (engine = truth)                     │
                                                        ▼
                                              PersistenceWorker (1 thread)
                                                 1. append → trade_events   (DURABLE FIRST)
                                                 2. publish → Kafka          (sync ack)
                                                 3. markPublished
                                                 4. publish OrderMatched     (async)
                                                        │
                                              Kafka TRADES_EXECUTED
                                                        ▼
                                              FillConsumer (batched)
                                                 ├─ AccountState mirror (in-mem)
                                                 └─ Postgres projection (dedup by tradeId:side)
```

| Stage | Thread | Blocks hot path? |
|---|---|---|
| `engine.match()` + `applyFills` | HTTP (under book lock) | n/a — this *is* the work |
| `FillQueue.enqueue` | HTTP | No — wait-free `offer`, ~1µs |
| append log + publish Kafka | `persistence-worker` daemon | No |
| DB + mirror write | Kafka listener | No |

## Why there is no drift on crash

`trade_events` is the **single durable source** both projections derive from:

- **Engine** is rebuilt by replaying `trade_events` oldest-first on restart
  (`MatchingService.replayFill`, driven by `AccountBootstrapper` when
  `fxoee.recovery.replay-on-startup=true`; default `false` keeps the fresh-start dev boot).
- **DB projection** is written by `FillConsumer` off the Kafka stream that the log feeds, and the
  unconfirmed tail is re-published by the startup relay.

Because both derive from the same committed log, they cannot diverge:

| Crash point | Engine on restart | DB | Result |
|---|---|---|---|
| **Before** log append (step 1) | replay has nothing → order absent | never written | **Agree** — order was never durably committed |
| **After** append, before/at publish | replay has it | relay re-publishes `published=FALSE` rows → FillConsumer writes it (dedup by `event_id`) | **Agree** — DB catches up |

The ordering **append-then-publish** is the invariant. Reversing it (publish first) would
reintroduce drift.

## Ordering & idempotency guarantees

- **One** `PersistenceWorker` thread → FIFO append + publish, ordering preserved per pair.
- Log insert is `ON CONFLICT (event_id) DO NOTHING` → worker retry after a publish failure is a no-op.
- `FillConsumer` dedups by `tradeId:side` (and `tradeId:FEE`) → at-least-once Kafka delivery is safe.

## Known follow-up — backpressure

`FillQueue` is **unbounded** by design for now. If the worker stalls (Kafka/DB outage) the
queue grows in heap and the DB projection lags (seconds/minutes) but the engine keeps matching.
Accepted trade-off. A bounded capacity + block/drop policy is the follow-up; `FillQueue.size()`
is exposed for monitoring the lag meanwhile.
