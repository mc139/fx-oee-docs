# ADR 004 — LMAX Disruptor for Order Ingress

**Status:** Proposed (dependency present; handler chain not yet wired — Phase 5)  
**Date:** 2026-06-02

## Context

Today orders enter via `POST /api/engine/orders` → `MatchingService.submit()` on a Tomcat worker thread. The call is synchronous: validate, match, enqueue fill, return `ExecutionReport`.

As load grows, direct servlet-thread execution couples HTTP thread pool sizing to matching throughput and makes it harder to enforce a strict handler pipeline (validate → risk → match → publish).

## Decision (planned)

Introduce **`Disruptor<OrderEvent>`** with a power-of-2 ring buffer (e.g. 1024 slots) as the sole ingress to the engine:

```
OrderController → ringBuffer.publish(OrderEvent)
                      ↓
              ValidationHandler
                      ↓
              RiskHandler
                      ↓
              MatchingHandler  → MatchingEngine
                      ↓
              EventPublishHandler → Kafka (async)
```

The disruptor dependency (`com.lmax:disruptor:4.0.0`) is already in `pom.xml`.

## Trade-offs vs `BlockingQueue`

| Aspect | LMAX Disruptor | `ArrayBlockingQueue` |
|--------|----------------|----------------------|
| Contention | Lock-free CAS on ring; cache-line padding avoids false sharing | Lock or lock-free queue; higher contention at high fan-in |
| Latency | Predictable, low jitter — designed for financial pipelines | Good but higher tail latency under burst |
| Backpressure | Fixed capacity; `tryPublish` / wait strategy configurable | `put` blocks producer; `offer` drops or returns false |
| Complexity | Handler chain, event pre-allocation, wait strategy tuning | Simple API, fewer concepts |
| GC | Reuse `OrderEvent` slots | New queue nodes or copied payloads |

**Why Disruptor over BlockingQueue:** FX ingress is single-producer (HTTP) → single-consumer-chain with strict ordering per sequence. Disruptor's sequenced handlers match the validate → risk → match pipeline without a thread pool hop per stage. `BlockingQueue` would still require a worker thread and serialise stages manually.

**Why not skip Disruptor:** Current `MatchingService` already meets latency targets for dev/demo load. Disruptor pays off when decoupling ingress from matching threads or pinning handlers to dedicated cores. Phase 5 implements this; until then servlet threads remain acceptable.

## Consequences

**When implemented**

- HTTP returns after publish to ring (or after wait strategy confirms slot) — response latency decoupled from downstream Kafka.
- `DisruptorBenchmark` (Phase 15) will measure end-to-end ring latency vs direct call baseline.

**Risks**

- Misconfigured wait strategy (`BlockingWait` vs `BusySpin`) affects CPU vs latency.
- Must not perform blocking I/O inside handlers — Kafka stays in `EventPublishHandler` after match, consistent with current `FillQueue` design.

**Current interim design**

`FillQueue` + `PersistenceWorker` already move Kafka off the hot path without Disruptor. Disruptor addresses **ingress sequencing**, not persistence — the two are complementary.
