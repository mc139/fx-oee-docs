# ADR 001 — Monolith over Microservices

**Status:** Accepted  
**Date:** 2026-06-02

## Context

FX-OEE is a low-latency order execution engine. The hot path — validate → reserve margin → match → apply fills — must complete in microseconds to low milliseconds. Splitting matching, risk, and persistence into separate network hops would add serialization cost and tail latency on every order.

The system also needs strong consistency between the in-memory order book, margin ledger, and position book during a single order's lifecycle. Distributed transactions or sagas across services would complicate partial-fill and cancel semantics.

## Decision

Run the entire execution stack as a **single Spring Boot monolith** (`fx-oee`):

| Layer | Package | Responsibility |
|-------|---------|----------------|
| API | `com.fxoee.api` | REST controllers, WebSocket handler |
| Engine | `com.fxoee.engine` | `MatchingService`, margin, positions, fill queue |
| Matching | `com.fxoee.matching` | `OrderBook`, `MatchingEngine` |
| Events | `com.fxoee.events` | Kafka producers/consumers (async projection) |
| Persistence | `com.fxoee.persistence` | jOOQ repositories (off hot path) |
| Frontend | `frontend/` | Svelte UI bundled into the JAR |

Kafka and PostgreSQL are **async projections** of engine state, not separate services on the critical path. See [async-fill-flow.md](../architecture/async-fill-flow.md).

## Consequences

**Positive**

- Zero network hops on the matching path; all state mutations are in-process method calls under a per-pair book lock.
- Simpler reasoning about order lifecycle: one JVM owns truth until `FillQueue` hands off to the persistence worker.
- Single deployable artifact (`docker-compose up` or one Kubernetes Deployment).

**Negative**

- Vertical scaling only for the hot path; cannot independently scale matching vs. API without splitting later.
- A JVM crash loses in-flight queue depth (bounded by uncommitted fills); recovery relies on the `trade_events` log (see ADR 002).

**Follow-ups**

- If sustained throughput exceeds single-node limits, shard by currency pair (7 independent books already exist) before extracting microservices.
- Circuit breaker and FIX adapter (Phases 11–12) remain in-process modules, not new services.
