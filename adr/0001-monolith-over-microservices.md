# ADR 0001 - Monolith over microservices

_Last updated: 2026-06-09 BST._

**Status:** Accepted

## Context

`fx-oee` is an order-execution engine. Its correctness rests on a few state mutations that must be
**atomic relative to each other**: validate funds → reserve margin → match → apply fills → reconcile
margin. Splitting these across network boundaries would turn in-process method calls into distributed
transactions, and the latency-critical match loop into a multi-hop saga.

The components are also tightly coupled by shared in-memory state: the `OrderBook`, `PositionBook`,
and `MarginLedger` are read and written within a single `submit` call under fine-grained locks (see
[ADR 0002](0002-in-memory-authoritative-engine.md)). That state cannot be sharded across services
without either replicating it or serializing every access over the wire.

## Decision

Run everything in **one Spring Boot process** ([FxOeeApplication](../../src/main/java/com/fxoee/FxOeeApplication.java)):
matching core, REST + WebSocket API, the async projection pipeline, the market simulator, and the
mock market maker. Use **internal module boundaries** (Java packages with a deliberately pure
`com.fxoee.engine` core that touches Spring only in `EngineConfig`) instead of network boundaries.

Scaling and decoupling that genuinely benefit from async are handled **inside** the monolith via
Kafka: the engine publishes events that drive the DB projection and WebSocket fan-out, without
fragmenting the transactional core.

## Consequences

**Positive**
- The hot path is in-process method calls under local locks: microsecond-scale, no distributed
  transactions, no partial-failure saga logic.
- One deployable artifact, one schema, one log stream: simple to run and reason about.
- The pure-Java engine core stays framework-free and trivially unit-testable
  (`EngineTestSupport.newService` wires the whole engine with no Spring/Kafka/DB).

**Negative / accepted trade-offs**
- Vertical scaling only for the matching core; horizontal scale comes from partitioning by currency
  pair within the process (one `OrderBook`/`MatchingEngine` per pair), not from more service instances.
- A deploy restarts the whole system, mitigated by warm-restart replay from the `trade_events` log
  ([doc 05](../05-event-sourcing-persistence.md)).
- Module boundaries are enforced by convention/packages, not by the compiler or the network.
