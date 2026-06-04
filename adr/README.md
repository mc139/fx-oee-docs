# Architecture Decision Records

Each ADR captures one significant decision: the context that forced it, the choice made, and the
consequences accepted. They are immutable once accepted — a reversed decision gets a new ADR that
supersedes the old one rather than an edit.

| ADR | Decision | Status |
|-----|----------|--------|
| [0001](0001-monolith-over-microservices.md) | Single Spring Boot monolith, not microservices | Accepted |
| [0002](0002-in-memory-authoritative-engine.md) | In-memory matching engine is the source of truth; DB/Kafka are projections | Accepted |
| [0003](0003-jooq-over-jpa.md) | jOOQ (codegen from migrated schema) over JPA/Hibernate | Accepted |
| [0004](0004-async-fill-queue-over-disruptor.md) | Async fill hand-off: interim `ConcurrentLinkedQueue`, LMAX Disruptor planned | Accepted (interim) |

Format: Status · Context · Decision · Consequences. Back to [docs index](../README.md).
